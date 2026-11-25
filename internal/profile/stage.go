package profile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Stager fetches and verifies a profile's artifacts and installs its unit.
// Nothing here stops or starts a service. Every side effect is a field.
type Stager struct {
	HFCache string
	Run     func(ctx context.Context, dir string, argv ...string) error
	Output  func(ctx context.Context, name string, args ...string) (string, error)
	// Available returns free bytes on the filesystem holding path.
	Available func(path string) (uint64, error)
	// Download streams url to dst, resuming at offset when the server allows it.
	Download func(ctx context.Context, url, dst string, offset int64) error
	Stat     func(string) (os.FileInfo, error)
	// Log receives progress lines.
	Log func(format string, a ...any)
	// ReserveBytes stays free after downloads.
	ReserveBytes uint64
}

func (s *Stager) logf(format string, a ...any) {
	if s.Log != nil {
		s.Log(format, a...)
	}
}

// ArtifactPlan says what staging one artifact still has to do.
type ArtifactPlan struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Present   bool   `json:"present"`
	NeedBytes int64  `json:"need_bytes"`
	// Path is the directory whose free space the download needs.
	Path string `json:"path,omitempty"`
	Note string `json:"note,omitempty"`
}

// StagePlan is what Stage would do, computed without changing anything.
type StagePlan struct {
	Profile   string         `json:"profile"`
	Artifacts []ArtifactPlan `json:"artifacts"`
	// NeedBytes counts files still to download. ImageBytes is the compressed
	// size of images still to pull; Docker's own storage is not checked here.
	NeedBytes    int64  `json:"need_bytes"`
	ImageBytes   int64  `json:"image_bytes"`
	ReserveBytes uint64 `json:"reserve_bytes"`
	Enough       bool   `json:"enough_space"`
	Shortfall    string `json:"shortfall,omitempty"`
}

func (s *Stager) imagePresent(ctx context.Context, a Artifact) bool {
	ref := a.Name
	if !strings.Contains(ref, "@") {
		ref += "@" + strings.TrimPrefix(a.Revision, "@")
	}
	if out, err := s.Output(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", ref); err == nil && out != "" {
		return true
	}
	// A local image has no repository digest; its content id is the pin.
	out, err := s.Output(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", a.Revision)
	return err == nil && out != ""
}

// Need returns the bytes still to be downloaded for an artifact.
func (s *Stager) Need(a Artifact) (int64, error) {
	var total int64
	switch a.Kind {
	case "huggingface":
		dir := HFDir(s.HFCache, a)
		for _, f := range a.Files {
			if st, err := s.Stat(filepath.Join(dir, f.Path)); err == nil && st.Size() == f.Bytes {
				continue
			}
			total += f.Bytes
		}
	case "url":
		if st, err := s.Stat(a.Name); err == nil && st.Size() == a.Bytes {
			return 0, nil
		}
		total = a.Bytes
		if st, err := s.Stat(a.Name + ".partial"); err == nil && st.Size() < a.Bytes {
			total -= st.Size()
		}
	}
	return total, nil
}

// Plan reports, per artifact, what is missing and whether the disks can hold it.
// Free space is checked per directory, so two artifacts on different
// filesystems are not pooled; two on one filesystem are each checked alone.
func (s *Stager) Plan(ctx context.Context, p Profile) (StagePlan, error) {
	plan := StagePlan{Profile: p.ID, ReserveBytes: s.ReserveBytes, Enough: true}
	perPath := map[string]int64{}
	for _, a := range p.Artifacts {
		ap := ArtifactPlan{Kind: a.Kind, Name: a.Name}
		switch a.Kind {
		case "image":
			ap.Present = s.imagePresent(ctx, a)
			if !ap.Present {
				plan.ImageBytes += a.Bytes
				ap.Note = "pulled into Docker's storage; free space there is not checked here"
			}
		case "git":
			_, err := s.Stat(a.Creates)
			ap.Present = a.Creates != "" && err == nil
		default:
			n, err := s.Need(a)
			if err != nil {
				return plan, err
			}
			ap.NeedBytes, ap.Present = n, n == 0
			switch {
			case a.Kind == "huggingface" && a.Dest != "":
				ap.Path = a.Dest
			case a.Kind == "huggingface":
				ap.Path = s.HFCache
			default:
				ap.Path = filepath.Dir(a.Name)
			}
			plan.NeedBytes += n
			perPath[ap.Path] += n
		}
		plan.Artifacts = append(plan.Artifacts, ap)
	}
	paths := make([]string, 0, len(perPath))
	for path := range perPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		need := perPath[path]
		if need == 0 {
			continue
		}
		free, err := s.Available(existingParent(path))
		if err != nil {
			return plan, fmt.Errorf("check free space at %s: %w", path, err)
		}
		if free < uint64(need)+s.ReserveBytes {
			plan.Enough = false
			plan.Shortfall = fmt.Sprintf("%s needs %.1f GiB plus a %.1f GiB reserve; %.1f GiB free at %s",
				p.ID, float64(need)/(1<<30), float64(s.ReserveBytes)/(1<<30), float64(free)/(1<<30), path)
			break
		}
	}
	return plan, nil
}

// Stage makes every artifact present and verified, then returns. It is safe to
// repeat: finished artifacts are skipped and partial downloads resume.
func (s *Stager) Stage(ctx context.Context, p Profile) error {
	plan, err := s.Plan(ctx, p)
	if err != nil {
		return err
	}
	if !plan.Enough {
		return errors.New(plan.Shortfall)
	}
	for _, a := range p.Artifacts {
		s.logf("artifact %s %s", a.Kind, a.Name)
		var err error
		switch a.Kind {
		case "huggingface":
			err = s.stageHF(ctx, a)
		case "url":
			err = s.stageURL(ctx, a)
		case "image":
			err = s.stageImage(ctx, a)
		case "git":
			err = s.stageGit(ctx, a)
		}
		if err != nil {
			return fmt.Errorf("%s %s: %w", a.Kind, a.Name, err)
		}
	}
	if p.Launch != nil {
		for _, d := range p.Launch.Dirs {
			if err := os.MkdirAll(d, 0o755); err != nil {
				return err
			}
		}
	}
	return nil
}

// existingParent returns the nearest existing ancestor of dir, whose
// filesystem is the one a download into dir will fill.
func existingParent(dir string) string {
	for {
		if _, err := os.Stat(dir); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return dir
		}
		dir = parent
	}
}

func (s *Stager) stageHF(ctx context.Context, a Artifact) error {
	dir := HFDir(s.HFCache, a)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, f := range a.Files {
		dst := filepath.Join(dir, f.Path)
		if st, err := s.Stat(dst); err == nil && st.Size() == f.Bytes {
			if f.SHA256 == "" {
				continue
			}
			if ok, err := fileSHA(dst, f.SHA256); err != nil {
				return err
			} else if ok {
				continue
			}
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		u := "https://huggingface.co/" + a.Name + "/resolve/" + a.Revision + "/" + escapePath(f.Path)
		if err := s.fetch(ctx, u, dst, f.Bytes, f.SHA256); err != nil {
			return fmt.Errorf("%s: %w", f.Path, err)
		}
	}
	return nil
}

func (s *Stager) stageURL(ctx context.Context, a Artifact) error {
	if st, err := s.Stat(a.Name); err == nil && st.Size() == a.Bytes {
		if ok, err := fileSHA(a.Name, a.SHA256); err != nil {
			return err
		} else if ok {
			return nil
		}
		return fmt.Errorf("%s has the right size but the wrong SHA-256; delete it and stage again", a.Name)
	}
	if err := os.MkdirAll(filepath.Dir(a.Name), 0o755); err != nil {
		return err
	}
	return s.fetch(ctx, a.URL, a.Name, a.Bytes, a.SHA256)
}

// fetch downloads to dst.partial, verifies size and SHA-256, then renames. A
// file that fails verification is never moved into place.
func (s *Stager) fetch(ctx context.Context, u, dst string, size int64, sum string) error {
	part := dst + ".partial"
	var offset int64
	if st, err := s.Stat(part); err == nil {
		offset = st.Size()
		if offset > size {
			if err := os.Remove(part); err != nil {
				return err
			}
			offset = 0
		}
	}
	if offset < size {
		if err := s.Download(ctx, u, part, offset); err != nil {
			return err
		}
	}
	st, err := s.Stat(part)
	if err != nil || st.Size() != size {
		return fmt.Errorf("download incomplete: want %d bytes", size)
	}
	if sum != "" {
		ok, err := fileSHA(part, sum)
		if err != nil {
			return err
		}
		if !ok {
			os.Remove(part)
			return errors.New("SHA-256 mismatch; the partial file was removed")
		}
	}
	return os.Rename(part, dst)
}

func (s *Stager) stageImage(ctx context.Context, a Artifact) error {
	if s.imagePresent(ctx, a) {
		return nil
	}
	ref := a.Name
	if !strings.Contains(ref, "@") {
		ref += "@" + strings.TrimPrefix(a.Revision, "@")
	}
	return s.Run(ctx, "", "docker", "pull", "--platform=linux/arm64", ref)
}

func (s *Stager) stageGit(ctx context.Context, a Artifact) error {
	if err := os.MkdirAll(a.Name, 0o755); err != nil {
		return err
	}
	if _, err := s.Stat(filepath.Join(a.Name, ".git")); err != nil {
		if err := s.Run(ctx, a.Name, "git", "init", "--quiet"); err != nil {
			return err
		}
	}
	if head, _ := s.Output(ctx, "git", "-C", a.Name, "rev-parse", "HEAD"); head != a.Revision {
		if err := s.Run(ctx, a.Name, "git", "fetch", "--depth=1", a.URL, a.Revision); err != nil {
			return err
		}
		if err := s.Run(ctx, a.Name, "git", "checkout", "--detach", a.Revision); err != nil {
			return err
		}
	}
	if err := s.Run(ctx, a.Name, "git", "diff", "--quiet", a.Revision, "--"); err != nil {
		return fmt.Errorf("pinned tree has tracked modifications: %w", err)
	}
	if a.Creates != "" {
		if _, err := s.Stat(a.Creates); err == nil {
			return nil
		}
	}
	for _, c := range a.Build {
		if err := s.Run(ctx, a.Name, c...); err != nil {
			return err
		}
	}
	if a.Creates != "" {
		if _, err := s.Stat(a.Creates); err != nil {
			return fmt.Errorf("build finished but %s is missing", a.Creates)
		}
	}
	return nil
}

func fileSHA(path, want string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == want, nil
}

func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, x := range parts {
		parts[i] = url.PathEscape(x)
	}
	return strings.Join(parts, "/")
}

// HTTPDownload is the default Download: one GET with an optional Range, appended
// to dst. It never follows a redirect to a non-https URL.
func HTTPDownload(ctx context.Context, u, dst string, offset int64) error {
	client := &http.Client{CheckRedirect: func(r *http.Request, _ []*http.Request) error {
		if r.URL.Scheme != "https" {
			return errors.New("refusing a redirect to a non-https URL")
		}
		return nil
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	flags := os.O_CREATE | os.O_WRONLY
	switch {
	case offset > 0 && res.StatusCode == http.StatusPartialContent:
		flags |= os.O_APPEND
	case res.StatusCode == http.StatusOK:
		flags |= os.O_TRUNC
	default:
		return fmt.Errorf("download %s: HTTP %d", u, res.StatusCode)
	}
	f, err := os.OpenFile(dst, flags, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, res.Body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// InstallUnit renders the unit and returns the exact commands an operator runs
// to install it. SparkRoster never runs sudo for installation; it writes the
// unit to a staging path the operator can inspect and prints the commands.
func InstallUnit(p Profile, user, group, stagingDir string) (path string, commands []string, err error) {
	text, err := RenderUnit(p, user, group)
	if err != nil {
		return "", nil, err
	}
	if err = os.MkdirAll(stagingDir, 0o700); err != nil {
		return "", nil, err
	}
	path = filepath.Join(stagingDir, UnitName(p.ID))
	if err = os.WriteFile(path, []byte(text), 0o600); err != nil {
		return "", nil, err
	}
	dst := "/etc/systemd/system/" + UnitName(p.ID)
	return path, []string{
		"sudo install -o root -g root -m 0644 " + shellQuote(path) + " " + dst,
		"sudo systemctl daemon-reload",
	}, nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

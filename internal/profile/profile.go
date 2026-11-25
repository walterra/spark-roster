// Package profile defines what SparkRoster can run: a pinned, verifiable
// description of one inference service. A profile either launches a service
// itself or adopts one that something else already runs.
package profile

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/walterra/spark-roster/internal/host"
)

const (
	SchemaVersion = 1
	Kind          = "profile"
)

var (
	idPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	hexRevision   = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	imagePin      = regexp.MustCompile(`(^|@)sha256:[0-9a-f]{64}$`)
	envName       = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// Duration is a JSON string such as "20m".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New("duration must be a string such as \"20m\"")
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q", s)
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.Duration.String()) }

// Artifact is something that must exist on the host before the profile can
// start: model weights, a drafter, a container image or a source checkout.
type Artifact struct {
	Kind     string `json:"kind"` // huggingface | url | image | git
	Name     string `json:"name"` // repo id, destination path, image name or checkout directory
	Revision string `json:"revision,omitempty"`
	URL      string `json:"url,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Bytes    int64  `json:"bytes,omitempty"`
	// Dest stores a huggingface artifact as a flat directory of files instead
	// of the Hugging Face cache layout.
	Dest string `json:"dest,omitempty"`
	// Files lists the repository files with their sizes. A sha256 is given for
	// large (LFS) files, which is what the Hugging Face API publishes. Staging
	// and Staged verify against this list.
	Files []File `json:"files,omitempty"`
	// Build runs in order inside a git artifact after it is checked out;
	// Creates names the file that must exist afterwards.
	Build   [][]string `json:"build,omitempty"`
	Creates string     `json:"creates,omitempty"`
}

// File is one file of a pinned repository.
type File struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256,omitempty"`
}

// Adopt names a service that something else deployed. SparkRoster starts and
// stops the units but never rewrites them.
type Adopt struct {
	Units     []string `json:"units"`               // start in order, stop in reverse
	Container string   `json:"container,omitempty"` // must be gone after stop
}

// Launch describes a service SparkRoster runs itself.
type Launch struct {
	Runtime     string            `json:"runtime"` // docker | process
	Image       string            `json:"image,omitempty"`
	Exe         string            `json:"exe,omitempty"`
	Args        []string          `json:"args"`
	Env         map[string]string `json:"env,omitempty"`
	MemoryLimit string            `json:"memory_limit,omitempty"` // e.g. "100g"
	// DockerArgs are docker run flags placed before the image. Mounts are
	// "src:dst[:ro]" bind mounts. Dirs are created before the first start.
	DockerArgs []string `json:"docker_args,omitempty"`
	Mounts     []string `json:"mounts,omitempty"`
	Dirs       []string `json:"dirs,omitempty"`
}

type Serving struct {
	Port         uint16   `json:"port"`
	Model        string   `json:"model"`
	KeyFile      string   `json:"key_file,omitempty"`
	ReadyTimeout Duration `json:"ready_timeout"`
	ToolCall     bool     `json:"tool_call_probe,omitempty"`
}

type Memory struct {
	// StartMinAvailableBytes is the host MemAvailable the profile needs before
	// it starts. It is a reviewed floor, not a measured peak.
	StartMinAvailableBytes int64  `json:"start_min_available_bytes"`
	SteadyBytes            int64  `json:"steady_bytes,omitempty"`
	Evidence               string `json:"evidence"` // measured | estimated
	Note                   string `json:"note"`
}

type Profile struct {
	SchemaVersion int        `json:"schema_version"`
	Kind          string     `json:"kind"`
	ID            string     `json:"id"`
	Title         string     `json:"title"`
	Artifacts     []Artifact `json:"artifacts"`
	Adopt         *Adopt     `json:"adopt,omitempty"`
	Launch        *Launch    `json:"launch,omitempty"`
	Serving       Serving    `json:"serving"`
	Memory        Memory     `json:"memory"`
	// Concurrency and ContextTokens describe the served configuration for the
	// planner and for reports. They do not change how the service launches.
	Concurrency   int `json:"concurrency,omitempty"`
	ContextTokens int `json:"context_tokens,omitempty"`
}

// Parse decodes and validates one profile. Unknown fields are rejected so a
// typo never silently drops a pin.
func Parse(data []byte) (Profile, error) {
	var p Profile
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	if dec.More() {
		return Profile{}, errors.New("decode profile: trailing data")
	}
	if err := p.Validate(); err != nil {
		return Profile{}, err
	}
	return p, nil
}

// Validate reports the first problem with the profile, naming the field.
func (p Profile) Validate() error {
	fail := func(format string, a ...any) error {
		id := p.ID
		if id == "" {
			id = "(no id)"
		}
		return fmt.Errorf("profile %s: %s", id, fmt.Sprintf(format, a...))
	}
	if p.SchemaVersion != SchemaVersion || p.Kind != Kind {
		return fail("expected schema_version %d and kind %q", SchemaVersion, Kind)
	}
	if !idPattern.MatchString(p.ID) {
		return fail("id must match %s", idPattern)
	}
	if strings.TrimSpace(p.Title) == "" {
		return fail("title is required")
	}
	if (p.Adopt == nil) == (p.Launch == nil) {
		return fail("exactly one of adopt and launch is required")
	}
	for i, a := range p.Artifacts {
		if err := validArtifact(a); err != nil {
			return fail("artifacts[%d]: %v", i, err)
		}
	}
	if p.Adopt != nil {
		if len(p.Adopt.Units) == 0 {
			return fail("adopt.units is required")
		}
		seen := map[string]bool{}
		for _, u := range p.Adopt.Units {
			if !host.ValidService(u) || seen[u] {
				return fail("adopt.units has an invalid or duplicate unit %q", u)
			}
			seen[u] = true
		}
	}
	if l := p.Launch; l != nil {
		if err := validLaunch(*l); err != nil {
			return fail("launch: %v", err)
		}
	}
	s := p.Serving
	if p.Concurrency < 0 || p.ContextTokens < 0 {
		return fail("concurrency and context_tokens must not be negative")
	}
	if s.Port == 0 || s.Model == "" || strings.ContainsAny(s.KeyFile, "\r\n") {
		return fail("serving needs a port and a model name")
	}
	if s.ReadyTimeout.Duration <= 0 || s.ReadyTimeout.Duration > 24*time.Hour {
		return fail("serving.ready_timeout must be between 1s and 24h")
	}
	m := p.Memory
	if m.StartMinAvailableBytes <= 0 || m.SteadyBytes < 0 || (m.Evidence != "measured" && m.Evidence != "estimated") || strings.TrimSpace(m.Note) == "" {
		return fail("memory needs start_min_available_bytes > 0, evidence measured|estimated and a note")
	}
	return nil
}

func validArtifact(a Artifact) error {
	if a.Name == "" {
		return errors.New("name is required")
	}
	switch a.Kind {
	case "huggingface", "git":
		if a.Kind == "huggingface" {
			if len(a.Files) == 0 {
				return errors.New("huggingface artifacts need a files list with sizes")
			}
			for _, f := range a.Files {
				if f.Path == "" || strings.HasPrefix(f.Path, "/") || strings.Contains(f.Path, "..") || f.Bytes < 0 || (f.SHA256 != "" && !sha256Pattern.MatchString(f.SHA256)) {
					return fmt.Errorf("invalid file entry %q", f.Path)
				}
			}
		}
		if !hexRevision.MatchString(a.Revision) {
			return errors.New("revision must be a full 40-character commit hash, not a branch or tag")
		}
		if a.Kind == "git" {
			if !strings.HasPrefix(a.URL, "https://") {
				return errors.New("git artifacts need an https url")
			}
			if len(a.Build) > 0 && a.Creates == "" {
				return errors.New("git artifacts with a build need creates")
			}
			for _, c := range a.Build {
				if len(c) == 0 || c[0] == "" {
					return errors.New("build commands need a program")
				}
			}
		}
	case "image":
		if !imagePin.MatchString(a.Revision) {
			return errors.New("revision must be a sha256 image digest")
		}
	case "url":
		if !strings.HasPrefix(a.URL, "https://") || !sha256Pattern.MatchString(a.SHA256) || a.Bytes <= 0 {
			return errors.New("url artifacts need an https url, sha256 and bytes")
		}
	default:
		return fmt.Errorf("unknown kind %q", a.Kind)
	}
	return nil
}

func validLaunch(l Launch) error {
	if len(l.Args) == 0 {
		return errors.New("args is required")
	}
	for k := range l.Env {
		if !envName.MatchString(k) {
			return fmt.Errorf("invalid environment name %q", k)
		}
	}
	for _, m := range l.Mounts {
		parts := strings.Split(m, ":")
		if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || !strings.HasPrefix(parts[1], "/") || (len(parts) == 3 && parts[2] != "ro" && parts[2] != "rw") {
			return fmt.Errorf("mount %q must be src:/dst[:ro|rw]", m)
		}
	}
	if l.Runtime == "process" && (len(l.Mounts) > 0 || len(l.DockerArgs) > 0) {
		return errors.New("mounts and docker_args apply to docker profiles only")
	}
	switch l.Runtime {
	case "docker":
		if !imagePin.MatchString(l.Image) {
			return errors.New("docker image must be pinned by sha256 digest")
		}
	case "process":
		if l.Exe == "" {
			return errors.New("process exe is required")
		}
	default:
		return fmt.Errorf("unknown runtime %q", l.Runtime)
	}
	return nil
}

// Units returns the systemd units SparkRoster controls for this profile.
func (p Profile) Units() []string {
	if p.Adopt != nil {
		return append([]string(nil), p.Adopt.Units...)
	}
	return []string{"spark-roster-" + p.ID + ".service"}
}

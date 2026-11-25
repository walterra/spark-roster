package profile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/walterra/spark-roster/internal/host"
	"github.com/walterra/spark-roster/internal/trial"
)

// Host runs profiles on the local machine through systemd and docker. Every
// side effect goes through a field, so tests replace it.
type Host struct {
	// Control runs "systemctl <action> <unit>" with whatever privilege the
	// operator has configured. The default uses sudo -n and names the exact
	// command in its error when sudo needs a password.
	Control func(ctx context.Context, action, unit string) error
	// Output runs a command and returns trimmed stdout.
	Output func(ctx context.Context, name string, args ...string) (string, error)
	// Inspect reads one unit's state.
	Inspect func(ctx context.Context, unit string) (host.Unit, error)
	// Memory reads host MemAvailable in bytes.
	Memory func(ctx context.Context) (uint64, error)
	// Generate verifies a real answer from a profile's endpoint.
	Generate func(ctx context.Context, p Profile) error
	// FileExists reports whether a path exists and its size.
	Stat func(path string) (os.FileInfo, error)
	// HFCache is the Hugging Face hub cache directory.
	HFCache string
}

// NewHost returns a Host wired to the real system.
func NewHost(hfCache string) *Host {
	return &Host{
		Control: sudoControl,
		Output:  runOutput,
		Inspect: host.InspectUnit,
		Memory: func(context.Context) (uint64, error) {
			f, err := os.Open("/proc/meminfo")
			if err != nil {
				return 0, err
			}
			defer f.Close()
			m, err := host.ReadMemory(f, timeNow())
			return m.Available, err
		},
		Generate: func(ctx context.Context, p Profile) error {
			key := ""
			if p.Serving.KeyFile != "" {
				b, err := os.ReadFile(p.Serving.KeyFile)
				if err != nil {
					return fmt.Errorf("read key file: %w", err)
				}
				key = strings.TrimSpace(string(b))
			}
			return trial.CheckGenerationTokens(ctx, p.Serving.Port, p.Serving.Model, key, 64)
		},
		Stat:    os.Stat,
		HFCache: hfCache,
	}
}

func sudoControl(ctx context.Context, action, unit string) error {
	if !host.ValidService(unit) || (action != "start" && action != "stop") {
		return errors.New("refusing an unexpected systemctl request")
	}
	args := []string{"-n", "systemctl", "--no-ask-password"}
	if action == "start" {
		args = append(args, "--no-block")
	}
	args = append(args, action, "--", unit)
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "sudo", args...)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("sudo needs a password or the unit failed (%s). Run: sudo systemctl %s %s", strings.TrimSpace(stderr.String()), action, unit)
	}
	return nil
}

func runOutput(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "", fmt.Errorf("%s: %w", name, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func (h *Host) unitState(ctx context.Context, unit string, want ...string) error {
	u, err := h.Inspect(ctx, unit)
	if err != nil {
		return err
	}
	if u.LoadState != "loaded" {
		return fmt.Errorf("%s is not loaded", unit)
	}
	for _, w := range want {
		if u.ActiveState == w {
			return nil
		}
	}
	return fmt.Errorf("%s is %s, expected %s", unit, u.ActiveState, strings.Join(want, " or "))
}

// containers returns the container names a profile can leave behind.
func containers(p Profile) []string {
	switch {
	case p.Adopt != nil && p.Adopt.Container != "":
		return []string{p.Adopt.Container}
	case p.Launch != nil && p.Launch.Runtime == "docker":
		return []string{ContainerName(p.ID)}
	}
	return nil
}

func (h *Host) Stop(ctx context.Context, p Profile) error {
	units := p.Units()
	for i := len(units) - 1; i >= 0; i-- {
		if err := h.Control(ctx, "stop", units[i]); err != nil {
			return err
		}
	}
	for _, u := range units {
		if err := h.unitState(ctx, u, "inactive", "failed"); err != nil {
			return err
		}
	}
	for _, c := range containers(p) {
		out, err := h.Output(ctx, "docker", "ps", "--filter", "name=^/"+c+"$", "--format", "{{.Names}}")
		if err != nil {
			return fmt.Errorf("check container %s: %w", c, err)
		}
		if out != "" {
			return fmt.Errorf("container %s is still running", c)
		}
	}
	return nil
}

func (h *Host) Start(ctx context.Context, p Profile) error {
	for _, u := range p.Units() {
		if err := h.Control(ctx, "start", u); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) Probe(ctx context.Context, p Profile) error {
	for _, u := range p.Units() {
		if err := h.unitState(ctx, u, "active"); err != nil {
			return err
		}
	}
	return h.Generate(ctx, p)
}

func (h *Host) Available(ctx context.Context) (uint64, error) { return h.Memory(ctx) }

// Staged verifies that every artifact is present at its pinned identity. It
// reads sizes and markers only; hashing a 28 GB file is the job of stage.
func (h *Host) Staged(ctx context.Context, p Profile) error {
	var problems []string
	for _, a := range p.Artifacts {
		if err := h.artifact(ctx, a); err != nil {
			problems = append(problems, fmt.Sprintf("%s %s: %v", a.Kind, a.Name, err))
		}
	}
	if p.Adopt != nil {
		for _, u := range p.Adopt.Units {
			if err := h.unitState(ctx, u, "active", "inactive", "failed"); err != nil {
				problems = append(problems, err.Error())
			}
		}
	} else if err := h.unitState(ctx, UnitName(p.ID), "active", "inactive", "failed"); err != nil {
		problems = append(problems, fmt.Sprintf("generated unit missing (run stage): %v", err))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (h *Host) artifact(ctx context.Context, a Artifact) error {
	switch a.Kind {
	case "huggingface":
		dir := HFDir(h.HFCache, a)
		if st, err := h.Stat(dir); err != nil || !st.IsDir() {
			return fmt.Errorf("revision %s not found at %s", a.Revision, dir)
		}
		var bad []string
		for _, f := range a.Files {
			st, err := h.Stat(filepath.Join(dir, f.Path))
			switch {
			case err != nil:
				bad = append(bad, f.Path+" missing")
			case st.Size() != f.Bytes:
				bad = append(bad, fmt.Sprintf("%s has %d bytes, expected %d", f.Path, st.Size(), f.Bytes))
			}
		}
		if len(bad) > 0 {
			return fmt.Errorf("%d of %d files wrong: %s", len(bad), len(a.Files), strings.Join(bad[:min(3, len(bad))], ", "))
		}
	case "image":
		out, err := h.Output(ctx, "docker", "image", "inspect", "--format", "{{.Id}} {{json .RepoDigests}}", a.Name+"@"+strings.TrimPrefix(a.Revision, "@"))
		if err != nil {
			// A local image identified by its content id has no repo digest.
			out, err = h.Output(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", a.Revision)
			if err != nil {
				return fmt.Errorf("image %s is not present", a.Revision)
			}
		}
		if out == "" {
			return fmt.Errorf("image %s is not present", a.Revision)
		}
	case "url":
		st, err := h.Stat(a.Name)
		if err != nil {
			return fmt.Errorf("file not present at %s", a.Name)
		}
		if st.Size() != a.Bytes {
			return fmt.Errorf("size %d, expected %d", st.Size(), a.Bytes)
		}
	case "git":
		out, err := h.Output(ctx, "git", "-C", a.Name, "rev-parse", "HEAD")
		if err != nil || out != a.Revision {
			return fmt.Errorf("checkout at %s is not at %s", a.Name, a.Revision)
		}
		if a.Creates != "" {
			if _, err := h.Stat(a.Creates); err != nil {
				return fmt.Errorf("build output %s is missing", a.Creates)
			}
		}
	}
	return nil
}

// HFDir is where a huggingface artifact's files live: a flat Dest directory
// when one is set, otherwise the revision's snapshot in the hub cache.
func HFDir(hfCache string, a Artifact) string {
	if a.Dest != "" {
		return a.Dest
	}
	return filepath.Join(hfCache, "models--"+strings.ReplaceAll(a.Name, "/", "--"), "snapshots", a.Revision)
}

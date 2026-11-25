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
	// Root means the process already is root: no sudo is needed or used.
	Root bool
}

// NewHost returns a Host wired to the real system.
func NewHost(hfCache string) *Host {
	root := os.Geteuid() == 0
	control := sudoControl
	if root {
		control = directControl
	}
	return &Host{
		Root:    root,
		Control: control,
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
			return trial.CheckGenerationTokens(ctx, p.Serving.Port, p.Serving.Model, key, 1024)
		},
		Stat:    os.Stat,
		HFCache: hfCache,
	}
}

// directControl is sudoControl for a process that already is root.
func directControl(ctx context.Context, action, unit string) error {
	if !host.ValidUnit(unit) || (action != "start" && action != "stop") {
		return errors.New("refusing an unexpected systemctl request")
	}
	args := []string{"--no-ask-password"}
	if action == "start" {
		args = append(args, "--no-block")
	}
	args = append(args, action, "--", unit)
	if err := exec.CommandContext(ctx, "systemctl", args...).Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("systemctl %s %s failed", action, unit)
	}
	return nil
}

func sudoControl(ctx context.Context, action, unit string) error {
	if !host.ValidUnit(unit) || (action != "start" && action != "stop") {
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

// UnitState is one systemd unit as the host reports it.
type UnitState struct {
	Unit   string `json:"unit"`
	Load   string `json:"load_state"`
	Active string `json:"active_state"`
}

// StateOf summarises a profile's units: running when every unit is active,
// stopped when none is active or activating, partial otherwise, and missing
// when a unit is not installed at all.
func StateOf(units []UnitState) string {
	if len(units) == 0 {
		return "missing"
	}
	active, missing := 0, 0
	for _, u := range units {
		switch {
		case u.Load != "loaded":
			missing++
		case u.Active == "active":
			active++
		}
	}
	switch {
	case missing == len(units):
		return "missing"
	case active == len(units):
		return "running"
	case active == 0 && missing == 0:
		return "stopped"
	case active == 0:
		return "stopped"
	}
	return "partial"
}

// Units reports the state of each of the profile's units.
func (h *Host) Units(ctx context.Context, p Profile) ([]UnitState, error) {
	var out []UnitState
	for _, name := range p.Units() {
		u, err := h.Inspect(ctx, name)
		if err != nil {
			return nil, err
		}
		out = append(out, UnitState{Unit: name, Load: u.LoadState, Active: u.ActiveState})
	}
	return out, nil
}

// SudoReady reports whether privileged systemctl calls will run without a
// password prompt. It never prompts.
func (h *Host) SudoReady(ctx context.Context) error {
	if h.Root {
		return nil
	}
	if _, err := h.Output(ctx, "sudo", "-n", "true"); err != nil {
		return errors.New("sudo needs a password. Run `sudo -v` in this terminal, then retry")
	}
	return nil
}

// ControlUnit starts or stops one named unit, a service or a timer, through the
// same privileged path as profile units.
func (h *Host) ControlUnit(ctx context.Context, action, unit string) error {
	return h.Control(ctx, action, unit)
}

// UnitActive reports whether a unit is currently active.
func (h *Host) UnitActive(ctx context.Context, unit string) (bool, error) {
	u, err := h.Inspect(ctx, unit)
	if err != nil {
		return false, err
	}
	return u.LoadState == "loaded" && u.ActiveState == "active", nil
}

// ConfirmInactive checks that a unit is no longer active, after a stop.
func (h *Host) ConfirmInactive(ctx context.Context, unit string) error {
	return h.unitState(ctx, unit, "inactive", "failed")
}

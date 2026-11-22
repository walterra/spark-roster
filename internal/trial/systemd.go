package trial

import (
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strings"

	"github.com/walterra/spark-roster/internal/host"
)

// Workload binds a trial role to ordered systemd units and a generation probe.
// ExitCheck must independently verify that no child or container remains.
// ArtifactCheck must verify staged artifact identities before any stock stop.
type Workload struct {
	ID            string
	Units         []string // start in order, stop in reverse
	Probe         func(context.Context) error
	ExitCheck     func(context.Context) error
	ArtifactCheck func(context.Context) error
}

// Systemd supplies the trial controller's systemd operations. Privileges must
// be supplied by the supervisor; the backend never invokes sudo or a shell.
type Systemd struct {
	stock, candidate Workload
	memory           func(context.Context) (uint64, error)
	inspect          func(context.Context, string) (host.Unit, error)
	control          func(context.Context, string, string, bool) error
}

func NewSystemd(stock, candidate Workload, memory func(context.Context) (uint64, error)) (*Systemd, error) {
	if stock.ID == candidate.ID || memory == nil {
		return nil, fmt.Errorf("invalid systemd trial binding")
	}
	units := map[string]bool{}
	for _, w := range []Workload{stock, candidate} {
		if w.ID == "" || len(w.Units) == 0 || w.Probe == nil || w.ExitCheck == nil || w.ArtifactCheck == nil {
			return nil, fmt.Errorf("incomplete workload binding")
		}
		for _, name := range w.Units {
			if !host.ValidService(name) || units[name] {
				return nil, fmt.Errorf("invalid or overlapping service unit binding")
			}
			units[name] = true
		}
	}
	stock.Units = slices.Clone(stock.Units)
	candidate.Units = slices.Clone(candidate.Units)
	return &Systemd{stock: stock, candidate: candidate, memory: memory, inspect: host.InspectUnit, control: controlSystemd}, nil
}

func controlSystemd(ctx context.Context, action, unit string, queue bool) error {
	args := []string{"--no-ask-password"}
	if queue {
		args = append(args, "--no-block")
	}
	args = append(args, action, "--", unit)
	if err := exec.CommandContext(ctx, "systemctl", args...).Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("systemctl %s failed for %s", action, unit)
	}
	return nil
}

func (b *Systemd) workload(id string) (Workload, error) {
	switch id {
	case b.stock.ID:
		return b.stock, nil
	case b.candidate.ID:
		return b.candidate, nil
	default:
		return Workload{}, fmt.Errorf("unknown trial workload")
	}
}

func (b *Systemd) state(ctx context.Context, unit string, states ...string) error {
	u, err := b.inspect(ctx, unit)
	if err != nil {
		return err
	}
	if u.Unit != unit || u.LoadState != "loaded" || !slices.Contains(states, u.ActiveState) {
		return fmt.Errorf("%s: expected loaded and %s", unit, strings.Join(states, " or "))
	}
	return nil
}

func (b *Systemd) Preflight(ctx context.Context, stock, candidate string) error {
	if stock != b.stock.ID || candidate != b.candidate.ID {
		return fmt.Errorf("trial roles do not match systemd binding")
	}
	for _, u := range b.stock.Units {
		if err := b.state(ctx, u, "active"); err != nil {
			return err
		}
	}
	for _, u := range b.candidate.Units {
		if err := b.state(ctx, u, "inactive", "failed"); err != nil {
			return err
		}
	}
	if err := b.candidate.ExitCheck(ctx); err != nil {
		return err
	}
	for _, w := range []Workload{b.stock, b.candidate} {
		if err := w.ArtifactCheck(ctx); err != nil {
			return fmt.Errorf("%s artifacts: %w", w.ID, err)
		}
	}
	return nil
}

func (b *Systemd) Stop(ctx context.Context, id string) error {
	w, err := b.workload(id)
	if err != nil {
		return err
	}
	for i := len(w.Units) - 1; i >= 0; i-- {
		if err := b.control(ctx, "stop", w.Units[i], false); err != nil {
			return err
		}
	}
	for _, u := range w.Units {
		if err := b.state(ctx, u, "inactive", "failed"); err != nil {
			return err
		}
	}
	return w.ExitCheck(ctx)
}

func (b *Systemd) Start(ctx context.Context, id string) error {
	w, err := b.workload(id)
	if err != nil {
		return err
	}
	// A multi-unit workload must declare ordering in systemd (e.g. After=).
	// Queue requests so the controller can observe memory during startup.
	for _, u := range w.Units {
		if err := b.control(ctx, "start", u, true); err != nil {
			return err
		}
	}
	return nil
}

func (b *Systemd) Probe(ctx context.Context, id string) error {
	w, err := b.workload(id)
	if err != nil {
		return err
	}
	for _, u := range w.Units {
		if err := b.state(ctx, u, "active"); err != nil {
			return err
		}
	}
	return w.Probe(ctx)
}

func (b *Systemd) Available(ctx context.Context) (uint64, error) { return b.memory(ctx) }

package host

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/walterra/spark-roster/internal/planner"
)

var inventoryID = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Inventory binds reviewed running profiles to local systemd units. A unit
// observation checks status only, not artifact identity or generation readiness.
type Inventory struct {
	planner.Header
	Services map[string]Binding `json:"services"`
}

type Binding struct {
	ProfileID string   `json:"profile_id"`
	Units     []string `json:"units"`
}

// CaptureSnapshot refuses state disagreement instead of guessing whether a
// partial workload is resident. Cgroup counters do not establish memory bounds.
func CaptureSnapshot(ctx context.Context, inventory Inventory, inspect func(context.Context, string) (Unit, error), memory func() (Memory, error)) (planner.Snapshot, error) {
	if inventory.Header != (planner.Header{SchemaVersion: 1, Kind: "systemd_inventory"}) || len(inventory.Services) == 0 {
		return planner.Snapshot{}, fmt.Errorf("expected a nonempty v1 systemd_inventory")
	}
	names := make([]string, 0, len(inventory.Services))
	for name := range inventory.Services {
		names = append(names, name)
	}
	slices.Sort(names)
	seen := make(map[string]bool)
	for _, name := range names {
		binding := inventory.Services[name]
		if !inventoryID.MatchString(name) || !inventoryID.MatchString(binding.ProfileID) || len(binding.Units) == 0 {
			return planner.Snapshot{}, fmt.Errorf("invalid binding for service %q", name)
		}
		for _, unit := range binding.Units {
			if !ValidService(unit) || seen[unit] {
				return planner.Snapshot{}, fmt.Errorf("invalid or duplicate unit for service %q", name)
			}
			seen[unit] = true
		}
	}
	running := make(map[string]planner.RunningService, len(names))
	for _, name := range names {
		binding := inventory.Services[name]
		for _, unit := range binding.Units {
			if err := ctx.Err(); err != nil {
				return planner.Snapshot{}, err
			}
			observed, err := inspect(ctx, unit)
			if err != nil {
				return planner.Snapshot{}, fmt.Errorf("inspect %s: %w", unit, err)
			}
			if observed.Unit != unit || observed.LoadState != "loaded" || observed.ActiveState != "active" {
				return planner.Snapshot{}, fmt.Errorf("inventory drift: %s is not confirmed loaded and active", unit)
			}
		}
		running[name] = planner.RunningService{ProfileID: binding.ProfileID}
	}
	if err := ctx.Err(); err != nil {
		return planner.Snapshot{}, err
	}
	m, err := memory()
	if err != nil {
		return planner.Snapshot{}, fmt.Errorf("read memory: %w", err)
	}
	if m.Total == 0 || m.Total > maxBytes || m.Available > m.Total || m.CapturedAt.IsZero() {
		return planner.Snapshot{}, fmt.Errorf("invalid memory observation")
	}
	total, available := int64(m.Total), int64(m.Available)
	return planner.Snapshot{
		Header:     planner.Header{SchemaVersion: 1, Kind: "host_snapshot"},
		CapturedAt: m.CapturedAt.UTC(), Total: &total, Available: &available, Running: running,
	}, nil
}

// SnapshotTimeout bounds a batch of systemd probes.
const SnapshotTimeout = 10 * time.Second

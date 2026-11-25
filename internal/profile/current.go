package profile

import (
	"context"
	"fmt"
	"strings"
)

// UnitReader reads the systemd state of a profile's units.
type UnitReader interface {
	Units(context.Context, Profile) ([]UnitState, error)
}

// Current says which profile is running. It returns nil when none is. It
// returns an error rather than guess when the host and the journal disagree or
// several profiles run at once, because acting on a guess stops the wrong
// service. running lists every profile with an active or partly active unit.
func Current(ctx context.Context, r UnitReader, st State, set map[string]Profile) (current *Profile, running []string, err error) {
	for _, id := range IDs(set) {
		units, e := r.Units(ctx, set[id])
		if e != nil {
			return nil, nil, fmt.Errorf("inspect %s: %w", id, e)
		}
		if s := StateOf(units); s == "running" || s == "partial" {
			running = append(running, id)
		}
	}
	switch len(running) {
	case 0:
		if st.Active != "" {
			return nil, running, fmt.Errorf("the journal says %q is active but none of its units are running. Start it with `spark-roster switch %s`, or clear the record by running `spark-roster stop`", st.Active, st.Active)
		}
		return nil, running, nil
	case 1:
		id := running[0]
		if st.Active != "" && st.Active != id {
			return nil, running, fmt.Errorf("the journal says %q is active but %q is running. Stop %q yourself or run `spark-roster stop`", st.Active, id, id)
		}
		p := set[id]
		return &p, running, nil
	}
	return nil, running, fmt.Errorf("more than one profile is running (%s); SparkRoster will not guess which to replace. Stop all but one", strings.Join(running, ", "))
}

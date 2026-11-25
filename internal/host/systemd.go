package host

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var serviceName = regexp.MustCompile(`^[A-Za-z0-9_.][A-Za-z0-9_.:@-]{0,247}\.service$`)
var timerName = regexp.MustCompile(`^[A-Za-z0-9_.][A-Za-z0-9_.:@-]{0,247}\.timer$`)
var showProperties = []string{"LoadState", "ActiveState", "UnitFileState", "MemoryCurrent", "MemoryMax"}

// ValidService restricts inspection to one literal service unit, not a pattern.
func ValidService(unit string) bool { return serviceName.MatchString(unit) }

// ValidUnit also accepts one literal timer unit. Benchmarks pause a timer such as
// a health canary; profiles never run as timers, so ValidService stays strict.
func ValidUnit(unit string) bool { return serviceName.MatchString(unit) || timerName.MatchString(unit) }

// Unit exposes allowlisted state only; it does not expose commands or environment.
type Unit struct {
	SchemaVersion      int     `json:"schema_version"`
	Kind               string  `json:"kind"`
	Unit               string  `json:"unit"`
	LoadState          string  `json:"load_state"`
	ActiveState        string  `json:"active_state"`
	UnitFileState      string  `json:"unit_file_state,omitempty"`
	MemoryCurrent      *uint64 `json:"memory_current_bytes,omitempty"`
	MemoryMax          *uint64 `json:"memory_max_bytes,omitempty"`
	MemoryMaxUnlimited bool    `json:"memory_max_unlimited,omitempty"`
}

// Draft identifies an existing lifecycle owner and lists facts still needed to reproduce it.
type Draft struct {
	SchemaVersion  int      `json:"schema_version"`
	Kind           string   `json:"kind"`
	Backend        string   `json:"backend"`
	Observed       Unit     `json:"observed"`
	RequiredInputs []string `json:"required_inputs"`
}

func (u Unit) Draft() Draft {
	return Draft{SchemaVersion: 1, Kind: "systemd_profile_draft", Backend: "systemd", Observed: u,
		RequiredInputs: []string{"model_revision", "runtime_revision", "reviewed_launch_configuration", "concurrency", "readiness_probe", "telemetry_reference", "measured_memory_peaks"}}
}

type boundedOutput struct{ bytes.Buffer }

func (b *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > maxInput-b.Len() {
		return 0, fmt.Errorf("systemd response too large")
	}
	return b.Buffer.Write(p)
}

// InspectUnit queries fixed properties without a shell, sudo, or lifecycle operations.
func InspectUnit(ctx context.Context, unit string) (Unit, error) {
	if !ValidUnit(unit) {
		return Unit{}, fmt.Errorf("expected a literal .service or .timer unit name")
	}
	var out boundedOutput
	cmd := exec.CommandContext(ctx, "systemctl", "--no-pager", "show", "--property="+strings.Join(showProperties, ","), "--", unit)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return Unit{}, fmt.Errorf("systemctl inspection: %w", ctx.Err())
		}
		return Unit{}, fmt.Errorf("systemctl show failed; verify systemd is available and the unit exists")
	}
	return ReadUnit(unit, &out)
}

// ReadUnit parses a synthetic or captured systemctl show response. Unknown keys
// are skipped, not retained; allowed fields are restricted to enums and counters.
func ReadUnit(unit string, r io.Reader) (Unit, error) {
	if !ValidUnit(unit) {
		return Unit{}, fmt.Errorf("expected a literal .service or .timer unit name")
	}
	u := Unit{SchemaVersion: 1, Kind: "systemd_unit_observation", Unit: unit}
	limited := &io.LimitedReader{R: r, N: maxInput + 1}
	scanner := bufio.NewScanner(limited)
	seen := map[string]bool{}
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), "=")
		if !ok || !slices.Contains(showProperties, key) {
			continue
		}
		if seen[key] {
			return Unit{}, fmt.Errorf("duplicate systemd property %s", key)
		}
		seen[key] = true
		switch key {
		case "LoadState":
			if !slices.Contains([]string{"loaded", "not-found", "masked", "error", "bad-setting", "merged", "stub"}, value) {
				return Unit{}, fmt.Errorf("invalid LoadState")
			}
			u.LoadState = value
		case "ActiveState":
			if !slices.Contains([]string{"active", "inactive", "failed", "activating", "deactivating", "reloading", "refreshing", "maintenance"}, value) {
				return Unit{}, fmt.Errorf("invalid ActiveState")
			}
			u.ActiveState = value
		case "UnitFileState":
			if !slices.Contains([]string{"", "enabled", "enabled-runtime", "linked", "linked-runtime", "alias", "masked", "masked-runtime", "static", "disabled", "indirect", "generated", "transient", "bad"}, value) {
				return Unit{}, fmt.Errorf("invalid UnitFileState")
			}
			u.UnitFileState = value
		case "MemoryCurrent", "MemoryMax":
			if key == "MemoryMax" && value == "infinity" {
				u.MemoryMaxUnlimited = true
				continue
			}
			if value == "" || value == "[not set]" || value == "18446744073709551615" {
				continue
			}
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil || n > maxBytes {
				return Unit{}, fmt.Errorf("invalid systemd property %s", key)
			}
			if key == "MemoryCurrent" {
				u.MemoryCurrent = &n
			} else {
				u.MemoryMax = &n
			}
		}
	}
	if err := scanner.Err(); err != nil || limited.N == 0 {
		return Unit{}, fmt.Errorf("cannot read bounded systemd input")
	}
	if !seen["LoadState"] || !seen["ActiveState"] {
		return Unit{}, fmt.Errorf("missing LoadState or ActiveState")
	}
	return u, nil
}

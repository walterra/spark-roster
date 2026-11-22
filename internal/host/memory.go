// Package host reads Linux host observations without changing host state.
package host

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const maxBytes = uint64(1<<53 - 1)
const maxInput = 64 << 10

// Memory is a host observation, not a planner snapshot with a service inventory.
type Memory struct {
	SchemaVersion int       `json:"schema_version"`
	Kind          string    `json:"kind"`
	CapturedAt    time.Time `json:"captured_at"`
	Total         uint64    `json:"total_bytes"`
	Available     uint64    `json:"available_bytes"`
	SwapTotal     *uint64   `json:"swap_total_bytes,omitempty"`
	SwapFree      *uint64   `json:"swap_free_bytes,omitempty"`
}

// ReadMemory parses Linux meminfo. Its kB counters use 1024-byte units.
func ReadMemory(r io.Reader, at time.Time) (Memory, error) {
	limited := &io.LimitedReader{R: r, N: maxInput + 1}
	scanner := bufio.NewScanner(limited)
	values := map[string]uint64{}
	for scanner.Scan() {
		key, rest, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue
		}
		switch key {
		case "MemTotal", "MemAvailable", "SwapTotal", "SwapFree":
		default:
			continue
		}
		parts := strings.Fields(rest)
		if len(parts) != 2 || parts[1] != "kB" {
			return Memory{}, fmt.Errorf("invalid %s: expected unsigned kB counter", key)
		}
		if _, ok := values[key]; ok {
			return Memory{}, fmt.Errorf("duplicate %s", key)
		}
		n, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || n > maxBytes/1024 {
			return Memory{}, fmt.Errorf("invalid %s: counter out of range", key)
		}
		values[key] = n * 1024
	}
	if err := scanner.Err(); err != nil || limited.N == 0 {
		return Memory{}, fmt.Errorf("cannot read bounded meminfo input")
	}
	total, hasTotal := values["MemTotal"]
	available, hasAvailable := values["MemAvailable"]
	if !hasTotal || total == 0 || !hasAvailable || available > total {
		return Memory{}, fmt.Errorf("missing or inconsistent MemTotal/MemAvailable")
	}
	m := Memory{SchemaVersion: 1, Kind: "linux_memory_observation", CapturedAt: at.UTC(), Total: total, Available: available}
	if n, ok := values["SwapTotal"]; ok {
		m.SwapTotal = &n
	}
	if n, ok := values["SwapFree"]; ok {
		m.SwapFree = &n
	}
	if m.SwapTotal != nil && m.SwapFree != nil && *m.SwapFree > *m.SwapTotal {
		return Memory{}, fmt.Errorf("SwapFree exceeds SwapTotal")
	}
	return m, nil
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/walterra/spark-roster/internal/host"
	"github.com/walterra/spark-roster/internal/planner"
)

func TestSnapshotToPlan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inventory.json")
	raw := `{"schema_version":1,"kind":"systemd_inventory","services":{"coding":{"profile_id":"stock-coding-v1","units":["engine.service","proxy.service"]}}}`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	var out, diag bytes.Buffer
	var calls []string
	inspect := func(_ context.Context, name string) (host.Unit, error) {
		calls = append(calls, name)
		n := uint64(11 << 20)
		return host.Unit{Unit: name, LoadState: "loaded", ActiveState: "active", MemoryCurrent: &n}, nil
	}
	memory := func() (host.Memory, error) {
		calls = append(calls, "memory")
		return host.ReadMemory(strings.NewReader("MemTotal: 126877696 kB\nMemAvailable: 29360128 kB\nSwapFree: 6291456 kB\n"), time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC))
	}
	if code := runSnapshot([]string{"--inventory", path}, &out, &diag, inspect, memory); code != 0 || diag.Len() != 0 {
		t.Fatalf("exit %d: %s", code, &diag)
	}
	if !reflect.DeepEqual(calls, []string{"engine.service", "proxy.service", "memory"}) {
		t.Fatalf("bad probe order %v", calls)
	}
	var got planner.Snapshot
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Running) != 1 || *got.Available != 28<<30 || got.Running["coding"] != (planner.RunningService{ProfileID: "stock-coding-v1"}) {
		t.Fatalf("bad snapshot: %+v", got)
	}
	snapshot := filepath.Join(dir, "snapshot.json")
	if err := os.WriteFile(snapshot, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	var d planner.DeploymentSet
	if err := readJSON(filepath.Join(fixture, "deployment-set.json"), &d); err != nil {
		t.Fatal(err)
	}
	d.Services = map[string]string{"coding": "stock-coding-v1", "coding-pi": "candidate-coding-v1"}
	d.Protected = []string{"coding"}
	set := filepath.Join(dir, "set.json")
	rawSet, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(set, rawSet, 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	args := []string{"plan", "--catalog", filepath.Join(fixture, "catalog.json"), "--deployment-set", set, "--snapshot", snapshot, "--as-of", "2026-11-22T12:00:00Z", "--json"}
	if code := run(args, &out, &diag, time.Time{}); code != 0 || diag.Len() != 0 {
		t.Fatalf("exit %d: %s", code, &diag)
	}
	var result planner.Result
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Verdict != "unknown" || len(result.Paths) != 1 || len(result.Paths[0].StopBeforeLaunch) != 0 || !reflect.DeepEqual(result.Paths[0].Reasons, []planner.Reason{{Code: "missing_running_growth", Subject: "coding"}}) {
		t.Fatalf("bad plan: %s", &out)
	}
}

func TestSnapshotFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, units, want string
		probes            int
	}{
		{"duplicate", `["engine.service","engine.service"]`, "duplicate unit", 0},
		{"nonliteral", `["*.service"]`, "invalid or duplicate unit", 0},
		{"inactive", `["engine.service"]`, "inventory drift", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inventory.json")
			raw := `{"schema_version":1,"kind":"systemd_inventory","services":{"coding":{"profile_id":"stock","units":` + tc.units + `}}}`
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			var out, diag bytes.Buffer
			probes := 0
			inspect := func(_ context.Context, unit string) (host.Unit, error) {
				probes++
				return host.Unit{Unit: unit, LoadState: "loaded", ActiveState: "inactive"}, nil
			}
			memory := func() (host.Memory, error) {
				t.Fatal("memory read after invalid inventory or drift")
				return host.Memory{}, nil
			}
			if code := runSnapshot([]string{"--inventory", path}, &out, &diag, inspect, memory); code != 1 || out.Len() != 0 || !strings.Contains(diag.String(), tc.want) || probes != tc.probes {
				t.Fatalf("exit %d, probes %d, stdout %q, stderr %q", code, probes, &out, &diag)
			}
		})
	}
}

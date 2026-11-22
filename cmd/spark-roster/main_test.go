package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/walterra/spark-roster/internal/planner"
)

const GiB = int64(1 << 30)
const fixture = "../../examples/coding-replacement"

func TestPlanVerdictsAndOutputParity(t *testing.T) {
	tests := []struct {
		name, verdict string
		available     int64
		change        func(*planner.Snapshot, *planner.Catalog, *planner.DeploymentSet)
	}{
		{name: "overlap fits", verdict: "fit", available: 56},
		{name: "replacement keeps protected services", verdict: "stop-first", available: 28},
		{name: "neither path fits", verdict: "insufficient", available: 10},
		{name: "missing retained growth", verdict: "unknown", available: 28, change: func(s *planner.Snapshot, _ *planner.Catalog, _ *planner.DeploymentSet) {
			r := s.Running["embeddings"]
			r.Growth = nil
			s.Running["embeddings"] = r
		}},
		{name: "unverified estimate", verdict: "unknown", available: 56, change: func(_ *planner.Snapshot, c *planner.Catalog, _ *planner.DeploymentSet) {
			c.Profiles["candidate-coding-v1"].Memory.Evidence = "estimated"
		}},
		{name: "stale snapshot", verdict: "unknown", available: 56, change: func(s *planner.Snapshot, _ *planner.Catalog, _ *planner.DeploymentSet) {
			s.CapturedAt = s.CapturedAt.Add(-time.Hour)
		}},
		{name: "protected replacement refused", verdict: "unknown", available: 56, change: func(_ *planner.Snapshot, _ *planner.Catalog, d *planner.DeploymentSet) {
			d.Protected = append(d.Protected, "coding")
		}},
		{name: "missing reclaim does not veto overlap", verdict: "fit", available: 56, change: func(s *planner.Snapshot, _ *planner.Catalog, _ *planner.DeploymentSet) {
			r := s.Running["coding"]
			r.Reclaim = nil
			s.Running["coding"] = r
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var c planner.Catalog
			var d planner.DeploymentSet
			var s planner.Snapshot
			for name, v := range map[string]any{"catalog": &c, "deployment-set": &d, "host-snapshot": &s} {
				if err := readJSON(filepath.Join(fixture, name+".json"), v); err != nil {
					t.Fatal(err)
				}
			}
			*s.Available = tc.available * GiB
			if tc.change != nil {
				tc.change(&s, &c, &d)
			}
			dir := t.TempDir()
			args := []string{"plan", "--as-of", "2026-11-22T12:00:00Z"}
			for _, item := range []struct {
				name string
				data any
			}{{"catalog", c}, {"deployment-set", d}, {"snapshot", s}} {
				file := filepath.Join(dir, item.name+".json")
				b, err := json.Marshal(item.data)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(file, b, 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--"+item.name, file)
			}
			var out, diag bytes.Buffer
			if code := run(append(append([]string{}, args...), "--json"), &out, &diag, time.Time{}); code != 0 || diag.Len() != 0 {
				t.Fatalf("exit %d: %s", code, &diag)
			}
			var r planner.Result
			if err := json.Unmarshal(out.Bytes(), &r); err != nil {
				t.Fatal(err)
			}
			if r.SchemaVersion != 1 || r.Verdict != tc.verdict {
				t.Fatalf("got %d / %s, want v1 / %s", r.SchemaVersion, r.Verdict, tc.verdict)
			}
			out.Reset()
			if code := run(args, &out, &diag, time.Time{}); code != 0 || diag.Len() != 0 {
				t.Fatalf("exit %d: %s", code, &diag)
			}
			if out.String() != human(r) || !strings.HasPrefix(out.String(), strings.ToUpper(tc.verdict)+":") {
				t.Fatalf("human report differs: %s", &out)
			}
			if tc.verdict == "stop-first" {
				if len(r.Paths) != 2 || *r.Paths[0].Phases[0].HeadroomBytes != -24*GiB || *r.Paths[1].Phases[0].HeadroomBytes != 10*GiB {
					t.Fatalf("unexpected budgets: %s", &out)
				}
				if !reflect.DeepEqual(r.Paths[1].StopBeforeLaunch, []string{"coding"}) || !reflect.DeepEqual(r.Protected, []string{"decisions", "embeddings"}) {
					t.Fatalf("unexpected stop list: %s", &out)
				}
			}
		})
	}
}

func TestUsageAndInputFailures(t *testing.T) {
	var out, diag bytes.Buffer
	if code := run([]string{"plan", "--json"}, &out, &diag, time.Time{}); code != 2 || out.Len() != 0 || diag.Len() == 0 {
		t.Fatalf("bad usage handling: %d %q %q", code, &out, &diag)
	}
	file := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(file, []byte(`{"schema_version":1,"unexpected":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	diag.Reset()
	if code := run([]string{"plan", "--catalog", file, "--deployment-set", file, "--snapshot", file, "--json"}, &out, &diag, time.Time{}); code != 1 || out.Len() != 0 || !strings.Contains(diag.String(), "unknown field") {
		t.Fatalf("bad input handling: %d %q %q", code, &out, &diag)
	}
}

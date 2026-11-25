package profile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/walterra/spark-roster/internal/planner"
)

func TestBuiltinsConvertToPlannerCatalogWithPins(t *testing.T) {
	set, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	c, err := Catalog(set)
	if err != nil {
		t.Fatal(err)
	}
	q := c.Profiles["qwen3-8-27b-dflash2"]
	if q.Model.Name != "RadixArk/Qwen3.8-27B-NVFP4" || q.Model.Revision != "52d1adc5f38aa5ebf099c29ed7025ba34cfbb854" {
		t.Fatalf("model %+v", q.Model)
	}
	if q.Drafter == nil || q.Drafter.Revision != "bd7a934213c47a9e7ef69eef36bb3325f47fd1f1" {
		t.Fatalf("drafter %+v", q.Drafter)
	}
	if q.Runtime.Revision != "sha256:e3a4f06001a29542fc4a8638536d9ffc5eb9c9942b08603c09b19a56eae4a1ae" || q.Concurrency != 8 || *q.ContextTokens != 262144 {
		t.Fatalf("runtime/concurrency %+v", q)
	}
	pi := c.Profiles["qwen3-8-27b-pi-q8"]
	if pi.Model.Revision != "bcb2b967901e772e76842bee0c67bc92f27c819c0ce7f14642e17d64aadd4d21" || pi.Runtime.Revision != "b1b770d90d94cad9e9ea5b993bdbc383a522fb28" || pi.Drafter != nil {
		t.Fatalf("pi %+v", pi)
	}
	if *q.Memory.Startup != 90<<30 || *q.Memory.Steady != 90<<30 || q.Memory.Evidence != "estimated" {
		t.Fatalf("memory %+v", q.Memory)
	}
}

func TestCatalogFeedsPlannerAndEstimatesStayUnverified(t *testing.T) {
	set, _ := Builtin()
	c, err := Catalog(set)
	if err != nil {
		t.Fatal(err)
	}
	total, avail, reserve := int64(128<<30), int64(120<<30), int64(10<<30)
	zero := int64(0)
	now := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	res, err := planner.Build(c,
		planner.DeploymentSet{Header: planner.Header{SchemaVersion: 1, Kind: "deployment_set"}, ID: "x", Services: map[string]string{"coding": "inkling-2-mini"}, Protected: []string{}, Reserve: &reserve},
		planner.Snapshot{Header: planner.Header{SchemaVersion: 1, Kind: "host_snapshot"}, CapturedAt: now, Total: &total, Available: &avail, Running: map[string]planner.RunningService{"other": {ProfileID: "qwen3-8-27b-pi-q8", Growth: &zero, Reclaim: &zero}}},
		now)
	if err != nil {
		t.Fatal(err)
	}
	// Profile floors are reviewed estimates, so the planner must not call this a fit.
	if res.Verdict == "fit" || res.Verdict == "stop-first" {
		t.Fatalf("an estimated floor produced a confident verdict: %s %s", res.Verdict, res.ReasonCode)
	}
}

func TestToPlannerNeedsModelAndRuntimeRoles(t *testing.T) {
	p := launched("process")
	if _, err := ToPlanner(p); err == nil || !strings.Contains(err.Error(), "role model and role runtime") {
		t.Fatalf("err=%v", err)
	}
	set := map[string]Profile{"pi-q8": p}
	if _, err := Catalog(set); err == nil {
		t.Fatal("unplannable profile silently dropped")
	}
}

func writeProfile(t *testing.T, dir, name, doc string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadAddsSiteProfilesAndRefusesReplacingABuiltin(t *testing.T) {
	dir := t.TempDir()
	writeProfile(t, dir, "site-qwen.json", goodAdopt)
	set, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := set["site-qwen"]; !ok || len(set) != 4 {
		t.Fatalf("ids %v", IDs(set))
	}
	clash := strings.Replace(goodAdopt, `"id":"site-qwen"`, `"id":"inkling-2-mini"`, 1)
	writeProfile(t, dir, "inkling-2-mini.json", clash)
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "cannot replace another profile") {
		t.Fatalf("err=%v", err)
	}
}

func TestLoadToleratesMissingDirectoryAndRejectsBadFiles(t *testing.T) {
	if set, err := Load(filepath.Join(t.TempDir(), "none")); err != nil || len(set) != 3 {
		t.Fatalf("err=%v n=%d", err, len(set))
	}
	if set, err := Load(""); err != nil || len(set) != 3 {
		t.Fatalf("err=%v", err)
	}
	dir := t.TempDir()
	writeProfile(t, dir, "bad.json", `{"schema_version":1}`)
	if _, err := Load(dir); err == nil {
		t.Fatal("invalid site profile accepted")
	}
	dir2 := t.TempDir()
	writeProfile(t, dir2, "wrong-name.json", goodAdopt)
	if _, err := Load(dir2); err == nil || !strings.Contains(err.Error(), "does not match the file name") {
		t.Fatalf("err=%v", err)
	}
}

// SPARK_ROSTER_SITE_PROFILES points at a directory of site profiles, for
// example a private deployment repo. The test is skipped when it is unset.
func TestSiteProfilesLoadAndPlan(t *testing.T) {
	dir := os.Getenv("SPARK_ROSTER_SITE_PROFILES")
	if dir == "" {
		t.Skip("SPARK_ROSTER_SITE_PROFILES not set")
	}
	set, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(set) <= 3 {
		t.Fatalf("no site profiles found in %s", dir)
	}
	if _, err := Catalog(set); err != nil {
		t.Fatal(err)
	}
	for id, p := range set {
		if p.Adopt != nil && len(p.Units()) == 0 {
			t.Errorf("%s adopts no units", id)
		}
	}
}

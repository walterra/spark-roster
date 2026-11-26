package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/walterra/spark-roster/internal/host"
	"github.com/walterra/spark-roster/internal/profile"
)

// stubBench replaces the Pi runner and the speed and memory probes, and restores them.
type benchStub struct {
	tasksRun []string
	taskErr  func(profileModel string, n int) error
	outcome  func(profileModel string) string
	onTask   func(profileModel string)
}

func stubBenchSeams(t *testing.T) *benchStub {
	t.Helper()
	st := &benchStub{outcome: func(string) string { return "pass" }}
	oldT, oldI, oldS, oldM := runTask, runnerImage, runSpeed, benchMemory
	runTask = func(_ context.Context, _ string, model, _ string, _ uint16, task piTask, rep int) (piResult, error) {
		st.tasksRun = append(st.tasksRun, model+"/"+task.ID)
		if st.onTask != nil {
			st.onTask(model)
		}
		var err error
		if st.taskErr != nil {
			err = st.taskErr(model, len(st.tasksRun))
		}
		return piResult{Task: task.ID, Repetition: rep, Outcome: st.outcome(model)}, err
	}
	runnerImage = func(context.Context) (string, error) { return "sha256:runner", nil }
	runSpeed = func(context.Context, uint16, string, string, string) []benchSpeed { return nil }
	benchMemory = func() (host.Memory, error) { return host.Memory{Available: 90 << 30, Total: 128 << 30}, nil }
	t.Cleanup(func() { runTask, runnerImage, runSpeed, benchMemory = oldT, oldI, oldS, oldM })
	return st
}

const (
	pBase = "qwen3-8-27b-pi-q8"
	pC1   = "inkling-2-mini"
	pC2   = "qwen3-8-27b-dflash2"
)

func workerArgs(x *fx, extra ...string) []string {
	args := []string{"benchmark", "worker", "--operator", "operator", "--baseline", pBase, "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles}
	return append(args, extra...)
}

func (x *fx) runWorker(args ...string) (int, string, string) {
	var out, diag bytes.Buffer
	e := x.env()
	e.euid = 0
	e.getenv = func(k string) string {
		if k == "INVOCATION_ID" {
			return "inv"
		}
		return ""
	}
	code, ok := manage(context.Background(), args, &out, &diag, e)
	if !ok {
		x.t.Fatalf("%v not handled", args)
	}
	return code, out.String(), diag.String()
}

func (x *fx) readLatest() benchReport {
	x.t.Helper()
	b, err := os.ReadFile(filepath.Join(x.dir, "data", "benchmarks", "latest.json"))
	if err != nil {
		x.t.Fatal(err)
	}
	var r benchReport
	if err := json.Unmarshal(b, &r); err != nil {
		x.t.Fatal(err)
	}
	return r
}

func baselineRunning(x *fx) {
	x.h.running[pBase] = true
	_ = profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}.SetActive(pBase)
}

func TestBudgetIsTheSumOfEnforcedLimits(t *testing.T) {
	set, _ := profile.Builtin()
	b := benchBudget(set, []string{pBase, pC1}, pBase, false)
	_, tasks, _ := benchInputs()
	wantAttempts := time.Duration(2*len(tasks)*benchRepeats) * taskLimit
	if b.Attempts != wantAttempts {
		t.Fatalf("attempts %v want %v", b.Attempts, wantAttempts)
	}
	if b.Speed != 2*benchSpeedProbes*benchProbeLimit {
		t.Fatalf("speed %v", b.Speed)
	}
	// Only the profile that has to be started adds a switch allowance; the baseline is already up.
	if b.Switches != set[pC1].Serving.ReadyTimeout.Duration+benchSwitchSlack || b.Candidates != 1 {
		t.Fatalf("switches %v candidates %d", b.Switches, b.Candidates)
	}
	if b.Restore != set[pBase].Serving.ReadyTimeout.Duration+benchSwitchSlack {
		t.Fatalf("restore %v", b.Restore)
	}
	if b.Total != b.Attempts+b.Speed+b.Switches+b.Restore+b.Margin {
		t.Fatalf("total %v is not the sum", b.Total)
	}
	// The unit must outlive the worker's deadline plus its own restoration, or systemd cuts the restore short.
	if b.systemdCeiling() <= b.Total+b.Restore || b.stopBudget() < b.Restore {
		t.Fatalf("ceiling %v stop %v total %v restore %v", b.systemdCeiling(), b.stopBudget(), b.Total, b.Restore)
	}
	g := benchBudget(set, []string{pBase, pC1}, pBase, true)
	if g.Attempts != 2*taskLimit || g.Total >= b.Total {
		t.Fatalf("gate budget %+v", g)
	}
	// A profile listed twice in a row is only started once.
	if d := benchBudget(set, []string{pC1, pC1}, pBase, false); d.Candidates != 1 {
		t.Fatalf("candidates %d", d.Candidates)
	}
}

func TestLaunchPrintsPasteSafeScriptWithHashCeilingAndRecoveryHook(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	code, out, _ := x.run("benchmark", "launch", pBase, pC1, "--json")
	doc := decode(t, out)
	if code != exitOK || doc["outcome"] != "ready" || doc["baseline"] != pBase {
		t.Fatalf("%d %v", code, doc)
	}
	script := doc["script"].(string)
	sha := strings.Repeat("ab", 32)
	for _, want := range []string{
		"sha=" + sha, "sha256sum -c", "sudo install -o root -g root -m 0755", "/usr/local/sbin/spark-roster-" + sha,
		"'--property=Type=exec'", "RuntimeMaxSec=", "TimeoutStopSec=", "ExecStopPost=", "recover", "--yes", "--operator", "benchmark worker",
		"--baseline", "'" + pBase + "'", "'" + pC1 + "'", `"${run_args[@]}"`, `"${worker[@]}"`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q\n%s", want, script)
		}
	}
	// The recovery hook runs as root and must be able to find the same state and profiles.
	hook := script[strings.Index(script, "ExecStopPost="):]
	hook = hook[:strings.Index(hook, "\n")]
	for _, want := range []string{"--data", "--profiles", "--operator", "operator"} {
		if !strings.Contains(hook, want) {
			t.Errorf("hook lacks %q: %s", want, hook)
		}
	}
	if len(x.h.log) != 0 {
		t.Fatalf("launch touched the host: %v", x.h.log)
	}
	if bud := doc["budget"].(map[string]any); bud["total"].(float64) <= 0 {
		t.Fatalf("%v", bud)
	}
	if dt := toStrings(doc["downtime"]); len(dt) != 2 || !strings.Contains(dt[0], pBase) || !strings.Contains(dt[1], pC1) {
		t.Fatalf("downtime %v", dt)
	}
	if strings.Contains(script, "--gate") {
		t.Fatal("gate flag in a full run")
	}
	_, out, _ = x.run("benchmark", "launch", pBase, "--gate", "--json")
	if !strings.Contains(decode(t, out)["script"].(string), "--gate") {
		t.Fatal("gate flag lost")
	}
}

// Benchmarking only the baseline leaves it serving. Saying it is stopped would make an operator
// expect downtime that never happens, and trust a restore step that has nothing to restore.
func TestLaunchDowntimeSaysWhatReallyStops(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	_, out, _ := x.run("benchmark", "launch", pBase, "--gate", "--json")
	dt := strings.Join(toStrings(decode(t, out)["downtime"]), " | ")
	if !strings.Contains(dt, "no model downtime") || strings.Contains(dt, "is stopped while") {
		t.Fatalf("baseline-only run: %q", dt)
	}
	_, out, _ = x.run("benchmark", "launch", pBase, pC1, "--json")
	dt = strings.Join(toStrings(decode(t, out)["downtime"]), " | ")
	if !strings.Contains(dt, pBase+" is stopped while the other profiles run") || !strings.Contains(dt, pC1+" is started and stopped") || strings.Contains(dt, "no model downtime") {
		t.Fatalf("two-profile run: %q", dt)
	}
	// A running timer a profile lists is reported, because it will be stopped.
	x.h.unitsActive = map[string]bool{"canary.timer": true}
	if err := os.MkdirAll(x.profiles, 0o755); err != nil {
		t.Fatal(err)
	}
	base, _ := profile.Builtin()
	p := base[pC1]
	p.ID, p.PauseDuringBenchmark = "site-ink", []string{"canary.timer"}
	b, _ := json.Marshal(p)
	_ = os.WriteFile(filepath.Join(x.profiles, "site-ink.json"), b, 0o600)
	_, out, _ = x.run("benchmark", "launch", "site-ink", "--json")
	if dt = strings.Join(toStrings(decode(t, out)["downtime"]), " | "); !strings.Contains(dt, "canary.timer is stopped during the run and started again after") {
		t.Fatalf("paused unit not announced: %q", dt)
	}
}

func TestLaunchRefusesWithoutChangingAnythingAndNamesTheFix(t *testing.T) {
	cases := map[string]struct {
		setup func(*fx)
		args  []string
		want  string
	}{
		"nothing serving but a profile expected": {func(x *fx) {
			_ = profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}.SetActive(pBase)
		}, []string{pC1}, "none of its units are running"},
		"unstaged candidate": {func(x *fx) { baselineRunning(x); x.h.staged[pC1] = errors.New("weights missing") }, []string{pC1}, "spark-roster stage " + pC1},
		"port busy":          {func(x *fx) { baselineRunning(x); x.portErr = errors.New("in use") }, []string{pC1}, "port 9932"},
		"unknown profile":    {func(x *fx) { baselineRunning(x) }, []string{"nope"}, "unknown profile"},
		"duplicate":          {func(x *fx) { baselineRunning(x) }, []string{pC1, pC1}, "listed twice"},
		"unsettled": {func(x *fx) {
			baselineRunning(x)
			j := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}
			_ = j.StartBench(profile.BenchRecord{ID: "old", Baseline: pBase})
		}, []string{pC1}, "spark-roster recover"},
		"two running": {func(x *fx) { x.h.running[pBase], x.h.running[pC1] = true, true }, []string{pC2}, "more than one profile is running"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stubBenchSeams(t)
			x := newFx(t)
			tc.setup(x)
			code, _, diag := x.run(append([]string{"benchmark", "launch"}, tc.args...)...)
			if code != exitPrecondition || !strings.Contains(diag, tc.want) {
				t.Fatalf("%d %q", code, diag)
			}
			if len(x.h.log) != 0 {
				t.Fatalf("host touched: %v", x.h.log)
			}
		})
	}
	x := newFx(t)
	stubBenchSeams(t)
	runnerImage = func(context.Context) (string, error) {
		return "", errors.New("Pi runner image missing; run benchmark stage")
	}
	baselineRunning(x)
	if code, _, diag := x.run("benchmark", "launch", pC1); code != exitPrecondition || !strings.Contains(diag, "benchmark stage") {
		t.Fatalf("%d %q", code, diag)
	}
}

func TestLaunchRefusedAsRootAndWithWorkerFlags(t *testing.T) {
	x := newFx(t)
	baselineRunning(x)
	x.euid = 0
	if code, _, _ := x.run("benchmark", "launch", pC1); code != exitUsage {
		t.Fatalf("root launch accepted: %d", code)
	}
	x.euid = 1000
	if code, _, _ := x.run("benchmark", "launch", pC1, "--operator", "operator"); code != exitUsage {
		t.Fatalf("worker flag accepted on launch: %d", code)
	}
	if code, _, _ := x.run("benchmark", "launch"); code != exitUsage {
		t.Fatalf("launch without profiles accepted: %d", code)
	}
}

func TestWorkerRunsEveryProfileThenRestoresBaselineAndRecordsDefinitions(t *testing.T) {
	x := newFx(t)
	stub := stubBenchSeams(t)
	baselineRunning(x)
	code, out, diag := x.runWorker(workerArgs(x, pBase, pC1)...)
	if code != exitOK {
		t.Fatalf("%d\n%s\n%s", code, out, diag)
	}
	_, tasks, _ := benchInputs()
	if len(stub.tasksRun) != 2*len(tasks)*benchRepeats {
		t.Fatalf("ran %d attempts", len(stub.tasksRun))
	}
	// Baseline is measured first without a switch; the candidate follows; the baseline is brought back last.
	if want := []string{"stop " + pBase, "start " + pC1, "stop " + pC1, "start " + pBase}; !slices.Equal(x.h.log, want) {
		t.Fatalf("log %v", x.h.log)
	}
	r := x.readLatest()
	if r.Schema != 3 || r.Run != "completed" || r.Restoration != "verified" || r.RestoredAt == nil || r.Baseline != pBase || r.Mode != "benchmark" {
		t.Fatalf("%+v", r)
	}
	if r.PiContext != scoredWindow || r.PiOutputCap != scoredOutput || !strings.HasPrefix(r.Tool, "spark-roster") || r.Host["platform"] != "linux/arm64" {
		t.Fatalf("settings not recorded: %+v", r)
	}
	if len(r.Defs) != 2 || r.Defs[pC1].Launch == nil || r.Defs[pC1].Launch.Args[0] != "serve" {
		t.Fatalf("definitions not recorded: %v", r.Defs)
	}
	// The report carries the profile as written, with variables, so another machine can repeat it.
	if strings.Contains(strings.Join(r.Defs[pC1].Launch.Mounts, " "), x.dir) || !strings.Contains(strings.Join(r.Defs[pC1].Launch.Mounts, " "), "{data}") {
		t.Fatalf("definition holds this machine's paths: %v", r.Defs[pC1].Launch.Mounts)
	}
	if len(r.Profiles) != 2 || r.Profiles[0].Profile != pBase || len(r.Profiles[1].Tasks) != len(tasks)*benchRepeats {
		t.Fatalf("%+v", r.Profiles)
	}
	st := x.journalState()
	if st.Active != pBase || st.Bench != nil || st.Unsettled() {
		t.Fatalf("journal not settled: %+v", st)
	}
	if !x.h.running[pBase] || x.h.running[pC1] {
		t.Fatalf("running %v", x.h.running)
	}
	if !strings.Contains(out, "baseline "+pBase+" verified") {
		t.Fatalf("%s", out)
	}
}

func TestWorkerPausesTimerBeforeTimingAndResumesItOnlyAfterBaselineServes(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	x.h.unitsActive = map[string]bool{"canary.timer": true}
	if err := os.MkdirAll(x.profiles, 0o755); err != nil {
		t.Fatal(err)
	}
	// A site profile asks for the canary to be paused while it is timed.
	base, _ := profile.Builtin()
	p := base[pBase]
	p.ID = "site-pi"
	p.PauseDuringBenchmark = []string{"canary.timer"}
	b, _ := json.Marshal(p)
	_ = os.WriteFile(filepath.Join(x.profiles, "site-pi.json"), b, 0o600)
	x.h.running[pBase] = false
	x.h.running["site-pi"] = true
	_ = profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}.SetActive("site-pi")
	var timerStoppedBeforeTask, timerRunningAtEnd bool
	stub := stubBenchSeams(t)
	stub.onTask = func(string) {
		if !x.h.unitsActive["canary.timer"] {
			timerStoppedBeforeTask = true
		}
	}
	args := []string{"benchmark", "worker", "--operator", "operator", "--baseline", "site-pi", "--gate", "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles, "site-pi"}
	if code, out, diag := x.runWorker(args...); code != exitOK {
		t.Fatalf("%d\n%s\n%s", code, out, diag)
	}
	timerRunningAtEnd = x.h.unitsActive["canary.timer"]
	if !timerStoppedBeforeTask || !timerRunningAtEnd {
		t.Fatalf("timer stopped during timing=%v running at end=%v log=%v", timerStoppedBeforeTask, timerRunningAtEnd, x.h.log)
	}
	if r := x.readLatest(); len(r.Paused) != 1 || r.Paused[0] != "canary.timer" || r.Mode != "gate" {
		t.Fatalf("%+v", r)
	}
}

func TestWorkerFailureStillRestoresBaselineAndExitsOne(t *testing.T) {
	x := newFx(t)
	stub := stubBenchSeams(t)
	baselineRunning(x)
	stub.taskErr = func(model string, n int) error {
		if model == "inkling-2-mini" {
			return errors.New("harness fault")
		}
		return nil
	}
	code, _, diag := x.runWorker(workerArgs(x, pBase, pC1)...)
	if code != exitError || !strings.Contains(diag, "harness fault") {
		t.Fatalf("%d %q", code, diag)
	}
	r := x.readLatest()
	if r.Run != "failed" || r.Restoration != "verified" {
		t.Fatalf("%+v", r)
	}
	if !x.h.running[pBase] || x.h.running[pC1] || x.journalState().Bench != nil {
		t.Fatalf("not restored: %v %+v", x.h.running, x.journalState())
	}
}

func TestWorkerReportsExitFiveWhenRestorationFails(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	x.h.startErr[pBase] = errors.New("baseline will not start")
	code, _, diag := x.runWorker(workerArgs(x, pBase, pC1)...)
	if code != exitUnsettled || !strings.Contains(diag, "restoration failed") {
		t.Fatalf("%d %q", code, diag)
	}
	r := x.readLatest()
	if r.Restoration != "failed" || r.Run != "failed" {
		t.Fatalf("%+v", r)
	}
	if st := x.journalState(); st.Bench == nil || !st.Unsettled() {
		t.Fatalf("record cleared although the baseline is down: %+v", st)
	}
}

func TestWorkerRestoresWhenCancelledMidRun(t *testing.T) {
	x := newFx(t)
	stub := stubBenchSeams(t)
	baselineRunning(x)
	ctx, cancel := context.WithCancel(context.Background())
	stub.onTask = func(model string) {
		if model == "inkling-2-mini" {
			cancel()
		}
	}
	var out, diag bytes.Buffer
	e := x.env()
	e.euid = 0
	e.getenv = func(k string) string {
		if k == "INVOCATION_ID" {
			return "inv"
		}
		return ""
	}
	code, _ := manage(ctx, workerArgs(x, pBase, pC1), &out, &diag, e)
	if code == exitOK {
		t.Fatalf("cancelled run reported success\n%s", out.String())
	}
	if !x.h.running[pBase] || x.h.running[pC1] || x.readLatest().Restoration != "verified" {
		t.Fatalf("running %v report %+v", x.h.running, x.readLatest())
	}
}

func TestWorkerRefusesToStartWithoutRootInvocationOrWhenPreflightFails(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	if code, _, diag := x.run(workerArgs(x, pC1)...); code != exitUsage || !strings.Contains(diag, "runs as root") {
		t.Fatalf("non-root worker: %d %q", code, diag)
	}
	var out, diag bytes.Buffer
	e := x.env()
	e.euid = 0
	if code, _ := manage(context.Background(), workerArgs(x, pC1), &out, &diag, e); code != exitUsage || !strings.Contains(diag.String(), "under systemd") {
		t.Fatalf("worker outside systemd: %d %q", code, diag.String())
	}
	x.h.staged[pC1] = errors.New("missing")
	code, _, d := x.runWorker(workerArgs(x, pC1)...)
	if code != exitError || !strings.Contains(d, "not staged") || len(x.h.log) != 0 {
		t.Fatalf("%d %q %v", code, d, x.h.log)
	}
	if _, err := os.Stat(filepath.Join(x.dir, "data", "benchmarks", "latest.json")); err == nil {
		t.Fatal("a refused run left a report")
	}
}

func TestWorkerWritesOperatorOwnedFilesAndUsesOperatorPaths(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	if code, out, diag := x.runWorker(workerArgs(x, pBase)...); code != exitOK {
		t.Fatalf("%d\n%s\n%s", code, out, diag)
	}
	var reports, state, lock bool
	for _, p := range x.chowned {
		if strings.HasSuffix(p, "/lock") {
			lock = true
		}
		if strings.HasSuffix(p, "latest.json") {
			reports = true
		}
		if strings.HasSuffix(p, "state.json") {
			state = true
		}
	}
	if !reports || !state || !lock {
		t.Fatalf("files not handed to the operator (report %v, state %v, lock %v): %v", reports, state, lock, x.chowned)
	}
	var out, diag bytes.Buffer
	e := x.env()
	e.euid = 0
	e.getenv = func(k string) string { return map[string]string{"INVOCATION_ID": "x"}[k] }
	if code, _ := manage(context.Background(), []string{"benchmark", "worker", "--operator", "nobody-here", "--baseline", pBase, pBase}, &out, &diag, e); code != exitUsage || !strings.Contains(diag.String(), "operator") {
		t.Fatalf("unknown operator accepted: %d %q", code, diag.String())
	}
	e.lookupUser = func(string) (OperatorInfo, error) { return OperatorInfo{Home: "/root", UID: 0}, nil }
	diag.Reset()
	if code, _ := manage(context.Background(), []string{"benchmark", "worker", "--operator", "operator", "--baseline", pBase, pBase}, &out, &diag, e); code != exitUsage {
		t.Fatalf("root operator accepted: %d", code)
	}
}

func TestRecoverHookAsRootRestoresBaselineAndResumesPausedUnits(t *testing.T) {
	x := newFx(t)
	// Killed mid-run: candidate up, baseline down, timer paused, benchmark record in the journal.
	x.h.running[pC1] = true
	j := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}
	_ = j.SetActive(pC1)
	if err := j.StartBench(profile.BenchRecord{ID: "run1", Baseline: pBase, Paused: []string{"canary.timer"}}); err != nil {
		t.Fatal(err)
	}
	x.h.unitsActive = map[string]bool{}
	var out, diag bytes.Buffer
	e := x.env()
	e.euid = 0
	code, _ := manage(context.Background(), []string{"recover", "--yes", "--operator", "operator", "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles}, &out, &diag, e)
	if code != exitOK {
		t.Fatalf("%d\n%s\n%s", code, out.String(), diag.String())
	}
	if x.h.running[pC1] || !x.h.running[pBase] || !x.h.unitsActive["canary.timer"] {
		t.Fatalf("running %v timer %v log %v", x.h.running, x.h.unitsActive, x.h.log)
	}
	if st := x.journalState(); st.Bench != nil || st.Active != pBase {
		t.Fatalf("%+v", st)
	}
	// --operator is only meaningful to recover.
	if code, _, _ := x.run("switch", pBase, "--operator", "operator", "--dry-run"); code != exitUsage {
		t.Fatalf("--operator accepted on switch: %d", code)
	}
}

func TestSwitchAndStopRefuseWhileABenchmarkOwnsTheHost(t *testing.T) {
	x := newFx(t)
	baselineRunning(x)
	j := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}
	if err := j.StartBench(profile.BenchRecord{ID: "run1", Baseline: pBase}); err != nil {
		t.Fatal(err)
	}
	code, out, _ := x.run("switch", pC1, "--yes", "--json")
	if code != exitPrecondition || !strings.Contains(decode(t, out)["error"].(string), "benchmark") || len(x.h.log) != 0 {
		t.Fatalf("%d %s %v", code, out, x.h.log)
	}
	if code, _, diag := x.run("stop", "--yes"); code != exitPrecondition || !strings.Contains(diag, "benchmark") || len(x.h.log) != 0 {
		t.Fatalf("%d %q %v", code, diag, x.h.log)
	}
	code, out, _ = x.run("status", "--json")
	if code != exitOK || !strings.Contains(decode(t, out)["benchmark"].(string), "run1") {
		t.Fatalf("%d %s", code, out)
	}
}

func writeReport(t *testing.T, dir, name string, r benchReport) string {
	t.Helper()
	b, _ := json.Marshal(r)
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func results(id string, passes, total int, byTask map[string]int, decode float64) benchResult {
	r := benchResult{Profile: id}
	n := 0
	for task, count := range byTask {
		for i := 0; i < count; i++ {
			r.Tasks = append(r.Tasks, piResult{Task: task, Repetition: i + 1, Outcome: "pass"})
			n++
		}
	}
	for n < total {
		r.Tasks = append(r.Tasks, piResult{Task: "fail-task", Repetition: n, Outcome: "fail"})
		n++
	}
	if decode > 0 {
		d := decode
		r.Speed = []benchSpeed{{Pair: 1, Cache: "cold-intended", DecodeEstimate: &d}}
	}
	return r
}

func TestCompareFlagsDifferentSettingsAndShowsTaskChanges(t *testing.T) {
	dir := t.TempDir()
	a := benchReport{Schema: 2, Suite: "s1", Repetitions: 3, TaskCount: 8, TaskLimitSec: 600, PiContext: 32768, PiOutputCap: 4096,
		Profiles: []benchResult{results("x", 2, 3, map[string]int{"t1": 2}, 40), results("only-a", 1, 1, map[string]int{"t1": 1}, 0)}}
	b := benchReport{Schema: 3, Suite: "s1", Repetitions: 3, TaskCount: 8, TaskLimitSec: 600, PiContext: 262144, PiOutputCap: 16384,
		Profiles: []benchResult{results("x", 3, 3, map[string]int{"t1": 3}, 48), results("only-b", 1, 1, map[string]int{"t2": 1}, 0)}}
	pa, pb := writeReport(t, dir, "a.json", a), writeReport(t, dir, "b.json", b)
	var out, diag bytes.Buffer
	if code := benchCompareFiles(pa, pb, true, &out, &diag); code != exitOK {
		t.Fatalf("%d %s", code, diag.String())
	}
	var d compareDoc
	if err := json.Unmarshal(out.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.Comparable || len(d.Differences) != 2 || !strings.Contains(strings.Join(d.Differences, ";"), "pi_context: 32768 vs 262144") || !strings.Contains(strings.Join(d.Differences, ";"), "pi_output_cap") {
		t.Fatalf("%+v", d)
	}
	byID := map[string]profileDiff{}
	for _, p := range d.Profiles {
		byID[p.Profile] = p
	}
	if byID["x"].In != "both" || byID["x"].PassA != 2 || byID["x"].PassB != 3 || byID["x"].TaskDeltas["t1"] != 1 || byID["x"].DecodeA != 40 || byID["x"].DecodeB != 48 {
		t.Fatalf("%+v", byID["x"])
	}
	if byID["only-a"].In != "first" || byID["only-b"].In != "second" {
		t.Fatalf("%+v %+v", byID["only-a"], byID["only-b"])
	}
	out.Reset()
	if code := benchCompareFiles(pa, pb, false, &out, &diag); code != exitOK || !strings.Contains(out.String(), "not directly comparable") || !strings.Contains(out.String(), "t1 +1") {
		t.Fatalf("%d %s", code, out.String())
	}
	same := writeReport(t, dir, "c.json", a)
	out.Reset()
	benchCompareFiles(pa, same, false, &out, &diag)
	if !strings.Contains(out.String(), "pass counts are comparable") {
		t.Fatalf("%s", out.String())
	}
}

func TestReportAndCompareReadFilesAndRejectOtherJSON(t *testing.T) {
	dir := t.TempDir()
	good := writeReport(t, dir, "g.json", benchReport{Schema: 3, Run: "completed", Restoration: "verified", Suite: "s", Repetitions: 1, TaskCount: 1, Profiles: []benchResult{results("x", 1, 1, map[string]int{"t": 1}, 0)}})
	bad := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(bad, []byte(`{"schema_version":9}`), 0o600)
	var out, diag bytes.Buffer
	if code := benchReportFile(good, &out, &diag); code != exitOK || !strings.Contains(out.String(), "| x |") {
		t.Fatalf("%d %s %s", code, out.String(), diag.String())
	}
	if code := benchReportFile(bad, &out, &diag); code != exitError || !strings.Contains(diag.String(), "not a benchmark report") {
		t.Fatalf("%d %s", code, diag.String())
	}
	if code := benchCompareFiles(good, filepath.Join(dir, "missing.json"), false, &out, &diag); code != exitError {
		t.Fatalf("%d", code)
	}
	x := newFx(t)
	if code, _, _ := x.run("benchmark", "compare", good); code != exitUsage {
		t.Fatalf("one-file compare accepted: %d", code)
	}
	if code, _, _ := x.run("benchmark", "report", good, bad); code != exitUsage {
		t.Fatalf("two-file report accepted: %d", code)
	}
}

func TestLegacyBenchmarkSubcommandsAreNotHandledByTheProfileCLI(t *testing.T) {
	for _, args := range [][]string{{"benchmark", "stage"}, {"benchmark", "run", "me"}, {"benchmark", "report"}, {"benchmark", "status"}, {"benchmark", "selftest"}} {
		if benchmarkManaged(args) {
			t.Errorf("%v claimed by the profile CLI", args)
		}
	}
	for _, args := range [][]string{{"benchmark", "launch", "x"}, {"benchmark", "worker"}, {"benchmark", "compare", "a", "b"}, {"benchmark", "report", "f.json"}} {
		if !benchmarkManaged(args) {
			t.Errorf("%v not claimed", args)
		}
	}
}

func TestWorkerRefusesWhenTheServingProfileIsNotTheNamedBaseline(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x) // pBase serves
	args := []string{"benchmark", "worker", "--operator", "operator", "--baseline", pC1, "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles, pC1}
	code, _, diag := x.runWorker(args...)
	if code != exitError || !strings.Contains(diag, "restores the profile that was serving") || len(x.h.log) != 0 {
		t.Fatalf("%d %q %v", code, diag, x.h.log)
	}
}

func TestWorkerWritesTheReportAndJournalRecordBeforeAnythingIsStopped(t *testing.T) {
	x := newFx(t)
	stub := stubBenchSeams(t)
	baselineRunning(x)
	x.h.unitsActive = map[string]bool{"canary.timer": true}
	if err := os.MkdirAll(x.profiles, 0o755); err != nil {
		t.Fatal(err)
	}
	base, _ := profile.Builtin()
	p := base[pBase]
	p.ID, p.PauseDuringBenchmark = "site-pi", []string{"canary.timer"}
	b, _ := json.Marshal(p)
	_ = os.WriteFile(filepath.Join(x.profiles, "site-pi.json"), b, 0o600)
	x.h.running[pBase], x.h.running["site-pi"] = false, true
	_ = profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}.SetActive("site-pi")
	var atFirstStop struct {
		report  bool
		journal *profile.BenchRecord
	}
	hook := &stopHook{fakeHost: x.h, onStop: func(unit string) {
		if atFirstStop.journal == nil {
			_, err := os.Stat(filepath.Join(x.dir, "data", "benchmarks", "latest.json"))
			atFirstStop.report = err == nil
			atFirstStop.journal = x.journalState().Bench
		}
	}}
	x.h2 = hook
	_ = stub
	args := []string{"benchmark", "worker", "--operator", "operator", "--baseline", "site-pi", "--gate", "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles, "site-pi"}
	if code, out, diag := x.runWorker(args...); code != exitOK {
		t.Fatalf("%d\n%s\n%s", code, out, diag)
	}
	if !atFirstStop.report || atFirstStop.journal == nil || atFirstStop.journal.Baseline != "site-pi" || len(atFirstStop.journal.Paused) != 1 {
		t.Fatalf("a kill at the first stop would not be recoverable: report=%v journal=%+v", atFirstStop.report, atFirstStop.journal)
	}
}

type stopHook struct {
	*fakeHost
	onStop func(unit string)
}

func (h *stopHook) ControlUnit(ctx context.Context, action, unit string) error {
	if action == "stop" {
		h.onStop(unit)
	}
	return h.fakeHost.ControlUnit(ctx, action, unit)
}

func TestWorkerDoesNotTimeAnythingWhileAPausedUnitIsStillRunning(t *testing.T) {
	x := newFx(t)
	stub := stubBenchSeams(t)
	baselineRunning(x)
	x.h.unitsActive = map[string]bool{"canary.timer": true}
	x.h.unitStuck = map[string]bool{"canary.timer": true}
	if err := os.MkdirAll(x.profiles, 0o755); err != nil {
		t.Fatal(err)
	}
	base, _ := profile.Builtin()
	p := base[pBase]
	p.ID, p.PauseDuringBenchmark = "site-pi", []string{"canary.timer"}
	b, _ := json.Marshal(p)
	_ = os.WriteFile(filepath.Join(x.profiles, "site-pi.json"), b, 0o600)
	x.h.running[pBase], x.h.running["site-pi"] = false, true
	_ = profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}.SetActive("site-pi")
	args := []string{"benchmark", "worker", "--operator", "operator", "--baseline", "site-pi", "--gate", "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles, "site-pi"}
	code, _, diag := x.runWorker(args...)
	if code != exitError || !strings.Contains(diag, "canary.timer is still active") || len(stub.tasksRun) != 0 {
		t.Fatalf("%d %q tasks=%v", code, diag, stub.tasksRun)
	}
}

// A cancellation or deadline that arrives during the very last attempt of the
// last profile must not be reported as a finished run.
func TestWorkerReportsFailureWhenCancelledDuringTheLastAttempt(t *testing.T) {
	x := newFx(t)
	stub := stubBenchSeams(t)
	baselineRunning(x)
	_, tasks, _ := benchInputs()
	total := len(tasks) * benchRepeats // the only profile is the baseline
	ctx, cancel := context.WithCancel(context.Background())
	stub.onTask = func(string) {
		if len(stub.tasksRun) == total {
			cancel()
		}
	}
	var out, diag bytes.Buffer
	e := x.env()
	e.euid = 0
	e.getenv = func(k string) string { return map[string]string{"INVOCATION_ID": "x"}[k] }
	code, _ := manage(ctx, workerArgs(x, pBase), &out, &diag, e)
	if code == exitOK || x.readLatest().Run == "completed" {
		t.Fatalf("a run cancelled in its last attempt was reported complete: code %d run %q", code, x.readLatest().Run)
	}
	if x.readLatest().Restoration != "verified" || !x.h.running[pBase] {
		t.Fatalf("not restored: %+v", x.readLatest())
	}
}

// The unit's lifetime and the recovery hook's time are what protect the restore. They must
// come from the budget, not merely be present, so test the numbers.
func TestLaunchUnitCeilingAndStopTimeComeFromTheBudget(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	_, out, _ := x.run("benchmark", "launch", pBase, pC1, "--json")
	doc := decode(t, out)
	set, _ := profile.Builtin()
	b := benchBudget(set, []string{pBase, pC1}, pBase, false)
	script := doc["script"].(string)
	wantCeiling := "RuntimeMaxSec=" + strconv.FormatInt(int64(b.systemdCeiling()/time.Second), 10) + "s"
	wantStop := "TimeoutStopSec=" + strconv.FormatInt(int64(b.stopBudget()/time.Second), 10) + "s"
	if !strings.Contains(script, wantCeiling) || !strings.Contains(script, wantStop) {
		t.Fatalf("want %s and %s in\n%s", wantCeiling, wantStop, script)
	}
	if b.systemdCeiling() <= b.Total {
		t.Fatal("the unit would be killed at the worker's own deadline, before its restoration")
	}
	if bud := doc["budget"].(map[string]any); int64(bud["total"].(float64)) != int64(b.Total) {
		t.Fatalf("printed budget differs from the one enforced: %v vs %v", bud["total"], int64(b.Total))
	}
}

func TestResolveHomePrefersAbsoluteEnvThenAccountAndNeverGuesses(t *testing.T) {
	for _, tc := range []struct{ env, account, want string }{
		{"/home/a", "/home/b", "/home/a"},
		{"", "/home/b", "/home/b"},
		{"relative", "/home/b", "/home/b"},
		{"", "", ""},
		{"relative", "also/relative", ""},
	} {
		if got := resolveHome(tc.env, tc.account); got != tc.want {
			t.Errorf("resolveHome(%q, %q) = %q, want %q", tc.env, tc.account, got, tc.want)
		}
	}
}

// Under a transient systemd unit there is no $HOME, and root's account home is not the operator's.
// The first live run on the GX10 died with "$HOME is not defined" before doing anything, and the
// ExecStopPost recovery hook runs the same way, so it would have died too.
func TestWorkerAndRecoveryHookStartWithoutAHomeDirectory(t *testing.T) {
	x := newFx(t)
	stubBenchSeams(t)
	baselineRunning(x)
	e := x.env()
	e.euid = 0
	e.home = "" // what realEnv now returns for root under systemd with no $HOME
	e.getenv = func(k string) string { return map[string]string{"INVOCATION_ID": "inv"}[k] }
	var out, diag bytes.Buffer
	code, _ := manage(context.Background(), workerArgs(x, pBase), &out, &diag, e)
	if code != exitOK {
		t.Fatalf("worker without $HOME: %d\n%s\n%s", code, out.String(), diag.String())
	}
	if x.readLatest().Restoration != "verified" {
		t.Fatalf("%+v", x.readLatest())
	}
	// The recovery hook, with the same empty home and an unsettled journal to act on.
	j := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}
	if err := j.StartBench(profile.BenchRecord{ID: "r2", Baseline: pBase}); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	diag.Reset()
	code, _ = manage(context.Background(), []string{"recover", "--yes", "--operator", "operator", "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles}, &out, &diag, e)
	if code != exitOK || x.journalState().Bench != nil {
		t.Fatalf("recovery hook without $HOME: %d\n%s\n%s", code, out.String(), diag.String())
	}
}

func TestACommandWithNoHomeAndNoOperatorFailsClearlyBeforeTouchingAnything(t *testing.T) {
	x := newFx(t)
	e := x.env()
	e.home = ""
	for _, args := range [][]string{{"status"}, {"switch", pBase, "--dry-run"}, {"benchmark", "launch", pBase}} {
		var out, diag bytes.Buffer
		code, ok := manage(context.Background(), args, &out, &diag, e)
		if !ok || code != exitUsage || !strings.Contains(diag.String(), "$HOME is unset") || !strings.Contains(diag.String(), "--operator") {
			t.Errorf("%v: handled=%v code=%d %q", args, ok, code, diag.String())
		}
	}
	if len(x.h.log) != 0 {
		t.Fatalf("host touched: %v", x.h.log)
	}
}

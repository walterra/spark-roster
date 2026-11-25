package profile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func jr(t *testing.T) Journal { return Journal{Path: filepath.Join(t.TempDir(), "state.json")} }

func TestJournalSuccessMakesTargetActive(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b := prof("a", 1), prof("b", 1)
	if err := j.SetActive("a"); err != nil {
		t.Fatal(err)
	}
	if err := Journaled(context.Background(), f, j, &a, b, opts(), nil); err != nil {
		t.Fatal(err)
	}
	if st, _ := j.Read(); st.Active != "b" || st.Switching != nil || st.Unresolved != "" {
		t.Fatalf("%+v", st)
	}
}

func TestJournalWrittenBeforeAnythingIsStopped(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b := prof("a", 1), prof("b", 1)
	var seen *Pending
	orig := f.stopErr
	_ = orig
	probe := f.probe
	_ = probe
	// Observe the journal at the moment the source is stopped.
	stopHook := &hookBackend{fake: f, onStop: func(id string) {
		if id == "a" && seen == nil {
			st, _ := j.Read()
			seen = st.Switching
		}
	}}
	if err := Journaled(context.Background(), stopHook, j, &a, b, opts(), nil); err != nil {
		t.Fatal(err)
	}
	if seen == nil || seen.From != "a" || seen.To != "b" {
		t.Fatalf("journal not written before stop: %+v", seen)
	}
}

type hookBackend struct {
	*fake
	onStop func(string)
}

func (h *hookBackend) Stop(ctx context.Context, p Profile) error {
	h.onStop(p.ID)
	return h.fake.Stop(ctx, p)
}

func TestJournalKeepsSourceActiveAfterVerifiedRestore(t *testing.T) {
	f, j := newFake(), jr(t)
	f.startErr["b"] = errors.New("boom")
	a, b := prof("a", 1), prof("b", 1)
	_ = j.SetActive("a")
	if err := Journaled(context.Background(), f, j, &a, b, opts(), nil); err == nil {
		t.Fatal("expected failure")
	}
	if st, _ := j.Read(); st.Active != "a" || st.Switching != nil || st.Unresolved != "" {
		t.Fatalf("%+v", st)
	}
}

func TestJournalMarksUnresolvedAndBlocksNextSwitch(t *testing.T) {
	f, j := newFake(), jr(t)
	f.startErr["b"] = errors.New("boom")
	f.stopErr["b"] = errors.New("still running")
	a, b := prof("a", 1), prof("b", 1)
	_ = j.SetActive("a")
	err := Journaled(context.Background(), f, j, &a, b, opts(), nil)
	if !errors.Is(err, ErrExitUnconfirmed) {
		t.Fatalf("err=%v", err)
	}
	st, _ := j.Read()
	if st.Unresolved == "" || st.Switching == nil {
		t.Fatalf("%+v", st)
	}
	before := len(f.log)
	err = Journaled(context.Background(), f, j, &a, prof("c", 1), opts(), nil)
	if err == nil || !strings.Contains(err.Error(), "run recover first") || len(f.log) != before {
		t.Fatalf("err=%v new ops=%v", err, f.log[before:])
	}
}

func TestJournalRefusesWhenActiveDiffersFromSource(t *testing.T) {
	f, j := newFake(), jr(t)
	_ = j.SetActive("x")
	a, b := prof("a", 1), prof("b", 1)
	if err := Journaled(context.Background(), f, j, &a, b, opts(), nil); err == nil || len(f.log) != 0 {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

func TestRecoverAfterKilledSwitchRestoresSource(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b := prof("a", 1), prof("b", 1)
	// A killed process leaves: a stopped, b half started, journal pending.
	f.running["b"] = true
	if err := j.write(State{Active: "a", Switching: &Pending{From: "a", To: "b", StartedAt: time.Now()}}); err != nil {
		t.Fatal(err)
	}
	err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b}, opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.log, []string{"stop b", "start a"}) || f.running["b"] || !f.running["a"] {
		t.Fatalf("log=%v running=%v", f.log, f.running)
	}
	if st, _ := j.Read(); st.Active != "a" || st.Switching != nil || st.Unresolved != "" {
		t.Fatalf("%+v", st)
	}
}

func TestRecoverDoesNotStartSourceWhenTargetStillRunning(t *testing.T) {
	f, j := newFake(), jr(t)
	f.stopErr["b"] = errors.New("container still running")
	a, b := prof("a", 1), prof("b", 1)
	_ = j.write(State{Active: "a", Switching: &Pending{From: "a", To: "b", StartedAt: time.Now()}})
	err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b}, opts(), nil)
	if !errors.Is(err, ErrExitUnconfirmed) || slices.Contains(f.log, "start a") {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
	if st, _ := j.Read(); st.Unresolved == "" || st.Switching == nil {
		t.Fatalf("%+v", st)
	}
}

func TestRecoverKeepsJournalWhenRestoreFails(t *testing.T) {
	f, j := newFake(), jr(t)
	f.startErr["a"] = errors.New("won't start")
	a, b := prof("a", 1), prof("b", 1)
	_ = j.write(State{Active: "a", Switching: &Pending{From: "a", To: "b", StartedAt: time.Now()}})
	err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b}, opts(), nil)
	if !errors.Is(err, ErrRestoreFailed) {
		t.Fatalf("err=%v", err)
	}
	if st, _ := j.Read(); st.Unresolved == "" || st.Switching == nil {
		t.Fatalf("%+v", st)
	}
}

func TestRecoverWithNothingPendingTouchesNothing(t *testing.T) {
	f, j := newFake(), jr(t)
	_ = j.SetActive("a")
	if err := Recover(context.Background(), f, j, nil, opts(), nil); err != nil || len(f.log) != 0 {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

func TestJournalFileIsPrivateAndStrict(t *testing.T) {
	j := jr(t)
	if err := j.SetActive("a"); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(j.Path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("%v %v", st, err)
	}
	if err := os.WriteFile(j.Path, []byte(`{"schema_version":1,"kind":"state","extra":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Read(); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestRecoverRefusesToStartSourceBelowItsMemoryFloor(t *testing.T) {
	f, j := newFake(), jr(t)
	f.available = 5 << 30
	a, b := prof("a", 20<<30), prof("b", 1)
	_ = j.write(State{Active: "a", Switching: &Pending{From: "a", To: "b", StartedAt: time.Now()}})
	err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b}, opts(), nil)
	if !errors.Is(err, ErrRestoreFailed) || slices.Contains(f.log, "start a") {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
	if st, _ := j.Read(); st.Unresolved == "" || st.Switching == nil {
		t.Fatalf("%+v", st)
	}
}

func benchRec(id, baseline string, paused ...string) BenchRecord {
	return BenchRecord{ID: id, Baseline: baseline, Paused: paused}
}

func TestStartBenchRecordsAndRefusesOverUnsettledState(t *testing.T) {
	j := jr(t)
	if err := j.StartBench(benchRec("b1", "a", "canary.timer")); err != nil {
		t.Fatal(err)
	}
	st, _ := j.Read()
	if st.Bench == nil || st.Bench.Baseline != "a" || st.Bench.Paused[0] != "canary.timer" || st.Bench.StartedAt.IsZero() || !st.Unsettled() {
		t.Fatalf("%+v", st)
	}
	if err := j.StartBench(benchRec("b2", "a")); err == nil || !strings.Contains(err.Error(), "run recover first") {
		t.Fatalf("second run hid the first record: %v", err)
	}
	if err := j.StartBench(BenchRecord{}); err == nil {
		t.Fatal("record without id accepted")
	}
	if err := j.EndBench(); err != nil {
		t.Fatal(err)
	}
	if st, _ := j.Read(); st.Bench != nil || st.Unsettled() {
		t.Fatalf("%+v", st)
	}
}

func TestJournalWritersKeepTheBenchmarkRecord(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b := prof("a", 1), prof("b", 1)
	_ = j.SetActive("a")
	_ = j.StartBench(benchRec("b1", "a", "canary.timer"))
	o := opts()
	o.AllowBenchmark = true // the benchmark worker owns the host for the run
	if err := Journaled(context.Background(), f, j, &a, b, o, nil); err != nil {
		t.Fatal(err)
	}
	if st, _ := j.Read(); st.Bench == nil || st.Bench.ID != "b1" || st.Active != "b" {
		t.Fatalf("a switch dropped the benchmark record: %+v", st)
	}
	if err := j.SetActive("b"); err != nil {
		t.Fatal(err)
	}
	if st, _ := j.Read(); st.Bench == nil {
		t.Fatalf("SetActive dropped the benchmark record: %+v", st)
	}
}

func TestRecoverBenchmarkStopsWhatRunsAndRestoresBaselineThenResumes(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b, c := prof("a", 1), prof("b", 1), prof("c", 1)
	// Killed mid-run: the baseline a is stopped, b is the active candidate, canary paused.
	f.running["b"] = true
	_ = j.write(State{Active: "b", Bench: &BenchRecord{ID: "b1", Baseline: "a", Paused: []string{"canary.timer"}, StartedAt: time.Now()}})
	var resumed []string
	o := opts()
	o.Resume = func(_ context.Context, u []string) error {
		if !f.running["a"] {
			t.Error("units resumed before the baseline was serving")
		}
		resumed = append(resumed, u...)
		return nil
	}
	if err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b, "c": c}, o, nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.log, []string{"stop b", "start a"}) || !f.running["a"] || f.running["b"] {
		t.Fatalf("log=%v running=%v", f.log, f.running)
	}
	if !slices.Equal(resumed, []string{"canary.timer"}) {
		t.Fatalf("resumed %v", resumed)
	}
	if st, _ := j.Read(); st.Bench != nil || st.Active != "a" || st.Unsettled() {
		t.Fatalf("%+v", st)
	}
}

func TestRecoverBenchmarkStopsBothSidesOfAnInterruptedSwitch(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b, c := prof("a", 1), prof("b", 1), prof("c", 1)
	// Killed between candidates: b was being stopped for c, which half started.
	f.running["b"], f.running["c"] = true, true
	_ = j.write(State{Active: "b", Switching: &Pending{From: "b", To: "c", StartedAt: time.Now()}, Bench: &BenchRecord{ID: "b1", Baseline: "a", StartedAt: time.Now()}})
	if err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b, "c": c}, opts(), nil); err != nil {
		t.Fatal(err)
	}
	if f.running["b"] || f.running["c"] || !f.running["a"] || slices.Index(f.log, "start a") < slices.Index(f.log, "stop b") || slices.Index(f.log, "start a") < slices.Index(f.log, "stop c") {
		t.Fatalf("log=%v running=%v", f.log, f.running)
	}
}

func TestRecoverBenchmarkKeepsHealthyBaselineWithoutRestart(t *testing.T) {
	f, j := newFake(), jr(t)
	a := prof("a", 1)
	f.running["a"] = true
	_ = j.write(State{Active: "a", Bench: &BenchRecord{ID: "b1", Baseline: "a", StartedAt: time.Now()}})
	if err := Recover(context.Background(), f, j, map[string]Profile{"a": a}, opts(), nil); err != nil {
		t.Fatal(err)
	}
	if len(f.log) != 0 {
		t.Fatalf("a serving baseline was restarted: %v", f.log)
	}
	if st, _ := j.Read(); st.Bench != nil {
		t.Fatalf("%+v", st)
	}
}

func TestRecoverBenchmarkWithoutBaselineStopsEverythingAndStartsNothing(t *testing.T) {
	f, j := newFake(), jr(t)
	b := prof("b", 1)
	f.running["b"] = true
	_ = j.write(State{Active: "b", Bench: &BenchRecord{ID: "b1", StartedAt: time.Now()}})
	if err := Recover(context.Background(), f, j, map[string]Profile{"b": b}, opts(), nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.log, []string{"stop b"}) {
		t.Fatalf("log %v", f.log)
	}
	if st, _ := j.Read(); st.Active != "" || st.Bench != nil {
		t.Fatalf("%+v", st)
	}
}

func TestRecoverBenchmarkLeavesRecordWhenResumeFailsOrIsMissing(t *testing.T) {
	for name, resume := range map[string]func(context.Context, []string) error{
		"resume fails":    func(context.Context, []string) error { return errors.New("systemctl refused") },
		"no resume given": nil,
	} {
		t.Run(name, func(t *testing.T) {
			f, j := newFake(), jr(t)
			a := prof("a", 1)
			f.running["a"] = true
			_ = j.write(State{Active: "a", Bench: &BenchRecord{ID: "b1", Baseline: "a", Paused: []string{"canary.timer"}, StartedAt: time.Now()}})
			o := opts()
			o.Resume = resume
			if err := Recover(context.Background(), f, j, map[string]Profile{"a": a}, o, nil); err == nil {
				t.Fatal("recovery reported success without resuming the paused units")
			}
			if st, _ := j.Read(); st.Bench == nil || !st.Unsettled() {
				t.Fatalf("record cleared although the canary is still paused: %+v", st)
			}
		})
	}
}

func TestRecoverBenchmarkNeverStartsBaselineWhileACandidateStillRuns(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b := prof("a", 1), prof("b", 1)
	f.running["b"] = true
	f.stopErr["b"] = errors.New("container still running")
	_ = j.write(State{Active: "b", Bench: &BenchRecord{ID: "b1", Baseline: "a", StartedAt: time.Now()}})
	err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b}, opts(), nil)
	if !errors.Is(err, ErrExitUnconfirmed) || slices.Contains(f.log, "start a") {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
	if st, _ := j.Read(); st.Bench == nil || st.Unresolved == "" {
		t.Fatalf("%+v", st)
	}
}

func TestRecoverKeepsAServingSourceWithoutRestartingItOrCheckingItsMemoryFloor(t *testing.T) {
	// A switch killed before the source was stopped leaves the source serving. Restarting it
	// would need the memory it already holds, so recovery must see that it serves and stop there.
	f, j := newFake(), jr(t)
	f.available = 1 << 30 // far below a's 20 GiB start floor
	a, b := prof("a", 20<<30), prof("b", 1)
	f.running["a"], f.running["b"] = true, true
	_ = j.write(State{Active: "a", Switching: &Pending{From: "a", To: "b", StartedAt: time.Now()}})
	if err := Recover(context.Background(), f, j, map[string]Profile{"a": a, "b": b}, opts(), nil); err != nil {
		t.Fatalf("a serving source was treated as lost: %v", err)
	}
	if !slices.Equal(f.log, []string{"stop b"}) || !f.running["a"] {
		t.Fatalf("log=%v running=%v", f.log, f.running)
	}
}

func TestParseValidatesPauseUnits(t *testing.T) {
	ok := strings.Replace(goodAdopt, `"serving":`, `"pause_during_benchmark":["canary.timer","x.service"],"serving":`, 1)
	if _, err := Parse([]byte(ok)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`"a b.timer"`, `"x.socket"`, `"--now"`} {
		doc := strings.Replace(goodAdopt, `"serving":`, `"pause_during_benchmark":[`+bad+`],"serving":`, 1)
		if _, err := Parse([]byte(doc)); err == nil || !strings.Contains(err.Error(), "pause_during_benchmark") {
			t.Errorf("%s accepted: %v", bad, err)
		}
	}
}

func TestJournaledRefusesToSwitchDuringABenchmarkUnlessTheWorkerAllowsIt(t *testing.T) {
	f, j := newFake(), jr(t)
	a, b := prof("a", 1), prof("b", 1)
	_ = j.SetActive("a")
	_ = j.StartBench(benchRec("b1", "a"))
	err := Journaled(context.Background(), f, j, &a, b, opts(), nil)
	if err == nil || !strings.Contains(err.Error(), "benchmark is running or was interrupted") || len(f.log) != 0 {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

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

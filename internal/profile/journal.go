package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// State is what SparkRoster knows about which profile should be running. It is
// written before any service is stopped, so a killed switch leaves a record
// that Recover can act on.
type State struct {
	SchemaVersion int          `json:"schema_version"`
	Kind          string       `json:"kind"`
	Active        string       `json:"active,omitempty"` // profile expected to be serving
	Switching     *Pending     `json:"switching,omitempty"`
	Unresolved    string       `json:"unresolved,omitempty"` // set when host state is unknown
	Bench         *BenchRecord `json:"benchmark,omitempty"`
	UpdatedAt     time.Time    `json:"updated_at"`
}

// Pending records a switch that has begun and not been settled.
type Pending struct {
	From      string    `json:"from,omitempty"`
	To        string    `json:"to"`
	StartedAt time.Time `json:"started_at"`
}

// BenchRecord marks an unfinished benchmark run. Baseline is the profile that
// was serving when the run began; recovery brings it back whatever step the run
// was in. Paused lists units the run stopped and recovery must start again.
type BenchRecord struct {
	ID        string    `json:"id"`
	Baseline  string    `json:"baseline,omitempty"`
	Paused    []string  `json:"paused_units,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// Unsettled reports whether anything is left for Recover to act on.
func (s State) Unsettled() bool { return s.Switching != nil || s.Bench != nil || s.Unresolved != "" }

// Journal persists State atomically in one file.
type Journal struct {
	Path string
	Now  func() time.Time
	// Chown, when set, is applied to the state directory and file after every
	// write. A benchmark that runs as root uses it to keep the operator's files
	// readable by the operator.
	Chown func(path string) error
}

func (j Journal) now() time.Time {
	if j.Now != nil {
		return j.Now().UTC()
	}
	return time.Now().UTC()
}

// Read returns the saved state, or an empty state when nothing was saved.
func (j Journal) Read() (State, error) {
	f, err := os.Open(j.Path)
	if errors.Is(err, os.ErrNotExist) {
		return State{SchemaVersion: SchemaVersion, Kind: "state"}, nil
	}
	if err != nil {
		return State{}, err
	}
	defer f.Close()
	var s State
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return State{}, fmt.Errorf("read journal %s: %w", j.Path, err)
	}
	if s.SchemaVersion != SchemaVersion || s.Kind != "state" {
		return State{}, fmt.Errorf("journal %s has an unexpected schema", j.Path)
	}
	return s, nil
}

// update reads the current state, applies fn and writes the result, so one
// writer never drops a field another one set (the benchmark record above all).
func (j Journal) update(fn func(*State)) error {
	st, err := j.Read()
	if err != nil {
		return err
	}
	fn(&st)
	return j.write(st)
}

func (j Journal) write(s State) error {
	s.SchemaVersion, s.Kind, s.UpdatedAt = SchemaVersion, "state", j.now()
	dir := filepath.Dir(j.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".state-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err = json.NewEncoder(f).Encode(s); err != nil {
		return err
	}
	if err = f.Chmod(0o600); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), j.Path); err != nil {
		return err
	}
	if j.Chown != nil {
		if err = errors.Join(j.Chown(dir), j.Chown(j.Path)); err != nil {
			return err
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// SetActive records the profile now serving, for example after adoption. It
// keeps a benchmark record, which only EndBench or Recover may remove.
func (j Journal) SetActive(id string) error {
	return j.update(func(s *State) { s.Active, s.Switching, s.Unresolved = id, nil, "" })
}

// StartBench records that a benchmark run began. It refuses to start over an
// unsettled state, so a second run cannot hide the first one's record.
func (j Journal) StartBench(rec BenchRecord) error {
	st, err := j.Read()
	if err != nil {
		return err
	}
	if st.Unsettled() {
		return fmt.Errorf("an earlier switch or benchmark is unsettled (%s); run recover first", describe(st))
	}
	if rec.ID == "" {
		return errors.New("benchmark record needs an id")
	}
	rec.StartedAt = j.now()
	st.Bench = &rec
	return j.write(st)
}

// EndBench removes the benchmark record after the baseline is verified again.
func (j Journal) EndBench() error { return j.update(func(s *State) { s.Bench = nil }) }

// Journaled runs Switch with the journal kept around it. It refuses to start
// while an earlier switch is unsettled. On success the target becomes active;
// after a failed switch with a verified restore the source stays active; when
// host state is unknown the journal is marked unresolved and nothing else may
// switch until Recover succeeds.
func Journaled(ctx context.Context, b Backend, j Journal, from *Profile, to Profile, o Options, emit func(Event)) error {
	st, err := j.Read()
	if err != nil {
		return err
	}
	if st.Switching != nil || st.Unresolved != "" {
		return fmt.Errorf("an earlier switch is unsettled (%s); run recover first", describe(st))
	}
	if st.Bench != nil && !o.AllowBenchmark {
		return fmt.Errorf("a benchmark is running or was interrupted (%s); wait for it to finish, or run recover if it died", describe(st))
	}
	if from != nil && st.Active != "" && st.Active != from.ID {
		return fmt.Errorf("journal says %s is active, not %s", st.Active, from.ID)
	}
	fromID := ""
	if from != nil {
		fromID = from.ID
	}
	pending := &Pending{From: fromID, To: to.ID, StartedAt: j.now()}
	if err = j.update(func(s *State) { s.Switching = pending }); err != nil {
		return fmt.Errorf("write journal before switching: %w", err)
	}
	serr := Switch(ctx, b, from, to, o, emit)
	switch {
	case serr == nil:
		return errors.Join(serr, j.update(func(s *State) { s.Active, s.Switching, s.Unresolved = to.ID, nil, "" }))
	case errors.Is(serr, ErrRestoreFailed) || errors.Is(serr, ErrExitUnconfirmed):
		return errors.Join(serr, j.update(func(s *State) { s.Switching, s.Unresolved = pending, serr.Error() }))
	default:
		// Switch restored the source, or never touched the host.
		return errors.Join(serr, j.update(func(s *State) { s.Active, s.Switching, s.Unresolved = fromID, nil, "" }))
	}
}

// Describe says in words what is unsettled.
func (s State) Describe() string { return describe(s) }

func describe(s State) string {
	switch {
	case s.Unresolved != "":
		return s.Unresolved
	case s.Switching != nil:
		return fmt.Sprintf("%q to %q, started %s", s.Switching.From, s.Switching.To, s.Switching.StartedAt.Format(time.RFC3339))
	case s.Bench != nil:
		return fmt.Sprintf("benchmark %s, started %s", s.Bench.ID, s.Bench.StartedAt.Format(time.RFC3339))
	}
	return "unknown"
}

// Recover settles an interrupted or unresolved switch or benchmark. It stops
// every profile the interrupted work may have left running, confirms each has
// exited, then brings the wanted profile back and verifies it generates. The
// wanted profile is the benchmark baseline, or for a plain switch its source.
// The profiles map must contain every id the journal names. A journal with
// nothing pending is left alone.
func Recover(ctx context.Context, b Backend, j Journal, profiles map[string]Profile, o Options, emit func(Event)) error {
	if emit == nil {
		emit = func(Event) {}
	}
	st, err := j.Read()
	if err != nil {
		return err
	}
	if st.Switching == nil && st.Bench == nil {
		return nil
	}
	want := ""
	if st.Bench != nil {
		want = st.Bench.Baseline
	} else {
		want = st.Switching.From
	}
	var stopIDs []string
	addStop := func(id string) {
		if id != "" && id != want && !slices.Contains(stopIDs, id) {
			stopIDs = append(stopIDs, id)
		}
	}
	if st.Switching != nil {
		addStop(st.Switching.To)
	}
	if st.Bench != nil {
		if st.Switching != nil {
			addStop(st.Switching.From)
		}
		addStop(st.Active)
	}
	unresolved := func(msg string) error {
		return j.update(func(s *State) { s.Unresolved = msg })
	}
	emit(Event{Phase: "recovering"})
	for _, id := range stopIDs {
		p, ok := profiles[id]
		if !ok {
			return fmt.Errorf("journal names unknown profile %q", id)
		}
		if err = b.Stop(ctx, p); err != nil {
			return errors.Join(fmt.Errorf("%s %w, nothing restored: %w", id, ErrExitUnconfirmed, err), unresolved("exit of "+id+" not confirmed during recovery"))
		}
	}
	resume := func() error {
		if st.Bench == nil || len(st.Bench.Paused) == 0 {
			return nil
		}
		if o.Resume == nil {
			return errors.New("the journal lists paused units but no way to resume them was given")
		}
		if e := o.Resume(ctx, st.Bench.Paused); e != nil {
			return errors.Join(fmt.Errorf("resume paused units %v: %w", st.Bench.Paused, e), unresolved("paused units were not resumed"))
		}
		return nil
	}
	settle := func() error {
		if e := resume(); e != nil {
			return e
		}
		return j.update(func(s *State) { s.Active, s.Switching, s.Unresolved, s.Bench = want, nil, "", nil })
	}
	if want == "" {
		emit(Event{Phase: "recovered"})
		return settle()
	}
	wantP, ok := profiles[want]
	if !ok {
		return fmt.Errorf("journal names unknown profile %q", want)
	}
	// Whatever else ran is stopped. If the wanted profile is serving already,
	// leave it alone: restarting it would need memory it is itself holding.
	probe, cancel := context.WithTimeout(ctx, o.ProbeTimeout)
	perr := b.Probe(probe, wantP)
	cancel()
	if perr == nil {
		emit(Event{Phase: "recovered", Profile: want})
		return settle()
	}
	if err = checkMemory(ctx, b, emit, uint64(wantP.Memory.StartMinAvailableBytes), "restore_memory"); err == nil {
		err = b.Start(ctx, wantP)
	}
	if err == nil {
		err = waitServing(ctx, b, emit, o, wantP, false)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("%w: %w", ErrRestoreFailed, err), unresolved("restore of "+want+" failed during recovery"))
	}
	emit(Event{Phase: "recovered", Profile: want})
	return settle()
}

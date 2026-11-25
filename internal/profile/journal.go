package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// State is what SparkRoster knows about which profile should be running. It is
// written before any service is stopped, so a killed switch leaves a record
// that Recover can act on.
type State struct {
	SchemaVersion int       `json:"schema_version"`
	Kind          string    `json:"kind"`
	Active        string    `json:"active,omitempty"` // profile expected to be serving
	Switching     *Pending  `json:"switching,omitempty"`
	Unresolved    string    `json:"unresolved,omitempty"` // set when host state is unknown
	UpdatedAt     time.Time `json:"updated_at"`
}

// Pending records a switch that has begun and not been settled.
type Pending struct {
	From      string    `json:"from,omitempty"`
	To        string    `json:"to"`
	StartedAt time.Time `json:"started_at"`
}

// Journal persists State atomically in one file.
type Journal struct {
	Path string
	Now  func() time.Time
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
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// SetActive records the profile now serving, for example after adoption.
func (j Journal) SetActive(id string) error { return j.write(State{Active: id}) }

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
	if from != nil && st.Active != "" && st.Active != from.ID {
		return fmt.Errorf("journal says %s is active, not %s", st.Active, from.ID)
	}
	fromID := ""
	if from != nil {
		fromID = from.ID
	}
	if err = j.write(State{Active: st.Active, Switching: &Pending{From: fromID, To: to.ID, StartedAt: j.now()}}); err != nil {
		return fmt.Errorf("write journal before switching: %w", err)
	}
	serr := Switch(ctx, b, from, to, o, emit)
	switch {
	case serr == nil:
		return errors.Join(serr, j.write(State{Active: to.ID}))
	case errors.Is(serr, ErrRestoreFailed) || errors.Is(serr, ErrExitUnconfirmed):
		return errors.Join(serr, j.write(State{Active: st.Active, Switching: &Pending{From: fromID, To: to.ID, StartedAt: j.now()}, Unresolved: serr.Error()}))
	default:
		// Switch restored the source, or never touched the host.
		return errors.Join(serr, j.write(State{Active: fromID}))
	}
}

func describe(s State) string {
	switch {
	case s.Unresolved != "":
		return s.Unresolved
	case s.Switching != nil:
		return fmt.Sprintf("%q to %q, started %s", s.Switching.From, s.Switching.To, s.Switching.StartedAt.Format(time.RFC3339))
	}
	return "unknown"
}

// Recover settles an interrupted or unresolved switch: it stops the target,
// confirms exit, restores the source and verifies it generates. The profiles
// map must contain both ids. A journal with nothing pending is left alone.
func Recover(ctx context.Context, b Backend, j Journal, profiles map[string]Profile, o Options, emit func(Event)) error {
	if emit == nil {
		emit = func(Event) {}
	}
	st, err := j.Read()
	if err != nil {
		return err
	}
	if st.Switching == nil {
		return nil
	}
	to, ok := profiles[st.Switching.To]
	if !ok {
		return fmt.Errorf("journal names unknown target profile %q", st.Switching.To)
	}
	var from *Profile
	if id := st.Switching.From; id != "" {
		p, ok := profiles[id]
		if !ok {
			return fmt.Errorf("journal names unknown source profile %q", id)
		}
		from = &p
	}
	emit(Event{Phase: "recovering", Profile: to.ID})
	if err = b.Stop(ctx, to); err != nil {
		return errors.Join(fmt.Errorf("target %w, nothing restored: %w", ErrExitUnconfirmed, err),
			j.write(State{Active: st.Active, Switching: st.Switching, Unresolved: "target exit not confirmed during recovery"}))
	}
	if from == nil {
		emit(Event{Phase: "recovered"})
		return j.write(State{})
	}
	if err = checkMemory(ctx, b, emit, uint64(from.Memory.StartMinAvailableBytes), "restore_memory"); err == nil {
		err = b.Start(ctx, *from)
	}
	if err == nil {
		err = waitServing(ctx, b, emit, o, *from, false)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("%w: %w", ErrRestoreFailed, err),
			j.write(State{Active: st.Active, Switching: st.Switching, Unresolved: "restore failed during recovery"}))
	}
	emit(Event{Phase: "recovered", Profile: from.ID})
	return j.write(State{Active: from.ID})
}

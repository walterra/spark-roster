package profile

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Backend performs the host operations a switch needs. Stop must confirm that
// the profile's processes and containers have exited. Start only submits the
// start; Probe verifies an actual answer from the service.
type Backend interface {
	Staged(context.Context, Profile) error
	Stop(context.Context, Profile) error
	Start(context.Context, Profile) error
	Probe(context.Context, Profile) error
	Available(context.Context) (uint64, error)
}

// Options bound a switch. ReserveBytes is the host headroom that must remain
// while the target starts and after it is ready.
type Options struct {
	ReserveBytes    uint64
	Poll            time.Duration
	ProbeTimeout    time.Duration
	RecoveryTimeout time.Duration
}

// ErrRestoreFailed and ErrExitUnconfirmed mark the two failures after which
// the host state is unknown. A caller keeping a journal must not clear it.
var (
	ErrRestoreFailed   = errors.New("restore failed")
	ErrExitUnconfirmed = errors.New("exit not confirmed")
)

// Event is one step of a switch, for progress output and the run journal.
type Event struct {
	Phase     string  `json:"phase"`
	Profile   string  `json:"profile,omitempty"`
	Available *uint64 `json:"available_bytes,omitempty"`
	Error     string  `json:"error,omitempty"`
}

// Switch stops from (if any), starts to and waits until it generates. If the
// target fails for any reason, including cancellation, it confirms the target
// has exited and restores from. The returned error joins the failure and any
// restore failure; a nil error means to is serving.
func Switch(ctx context.Context, b Backend, from *Profile, to Profile, o Options, emit func(Event)) (err error) {
	if emit == nil {
		emit = func(Event) {}
	}
	if o.Poll <= 0 || o.ProbeTimeout <= 0 || o.RecoveryTimeout <= 0 {
		return errors.New("invalid switch options")
	}
	if from != nil && from.ID == to.ID {
		return fmt.Errorf("profile %s is already the active profile", to.ID)
	}
	if err = to.Validate(); err != nil {
		return err
	}
	emit(Event{Phase: "preflight", Profile: to.ID})
	if err = b.Staged(ctx, to); err != nil {
		return fmt.Errorf("preflight: %s is not staged: %w", to.ID, err)
	}
	targetTouched := false
	defer func() {
		if err == nil {
			emit(Event{Phase: "ready", Profile: to.ID})
			return
		}
		if from == nil {
			if targetTouched {
				rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.RecoveryTimeout)
				defer cancel()
				if e := b.Stop(rc, to); e != nil {
					err = errors.Join(err, fmt.Errorf("target %w: %w", ErrExitUnconfirmed, e))
				}
			}
			return
		}
		rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.RecoveryTimeout)
		defer cancel()
		emit(Event{Phase: "restoring", Profile: from.ID})
		restore := func() error {
			if targetTouched {
				if e := b.Stop(rc, to); e != nil {
					return fmt.Errorf("target %w, %s not started: %w", ErrExitUnconfirmed, from.ID, e)
				}
			}
			if e := checkMemory(rc, b, emit, uint64(from.Memory.StartMinAvailableBytes), "restore_memory"); e != nil {
				return e
			}
			if e := b.Start(rc, *from); e != nil {
				return fmt.Errorf("start %s: %w", from.ID, e)
			}
			return waitServing(rc, b, emit, o, *from, false)
		}
		if e := restore(); e != nil {
			emit(Event{Phase: "restore_failed", Profile: from.ID, Error: e.Error()})
			err = errors.Join(err, fmt.Errorf("%w: %w", ErrRestoreFailed, e))
		} else {
			emit(Event{Phase: "restored", Profile: from.ID})
		}
	}()
	if from != nil {
		emit(Event{Phase: "stopping", Profile: from.ID})
		if err = b.Stop(ctx, *from); err != nil {
			return fmt.Errorf("stop %s: %w", from.ID, err)
		}
	}
	if err = checkMemory(ctx, b, emit, uint64(to.Memory.StartMinAvailableBytes), "post_stop_memory"); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	targetTouched = true // Start can partially succeed even when it reports failure.
	emit(Event{Phase: "starting", Profile: to.ID})
	if err = b.Start(ctx, to); err != nil {
		return fmt.Errorf("start %s: %w", to.ID, err)
	}
	return waitServing(ctx, b, emit, o, to, true)
}

// Step is one planned action of a switch, for --dry-run output.
type Step struct {
	Action  string   `json:"action"`
	Profile string   `json:"profile"`
	Units   []string `json:"units,omitempty"`
}

// PlanSwitch lists the steps Switch would take without touching the host.
func PlanSwitch(from *Profile, to Profile) []Step {
	var steps []Step
	steps = append(steps, Step{Action: "check staged artifacts", Profile: to.ID})
	if from != nil {
		steps = append(steps, Step{Action: "stop and confirm exit", Profile: from.ID, Units: from.Units()})
	}
	steps = append(steps,
		Step{Action: "check host memory against the start floor", Profile: to.ID},
		Step{Action: "start", Profile: to.ID, Units: to.Units()},
		Step{Action: "wait for a real generation", Profile: to.ID},
	)
	if from != nil {
		steps = append(steps, Step{Action: "on failure: confirm exit, restore", Profile: from.ID, Units: from.Units()})
	}
	return steps
}

// checkMemory reads host memory, reports it and fails below the floor.
func checkMemory(ctx context.Context, b Backend, emit func(Event), floor uint64, phase string) error {
	a, e := b.Available(ctx)
	if e != nil {
		return fmt.Errorf("%s: read memory: %w", phase, e)
	}
	emit(Event{Phase: phase, Available: &a})
	if a < floor {
		return fmt.Errorf("%s: %d bytes available, below the %d-byte floor", phase, a, floor)
	}
	return nil
}

// waitServing polls until the profile answers a real generation or its
// ready_timeout passes. With guard set it also holds the memory reserve.
func waitServing(ctx context.Context, b Backend, emit func(Event), o Options, p Profile, guard bool) error {
	deadline, cancel := context.WithTimeout(ctx, p.Serving.ReadyTimeout.Duration)
	defer cancel()
	tick := time.NewTicker(o.Poll)
	defer tick.Stop()
	for {
		if guard {
			if e := checkMemory(deadline, b, emit, o.ReserveBytes, "startup_memory"); e != nil {
				return e
			}
		}
		probe, stop := context.WithTimeout(deadline, o.ProbeTimeout)
		e := b.Probe(probe, p)
		stop()
		if e == nil {
			if guard {
				return checkMemory(deadline, b, emit, o.ReserveBytes, "ready_memory")
			}
			return nil
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("%s did not generate before its ready_timeout (last probe: %v)", p.ID, e)
		case <-tick.C:
		}
	}
}

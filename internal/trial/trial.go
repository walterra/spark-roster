// Package trial sequences a supervised, single-workload replacement and restore.
package trial

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Backend operations must honor their context. Stop must confirm the workload
// has exited (including containers and children), even after a failed Start.
// Start submits the start operation; Probe verifies actual generation.
type Backend interface {
	Preflight(context.Context, string, string) error
	Stop(context.Context, string) error
	Start(context.Context, string) error
	Probe(context.Context, string) error
	Available(context.Context) (uint64, error)
}

// Settings bounds the trial and the independent recovery attempt. StartMin is
// a reviewed isolated-start threshold, not a measured peak or admission proof.
type Settings struct {
	Stock, Candidate                                                string
	StartMin, RestoreMin, Reserve                                   uint64
	StartupTimeout, TestWindow, RecoveryTimeout, Poll, ProbeTimeout time.Duration
}

type Event struct {
	Phase          string  `json:"phase"`
	Workload       string  `json:"workload,omitempty"`
	AvailableBytes *uint64 `json:"available_bytes,omitempty"`
}

// Run restores stock after operator completion, timeout, cancellation, or
// candidate failure. This in-process guarantee cannot survive an abrupt kill.
// The caller must serialize changes and provide a durable recovery mechanism
// before using this controller to disrupt a live service.
func Run(ctx context.Context, b Backend, s Settings, done <-chan struct{}, emit func(Event)) (err error) {
	if s.Stock == "" || s.Candidate == "" || s.Stock == s.Candidate || s.Reserve == 0 || s.StartMin <= s.Reserve || s.RestoreMin <= s.Reserve ||
		s.StartupTimeout <= 0 || s.TestWindow <= 0 || s.RecoveryTimeout <= 0 || s.Poll <= 0 || s.ProbeTimeout <= 0 {
		return errors.New("invalid trial settings")
	}
	if emit == nil {
		emit = func(Event) {}
	}
	event := func(phase, name string) { emit(Event{Phase: phase, Workload: name}) }
	memory := func(ctx context.Context, minimum uint64, phase string) error {
		available, e := b.Available(ctx)
		if e != nil {
			return fmt.Errorf("%s: read memory: %w", phase, e)
		}
		emit(Event{Phase: phase, AvailableBytes: &available})
		if available < minimum {
			return fmt.Errorf("%s: %d bytes available, below %d-byte floor", phase, available, minimum)
		}
		return nil
	}
	probe := func(ctx context.Context, name string) error {
		p, cancel := context.WithTimeout(ctx, s.ProbeTimeout)
		defer cancel()
		return b.Probe(p, name)
	}
	waitReady := func(ctx context.Context, name string, guard bool) error {
		ticker := time.NewTicker(s.Poll)
		defer ticker.Stop()
		for {
			if e := ctx.Err(); e != nil {
				return fmt.Errorf("%s readiness: %w", name, e)
			}
			if guard {
				if e := memory(ctx, s.Reserve, "startup_memory"); e != nil {
					return e
				}
			}
			if e := probe(ctx, name); e == nil {
				if guard {
					return memory(ctx, s.Reserve, "ready_memory")
				}
				return ctx.Err()
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("%s readiness: %w", name, ctx.Err())
			case <-ticker.C:
			}
		}
	}
	prep, cancelPrep := context.WithTimeout(ctx, s.StartupTimeout)
	defer cancelPrep()
	if err = b.Preflight(prep, s.Stock, s.Candidate); err != nil {
		return fmt.Errorf("preflight: %w", err)
	}
	if err = probe(prep, s.Stock); err != nil {
		return fmt.Errorf("preflight stock generation: %w", err)
	}
	if err = prep.Err(); err != nil {
		return err
	}
	cancelPrep()
	if err = ctx.Err(); err != nil {
		return err
	}
	touchedCandidate := false
	defer func() {
		recoverCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.RecoveryTimeout)
		defer cancel()
		event("restoring", s.Stock)
		restore := func() error {
			if touchedCandidate {
				if e := b.Stop(recoverCtx, s.Candidate); e != nil {
					return fmt.Errorf("candidate exit not confirmed; stock not started: %w", e)
				}
			}
			if e := memory(recoverCtx, s.RestoreMin, "restore_memory"); e != nil {
				return e
			}
			if e := b.Start(recoverCtx, s.Stock); e != nil {
				return fmt.Errorf("start stock: %w", e)
			}
			return waitReady(recoverCtx, s.Stock, false)
		}
		if e := restore(); e != nil {
			event("restore_failed", s.Stock)
			err = errors.Join(err, fmt.Errorf("restore failed: %w", e))
		} else {
			event("restored", s.Stock)
		}
	}()
	startup, cancelStartup := context.WithTimeout(ctx, s.StartupTimeout)
	defer cancelStartup()
	event("stopping", s.Stock)
	if err = b.Stop(startup, s.Stock); err != nil {
		return fmt.Errorf("stop stock: %w", err)
	}
	if err = memory(startup, s.StartMin, "post_stop_memory"); err != nil {
		return err
	}
	if err = startup.Err(); err != nil {
		return err
	}
	touchedCandidate = true // Start may partially succeed even when it reports failure.
	event("starting", s.Candidate)
	if err = b.Start(startup, s.Candidate); err != nil {
		return fmt.Errorf("start candidate: %w", err)
	}
	if err = waitReady(startup, s.Candidate, true); err != nil {
		return err
	}
	cancelStartup()
	window, cancelWindow := context.WithTimeout(ctx, s.TestWindow)
	defer cancelWindow()
	event("operator_handoff", s.Candidate)
	ticker := time.NewTicker(s.Poll)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			event("operator_complete", s.Candidate)
			return nil
		case <-window.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			event("test_window_expired", s.Candidate)
			return nil
		case <-ticker.C:
			check, cancel := context.WithTimeout(window, s.ProbeTimeout)
			err = memory(check, s.Reserve, "trial_memory")
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

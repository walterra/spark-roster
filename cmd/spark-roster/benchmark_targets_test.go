package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func recordedSwitches(log *[]string, stopErr error) benchSwitches {
	return benchSwitches{
		switchPi: func(_ context.Context, w int) error {
			*log = append(*log, fmt.Sprintf("switch pi-q8 window=%d", w))
			return nil
		},
		stopPi: func(context.Context) error { *log = append(*log, "stop pi-q8"); return stopErr },
		launchInkling: func(_ context.Context, w int) error {
			*log = append(*log, fmt.Sprintf("launch inkling window=%d", w))
			return nil
		},
	}
}

func runTargets(ts []benchTarget) error {
	for _, t := range ts {
		if t.switchTo != nil {
			if err := t.switchTo(context.Background()); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestBenchTargetsSequenceAndWindows(t *testing.T) {
	var log []string
	ts := benchTargets(recordedSwitches(&log, nil), "k", false)
	ids := []string{}
	for _, x := range ts {
		ids = append(ids, x.id)
	}
	if !slices.Equal(ids, []string{"stock-qwen", "pi-q8", "inkling-2-mini"}) || ts[0].switchTo != nil || ts[0].key != "k" {
		t.Fatalf("targets %v", ids)
	}
	if err := runTargets(ts); err != nil {
		t.Fatal(err)
	}
	want := []string{"switch pi-q8 window=262144", "stop pi-q8", "launch inkling window=262144"}
	if !slices.Equal(log, want) {
		t.Fatalf("log %v", log)
	}
}

func TestBenchTargetsGateSkipsStock(t *testing.T) {
	var log []string
	ts := benchTargets(recordedSwitches(&log, nil), "k", true)
	if len(ts) != 2 || ts[0].id != "pi-q8" || ts[1].id != "inkling-2-mini" {
		t.Fatalf("gate targets %+v", ts)
	}
}

func TestBenchTargetsInklingNeverLaunchesWithoutConfirmedPiStop(t *testing.T) {
	var log []string
	stopErr := errors.New("exit not confirmed")
	err := runTargets(benchTargets(recordedSwitches(&log, stopErr), "k", false))
	if !errors.Is(err, stopErr) || slices.Contains(log, "launch inkling window=262144") {
		t.Fatalf("err=%v log=%v", err, log)
	}
}

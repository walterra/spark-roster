package main

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestBenchmarkHandoff(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stopErr error
		want    []string
	}{
		{"candidate exit confirmed", nil, []string{"stop Pi Q8", "launch Inkling"}},
		{"candidate exit unknown", errors.New("exit not confirmed"), []string{"stop Pi Q8"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var steps []string
			err := handoffBenchmark(context.Background(), func(context.Context) error { steps = append(steps, "stop Pi Q8"); return tc.stopErr }, func(context.Context) error { steps = append(steps, "launch Inkling"); return nil })
			if !errors.Is(err, tc.stopErr) || !slices.Equal(steps, tc.want) {
				t.Fatalf("steps=%v err=%v", steps, err)
			}
		})
	}
}

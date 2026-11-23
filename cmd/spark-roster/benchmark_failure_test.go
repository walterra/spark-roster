package main

import (
	"errors"
	"strings"
	"testing"
)

func TestCollectPiEventsKeepsErrorMessage(t *testing.T) {
	stream := `{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"stream closed before finish"}}` + "\n"
	var res piResult
	_, err := collectPiEvents(strings.NewReader(stream), &res)
	if err == nil || !strings.Contains(err.Error(), "stream closed before finish") {
		t.Fatalf("Pi error message lost: %v", err)
	}
}

// A stream that ends without agent_end must produce a diagnostic that says so.
func TestDiagnoseFailureNamesMissingTerminalEvent(t *testing.T) {
	d := piFailureDiagnostic(nil, errors.New("exit status 1"), false, "boom", "k")
	for _, want := range []string{"ended: false", "exit status 1", "boom"} {
		if !strings.Contains(d, want) {
			t.Fatalf("diagnostic %q missing %q", d, want)
		}
	}
}

func TestDiagnoseFailureRedactsKeyAndBoundsLength(t *testing.T) {
	d := piFailureDiagnostic(nil, nil, false, "token sk-secret "+strings.Repeat("x", 5000), "sk-secret")
	if strings.Contains(d, "sk-secret") {
		t.Fatal("key leaked into diagnostic")
	}
	if len(d) > 2048 {
		t.Fatalf("diagnostic unbounded: %d bytes", len(d))
	}
}

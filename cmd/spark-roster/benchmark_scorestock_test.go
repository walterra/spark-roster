package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func i64(v int64) *int64 { return &v }

var okRelay = qualRequest{Status: 200, End: "complete", Finish: "stop", SawDone: true}

func attempt(task string, rep int, outcome string, requests int64, relay ...qualRequest) piResult {
	return piResult{Task: task, Repetition: rep, Outcome: outcome, Requests: requests, Relay: relay}
}

func TestScoreStockProblems(t *testing.T) {
	cases := []struct {
		name string
		in   []piResult
		want string // substring expected in the single problem, "" for none
	}{
		{"every request completed", []piResult{attempt("a", 1, "pass", 2, okRelay, okRelay), attempt("a", 2, "fail", 1, okRelay)}, ""},
		{"a wrong answer is the model's, not the harness's", []piResult{attempt("a", 1, "fail", 1, okRelay)}, ""},
		{"runner_error is a harness problem", []piResult{attempt("csv", 2, "runner_error", 1, okRelay)}, "csv #2"},
		{"stream ended without a terminal event", []piResult{attempt("a", 1, "pass", 1, qualRequest{Status: 200, End: "eof_without_terminal"})}, "eof_without_terminal"},
		{"upstream read error", []piResult{attempt("a", 1, "pass", 1, qualRequest{Status: 200, End: "upstream_read_error", Error: "reset"})}, "upstream_read_error"},
		{"client_closed that the harness did not cause", []piResult{attempt("a", 1, "pass", 1, qualRequest{Status: 200, End: "client_closed"})}, "client_closed"},
		{"a timeout tears the attempt down on purpose", []piResult{attempt("a", 1, "timeout", 3, okRelay, okRelay, qualRequest{Status: 200, End: "client_closed", TeardownMS: i64(120)})}, ""},
		{"an upstream rejection is not a normal scored attempt", []piResult{attempt("a", 1, "fail", 1, qualRequest{Status: 400, End: "upstream_status", Error: "context length exceeded"})}, "400"},
		{"a record that never ended", []piResult{attempt("a", 1, "pass", 1, qualRequest{Status: 200})}, "no end"},
		{"requests with no relay record", []piResult{attempt("a", 1, "pass", 2, okRelay)}, "2 requests but 1"},
		{"request-limit stop has no record for the refused request", []piResult{attempt("a", 1, "turn_limit", 21, repeat(okRelay, 20)...)}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := scoreStockProblems(c.in)
			switch {
			case c.want == "" && len(got) != 0:
				t.Fatalf("unexpected problems: %v", got)
			case c.want != "" && (len(got) != 1 || !strings.Contains(got[0], c.want)):
				t.Fatalf("want one problem containing %q, got %v", c.want, got)
			}
		})
	}
}

func repeat(r qualRequest, n int) []qualRequest {
	out := make([]qualRequest, n)
	for i := range out {
		out[i] = r
	}
	return out
}

func TestScoreStockTableOrdersByFirstAppearance(t *testing.T) {
	// zeta appears first and sorts last, so sorted order and first-appearance order differ.
	tasks := []piResult{
		attempt("zeta", 1, "pass", 1), attempt("alpha", 1, "fail", 1),
		attempt("zeta", 2, "pass", 1), attempt("alpha", 2, "timeout", 1),
		attempt("zeta", 3, "pass", 1), attempt("alpha", 3, "turn_limit", 1),
		attempt("mid", 1, "runner_error", 1),
	}
	want := "zeta PPP\nalpha FTL\nmid E\n"
	if got := scoreStockTable(tasks); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestWriteJSONAtomicLeavesNoTempFileAndHonorsModes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := writeJSONAtomic(dir, 0700, 0600, "latest.json", map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONAtomic(dir, 0700, 0600, "latest.json", map[string]int{"a": 2}); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "latest.json" {
		t.Fatalf("expected only latest.json, got %v", entries)
	}
	if st, _ := os.Stat(filepath.Join(dir, "latest.json")); st.Mode().Perm() != 0600 {
		t.Fatalf("file mode %v", st.Mode().Perm())
	}
	if st, _ := os.Stat(dir); st.Mode().Perm() != 0700 {
		t.Fatalf("dir mode %v", st.Mode().Perm())
	}
	b, _ := os.ReadFile(filepath.Join(dir, "latest.json"))
	if !strings.Contains(string(b), `"a":2`) {
		t.Fatalf("not replaced: %s", b)
	}
}

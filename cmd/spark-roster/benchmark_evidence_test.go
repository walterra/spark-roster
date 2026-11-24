package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFirstMismatchNamesCaseAndInput(t *testing.T) {
	task := piTask{ID: "t", Cases: json.RawMessage(`[["a"],["b,c"],["d"]]`), Want: json.RawMessage(`[1,2,3]`)}
	m := firstMismatch([]byte(`[1,9,8]`), task)
	if m == nil || m.Case != 1 || m.Got != "9" || m.Want != "2" || m.Args != `["b,c"]` {
		t.Fatalf("mismatch = %+v", m)
	}
	if m := firstMismatch([]byte(`[1,2]`), task); m == nil || m.Case != 2 || m.Note == "" {
		t.Fatalf("short output = %+v", m)
	}
	if m := firstMismatch([]byte(`not json`), task); m == nil || m.Case != -1 {
		t.Fatalf("garbage = %+v", m)
	}
	if m := firstMismatch([]byte(`[1,2,3]`), task); m != nil {
		t.Fatalf("equal results reported mismatch: %+v", m)
	}
}

func TestBoundedTextKeepsRunesAndSaysTruncated(t *testing.T) {
	s := strings.Repeat("é", 100)
	got := boundedText(s, 51)
	if !strings.Contains(got, "truncated, 200 bytes") {
		t.Fatalf("no truncation note: %q", got)
	}
	if strings.ContainsRune(got, '\uFFFD') {
		t.Fatalf("cut inside a rune: %q", got)
	}
	if boundedText("abc", 10) != "abc" {
		t.Fatal("short text changed")
	}
}

func TestReadSolutionBoundsAndReportsFullSize(t *testing.T) {
	dir := t.TempDir()
	if s, n := readSolution(dir); s != "" || n != 0 {
		t.Fatalf("missing file = %q %d", s, n)
	}
	big := strings.Repeat("x", 3*evidenceSolutionMax)
	if err := os.WriteFile(filepath.Join(dir, "solution.cjs"), []byte(big), 0o600); err != nil {
		t.Fatal(err)
	}
	s, n := readSolution(dir)
	if n != len(big) || len(s) > evidenceSolutionMax+100 || !strings.Contains(s, "truncated") {
		t.Fatalf("len(s)=%d n=%d", len(s), n)
	}
}

func TestScoredSettingsAreTheAgreedOnes(t *testing.T) {
	if scoredWindow != 262144 || scoredOutput != 16384 {
		t.Fatalf("window %d output %d", scoredWindow, scoredOutput)
	}
}

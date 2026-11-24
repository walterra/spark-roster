package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"time"
	"unicode/utf8"
)

// Evidence kept for an attempt that did not pass. The earlier runs kept only a
// verdict, so a task that failed in every run (csv-record) could not be explained.
// Everything here is bounded and stays on the host in the report.
const (
	evidenceSolutionMax = 8 << 10
	evidenceTextMax     = 1 << 10
	evidenceReadMax     = 64 << 10
)

type piMismatch struct {
	Case int    `json:"case_index"`
	Args string `json:"args,omitempty"`
	Got  string `json:"got,omitempty"`
	Want string `json:"want,omitempty"`
	Note string `json:"note,omitempty"`
}

type piEvidence struct {
	Solution      string      `json:"solution,omitempty"`
	SolutionBytes int         `json:"solution_bytes,omitempty"`
	GraderStderr  string      `json:"grader_stderr,omitempty"`
	Mismatch      *piMismatch `json:"first_mismatch,omitempty"`
}

// boundedText cuts at a rune boundary and says how much was dropped.
func boundedText(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return fmt.Sprintf("%s…[truncated, %d bytes in total]", s[:cut], len(s))
}

// firstMismatch compares the grader's results with the expected ones and reports the
// first case that differs, with its input. got is the grader's stdout.
func firstMismatch(got []byte, task piTask) *piMismatch {
	var g, w []any
	if err := json.Unmarshal(got, &g); err != nil {
		return &piMismatch{Case: -1, Note: "grader output is not a JSON array", Got: boundedText(string(got), evidenceTextMax)}
	}
	if err := json.Unmarshal(task.Want, &w); err != nil {
		return &piMismatch{Case: -1, Note: "expected results are not a JSON array"}
	}
	var cases []json.RawMessage
	_ = json.Unmarshal(task.Cases, &cases)
	n := min(len(g), len(w))
	enc := func(v any) string { b, _ := json.Marshal(v); return boundedText(string(b), evidenceTextMax) }
	for i := 0; i < n; i++ {
		if reflect.DeepEqual(g[i], w[i]) {
			continue
		}
		m := &piMismatch{Case: i, Got: enc(g[i]), Want: enc(w[i])}
		if i < len(cases) {
			m.Args = boundedText(string(cases[i]), evidenceTextMax)
		}
		return m
	}
	if len(g) != len(w) {
		return &piMismatch{Case: n, Note: fmt.Sprintf("grader returned %d results, expected %d", len(g), len(w))}
	}
	return nil
}

// readSolution returns the agent's solution.cjs, bounded, and its full size.
func readSolution(work string) (string, int) {
	f, err := os.Open(filepath.Join(work, "solution.cjs"))
	if err != nil {
		return "", 0
	}
	defer f.Close()
	b, _ := io.ReadAll(io.LimitReader(f, evidenceReadMax+1))
	size := len(b)
	if st, err := f.Stat(); err == nil {
		size = int(st.Size())
	}
	return boundedText(string(b[:min(len(b), evidenceReadMax)]), evidenceSolutionMax), size
}

// gradePiTaskDetail is gradePiTask plus the grader's stderr and the first mismatch.
func gradePiTaskDetail(ctx context.Context, image, work string, task piTask) (string, *piEvidence, error) {
	grade, done := context.WithTimeout(ctx, 15*time.Second)
	defer done()
	input := fmt.Sprintf(`{"immutable":%t,"cases":%s}`, task.Immutable, task.Cases)
	gradeArgs := append(benchDockerArgs(work, true), image, "/opt/bench/runner.cjs", "grade", input)
	cmd := exec.CommandContext(grade, "docker", gradeArgs...)
	var diagnostic bytes.Buffer
	cmd.Stderr = &benchDiagnostic{buf: &diagnostic}
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", nil, err
	}
	if err = cmd.Start(); err != nil {
		return "", nil, err
	}
	data, err := io.ReadAll(io.LimitReader(pipe, 64<<10))
	if len(data) == 64<<10 || err != nil {
		done()
	}
	waitErr := cmd.Wait()
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = cleanupBenchContainer(c); err != nil {
		return "", nil, err
	}
	if ctx.Err() != nil {
		return "", nil, ctx.Err()
	}
	ev := &piEvidence{GraderStderr: boundedText(diagnostic.String(), evidenceTextMax)}
	if grade.Err() != nil {
		return "grader_deadline_or_output_limit", ev, nil
	}
	var exit *exec.ExitError
	if errors.As(waitErr, &exit) && (exit.ExitCode() == 125 || exit.ExitCode() == 126 || exit.ExitCode() == 127) {
		return "", nil, fmt.Errorf("grader container failed: %w", waitErr)
	}
	if waitErr != nil {
		if task.ID == "reference" {
			return "", nil, fmt.Errorf("reference: %s (%w)", diagnostic.String(), waitErr)
		}
		return "submission_error", ev, nil
	}
	if sameJSON(data, task.Want) {
		return "pass", nil, nil
	}
	ev.Mismatch = firstMismatch(data, task)
	return "incorrect_output", ev, nil
}

func gradePiTask(ctx context.Context, image, work string, task piTask) (string, error) {
	v, _, err := gradePiTaskDetail(ctx, image, work, task)
	return v, err
}

// Settings of the context-aligned scored benchmark and the startup budgets that were
// previously literals in the profile launchers.
const (
	scoredWindow         = 262144
	scoredOutput         = 16384
	piStartupBudget      = 4 * time.Minute
	inklingStartupBudget = 9 * time.Minute
)

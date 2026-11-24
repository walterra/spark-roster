package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// score-stock runs the scored Pi suite against the already-running stock Qwen and
// nothing else: no model switch, no sudo, no downtime. It exists to check that the
// relay changes did not alter scored results. Settings are the archived ones (32,768
// context, 4,096 output, the agreed 10 minutes per attempt) so the result is
// comparable with the two archived stock runs.
//
// Deadline: the batch ceiling is the agreed per-attempt limit times the number of
// attempts, plus 15 minutes for the pre-run checks and grading. It is a worst case.
// The two archived stock runs took 21.2 and 27.7 minutes, with a longest attempt of
// 198 s and 283 s.

type scoreStockReport struct {
	Schema         int        `json:"schema_version"`
	Started        time.Time  `json:"started_at"`
	Finished       *time.Time `json:"finished_at,omitempty"`
	Run            string     `json:"run"`
	Error          string     `json:"error,omitempty"`
	Suite          string     `json:"suite"`
	PiImage        string     `json:"pi_runner_image"`
	Model          string     `json:"model"`
	TaskTimeoutSec int        `json:"task_timeout_seconds"`
	PiContext      int        `json:"pi_context"`
	PiOutputCap    int        `json:"pi_output_cap"`
	Repetitions    int        `json:"repetitions"`
	CanaryActive   bool       `json:"canary_was_active"`
	Tasks          []piResult `json:"tasks"`
	Table          string     `json:"table,omitempty"`
	Problems       []string   `json:"harness_problems"`
}

// scoreStockProblems lists attempts whose result cannot be trusted as a statement
// about the model: a harness failure, or a relay exchange that did not end normally.
// A wrong answer, a timeout, and the request-limit stop are the model's behavior.
func scoreStockProblems(tasks []piResult) []string {
	var out []string
	for _, t := range tasks {
		id := fmt.Sprintf("%s #%d", t.Task, t.Repetition)
		if t.Outcome == "runner_error" {
			out = append(out, id+": runner_error: "+t.Diagnostic)
		}
		want := int(t.Requests)
		if t.Outcome == "turn_limit" {
			want-- // the refused request never reaches the upstream, so has no record
		}
		if len(t.Relay) != want {
			out = append(out, fmt.Sprintf("%s: %d requests but %d relay records", id, t.Requests, len(t.Relay)))
		}
		for i, r := range t.Relay {
			where := fmt.Sprintf("%s request %d", id, i+1)
			switch {
			case r.End == "":
				out = append(out, where+": no end recorded")
			case r.End == "complete":
			case r.End == "upstream_status":
				out = append(out, fmt.Sprintf("%s: upstream_status HTTP %d (%s)", where, r.Status, r.Error))
			case r.End == "client_closed" && r.TeardownMS != nil:
				// The harness ended the attempt on purpose (deadline or its own cancel).
			default:
				out = append(out, fmt.Sprintf("%s: %s %s", where, r.End, r.Error))
			}
		}
	}
	return out
}

// scoreStockTable renders one line per task, in order of first appearance: P pass,
// F fail, T timeout, L request limit, E runner error.
func scoreStockTable(tasks []piResult) string {
	letter := map[string]string{"pass": "P", "fail": "F", "timeout": "T", "turn_limit": "L", "runner_error": "E"}
	var order []string
	seen := map[string]string{}
	for _, t := range tasks {
		if _, ok := seen[t.Task]; !ok {
			order = append(order, t.Task)
		}
		l := letter[t.Outcome]
		if l == "" {
			l = "?"
		}
		seen[t.Task] += l
	}
	var b strings.Builder
	for _, id := range order {
		fmt.Fprintf(&b, "%s %s\n", id, seen[id])
	}
	return b.String()
}

func runScoreStock(ctx context.Context, home string, out io.Writer) (runErr error) {
	if err := benchStockIdentity(ctx); err != nil {
		return err
	}
	image, err := benchRunnerImage(ctx)
	if err != nil {
		return err
	}
	suite, tasks, err := benchInputs()
	if err != nil {
		return err
	}
	keyBytes, err := os.ReadFile(filepath.Join(home, ".config", "qwen38", "api-key"))
	if err != nil {
		return err
	}
	key := strings.TrimSpace(string(keyBytes))
	probe, stop := context.WithTimeout(ctx, 15*time.Second)
	err = stockProbe(probe, home)
	stop()
	if err != nil {
		return fmt.Errorf("stock generation preflight: %w", err)
	}
	canary, err := qualCanaryState(ctx)
	if err != nil {
		return err
	}

	dir := filepath.Join(home, ".local", "share", "spark-roster", "score-stock")
	ceiling := time.Duration(len(tasks)*benchRepeats)*taskLimit + 15*time.Minute
	ctx, cancel := context.WithTimeout(ctx, ceiling)
	defer cancel()

	r := scoreStockReport{Schema: 1, Started: time.Now().UTC(), Run: "running", Suite: suite, PiImage: image, Model: "qwen3.8-27b",
		TaskTimeoutSec: int(taskLimit / time.Second), PiContext: scoredWindow, PiOutputCap: scoredOutput, Repetitions: benchRepeats, CanaryActive: canary, Problems: []string{}}
	save := func() error {
		r.Table = scoreStockTable(r.Tasks)
		r.Problems = append([]string{}, scoreStockProblems(r.Tasks)...)
		return writeJSONAtomic(dir, 0700, 0600, "latest.json", r)
	}
	if err = save(); err != nil {
		return err
	}
	if tr, e := openBenchTrace(filepath.Join(dir, "events"), "score-stock", r.Started, benchOTLP); e != nil {
		fmt.Fprintln(out, "telemetry unavailable:", e)
	} else {
		fmt.Fprintln(out, "benchmark trace:", tr.id)
		ctx = tr.context(ctx)
		defer func() {
			if e := tr.close(); e != nil {
				fmt.Fprintln(out, "telemetry warning:", e)
			}
		}()
	}
	benchEventAt(ctx, "run.start", map[string]any{"mode": "score-stock", "profile": "stock-qwen", "ceiling_ms": ceiling.Milliseconds(),
		"task_timeout_ms": taskLimit.Milliseconds(), "pi_context": r.PiContext, "pi_output_cap": r.PiOutputCap, "canary_was_active": canary, "pi_image": image, "suite": suite})
	defer func() {
		now := time.Now().UTC()
		r.Finished = &now
		switch {
		case runErr != nil:
			r.Run, r.Error = "failed", runErr.Error()
		default:
			r.Run = "completed"
		}
		runErr = errors.Join(runErr, save())
		benchEventAt(ctx, "run.end", map[string]any{"outcome": r.Run, "problems": len(r.Problems), "error_kind": benchErrorKind(runErr)})
	}()

	fmt.Fprintf(out, "score-stock: %d tasks x %d, ceiling %s, canary active=%t\n", len(tasks), benchRepeats, ceiling, canary)
	for rep := 1; rep <= benchRepeats; rep++ {
		for _, task := range tasks {
			var result piResult
			err := benchPhase(benchFields(ctx, map[string]any{"profile": "stock-qwen", "task": task.ID, "repetition": rep}), "attempt", func(c context.Context) error {
				var e error
				result, e = runPiTaskScored(c, image, "qwen3.8-27b", key, 30001, task, rep)
				return e
			})
			if err != nil {
				return fmt.Errorf("%s #%d: %w", task.ID, rep, err)
			}
			r.Tasks = append(r.Tasks, result)
			fmt.Fprintf(out, "%s #%d: %s (grade=%s) requests=%d tools=%d time=%s\n", task.ID, rep, result.Outcome, result.Grade, result.Requests, result.ToolCalls, time.Duration(result.WallMS)*time.Millisecond)
			if err = save(); err != nil {
				return err
			}
		}
	}
	fmt.Fprint(out, "\n"+scoreStockTable(r.Tasks))
	if p := scoreStockProblems(r.Tasks); len(p) > 0 {
		fmt.Fprintf(out, "\nharness problems: %d\n", len(p))
		for _, x := range p {
			fmt.Fprintln(out, " -", x)
		}
	} else {
		fmt.Fprintln(out, "\nharness problems: none")
	}
	return nil
}

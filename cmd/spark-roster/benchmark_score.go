package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func runSpeedPairs(ctx context.Context, port uint16, model, key, seed string) []benchSpeed {
	var results []benchSpeed
	for n, lines := range []int{16, 64, 192} {
		var prefix strings.Builder
		fmt.Fprintf(&prefix, "Benchmark fixture %s/%d. Here is a log from a queue simulation:\n", seed, n)
		for i := range lines {
			fmt.Fprintf(&prefix, "Job %04d entered queue %d with priority %d and waited %d ticks before service.\n", i, i%7, i%11, i%17)
		}
		prompt := prefix.String() + "Explain at length how to design a fair scheduler for these queues. Include examples and pseudocode."
		for _, cache := range []string{"cold-intended", "warm-repeat"} {
			s, _, err := benchCompletion(ctx, port, model, key, prompt, 256)
			s.Pair = n + 1
			s.Cache = cache
			if err != nil {
				s.Error = err.Error()
			}
			results = append(results, s)
			if ctx.Err() != nil {
				return results
			}
		}
	}
	return results
}

func checkBenchPi(ctx context.Context, home string, out io.Writer) error {
	if err := benchStockIdentity(ctx); err != nil {
		return err
	}
	image, err := benchRunnerImage(ctx)
	if err != nil {
		return err
	}
	_, tasks, err := benchInputs()
	if err != nil {
		return err
	}
	key, err := os.ReadFile(filepath.Join(home, ".config", "qwen38", "api-key"))
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(key))
	s, _, err := benchCompletion(ctx, 30001, "qwen3.8-27b", token, "List ten ordinary data structures.", 256)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "stock streaming verified: first text %d ms, wall %d ms, usage=%t\n", s.FirstDeltaMS, s.WallMS, s.CompletionTokens != nil)
	dir, err := sandboxRoot()
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err = os.Chmod(dir, 0755); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, "solution.cjs"), []byte(`module.exports = (a,x) => { let lo=0,hi=a.length; while(lo<hi){const m=(lo+hi)>>>1;if(a[m]<x)lo=m+1;else hi=m;} return a[lo]===x?lo:-1; };`), 0644); err != nil {
		return err
	}
	ref := tasks[0]
	ref.ID = "reference"
	if grade, err := gradePiTask(ctx, image, dir, ref); err != nil || grade != "pass" {
		return fmt.Errorf("reference grader check: %s: %v", grade, err)
	}
	fmt.Fprintln(out, "reference grader check passed")
	result, err := runPiTask(ctx, image, "qwen3.8-27b", token, 30001, tasks[0], 1)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "stock Pi check: %s %s (grade=%s), requests=%d, tools=%d, tokens=%d/%d, time=%s\n", result.Task, result.Outcome, result.Grade, result.Requests, result.ToolCalls, result.PromptTokens, result.CompletionTokens, time.Duration(result.WallMS)*time.Millisecond)
	if (result.Outcome != "pass" && result.Outcome != "fail") || result.ToolCalls == 0 {
		return fmt.Errorf("stock Pi check did not pass: %s", result.Outcome)
	}
	return nil
}

func median(values []float64, unit string) string {
	if len(values) == 0 {
		return "—"
	}
	sort.Float64s(values)
	n := len(values)
	return fmt.Sprintf("%.2f%s", (values[n/2]+values[(n-1)/2])/2, unit)
}

func printBenchScorecard(w io.Writer, r benchReport) error {
	if r.Schema != 2 {
		return fmt.Errorf("no full benchmark report yet")
	}
	if _, err := fmt.Fprintf(w, "Run: %s | restoration: %s | suite: %s\n\n", r.Run, r.Restoration, r.Suite); err != nil {
		return err
	}
	common := map[string]int{}
	for _, p := range r.Profiles {
		for _, t := range p.Tasks {
			if t.Outcome == "pass" {
				common[fmt.Sprintf("%s/%d", t.Task, t.Repetition)]++
			}
		}
	}
	profiles := append([]benchResult(nil), r.Profiles...)
	passes := func(p benchResult) int {
		n := 0
		for _, t := range p.Tasks {
			if t.Outcome == "pass" {
				n++
			}
		}
		return n
	}
	sort.SliceStable(profiles, func(i, j int) bool { return passes(profiles[i]) > passes(profiles[j]) })
	if _, err := fmt.Fprintln(w, "| Profile | Pass / attempted / planned | Timeout / error / turn limit | Joint-pass median | Total Pi time | Cold / warm TTFT | Cold / warm decode estimate | Cold / warm wall | Speed errors |"); err != nil {
		return err
	}
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|")
	for _, p := range profiles {
		var joined []float64
		var total int64
		var timeout, failed, limit, speedErrors int
		for _, t := range p.Tasks {
			total += t.WallMS
			switch t.Outcome {
			case "timeout":
				timeout++
			case "runner_error":
				failed++
			case "turn_limit":
				limit++
			}
			if len(profiles) == 3 && common[fmt.Sprintf("%s/%d", t.Task, t.Repetition)] == 3 {
				joined = append(joined, float64(t.WallMS)/1000)
			}
		}
		var ttft, decode, wall [2][]float64
		for _, s := range p.Speed {
			if s.Error != "" {
				speedErrors++
				continue
			}
			i := 0
			if s.Cache == "warm-repeat" {
				i = 1
			}
			ttft[i] = append(ttft[i], float64(s.FirstDeltaMS))
			wall[i] = append(wall[i], float64(s.WallMS))
			if s.DecodeEstimate != nil {
				decode[i] = append(decode[i], *s.DecodeEstimate)
			}
		}
		if _, err := fmt.Fprintf(w, "| %s | %d / %d / %d | %d / %d / %d | %s | %s | %s / %s | %s / %s | %s / %s | %d |\n", p.Profile, passes(p), len(p.Tasks), r.Repetitions*r.TaskCount, timeout, failed, limit, median(joined, " s"), (time.Duration(total) * time.Millisecond).Round(time.Second), median(ttft[0], " ms"), median(ttft[1], " ms"), median(decode[0], " tok/s"), median(decode[1], " tok/s"), median(wall[0], " ms"), median(wall[1], " ms"), speedErrors); err != nil {
			return err
		}
	}
	return nil
}

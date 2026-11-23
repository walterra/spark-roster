package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestBenchmarkStream(t *testing.T) {
	stream := `data: {"choices":[{"delta":{"reasoning_content":"think"}}]}

data: {"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}

data: {"choices":[],"usage":{"prompt_tokens":30,"completion_tokens":2}}

data: [DONE]
`
	s, out, err := readBenchStream(strings.NewReader(stream), time.Now())
	if err != nil || out != "answer" || s.FirstAnswerMS == nil || s.CompletionTokens == nil || *s.CompletionTokens != 2 || s.FinishReason != "stop" {
		t.Fatalf("unexpected result: %+v %q %v", s, out, err)
	}
	if _, _, err = readBenchStream(strings.NewReader(strings.Split(stream, "data: [DONE]")[0]), time.Now()); err == nil {
		t.Fatal("accepted truncated stream")
	}
}

func TestBenchmarkSuiteInputs(t *testing.T) {
	_, tasks, err := benchInputs()
	if err != nil || len(tasks) != 8 || benchRepeats != 3 {
		t.Fatalf("unexpected suite: %d tasks, %d repetitions, %v", len(tasks), benchRepeats, err)
	}
	seen := map[string]bool{}
	for _, task := range tasks {
		var cases [][]any
		var want []any
		if seen[task.ID] || task.ID == "" || task.Prompt == "" || task.Starter == "" || json.Unmarshal(task.Cases, &cases) != nil || json.Unmarshal(task.Want, &want) != nil || len(cases) < 3 || len(cases) != len(want) {
			t.Fatalf("invalid task fixture: %s", task.ID)
		}
		seen[task.ID] = true
	}
	if !sameJSON([]byte(`[1, null]`), []byte(`[1,null]`)) || sameJSON([]byte(`[1,"1"]`), []byte(`[1,1]`)) {
		t.Fatal("grader did not compare typed JSON values")
	}
}

func TestBenchmarkScorecardShowsFailedAndMissingRuns(t *testing.T) {
	var out bytes.Buffer
	r := benchReport{Schema: 2, TaskCount: 8, Repetitions: 3, Run: "interrupted", Restoration: "verified", Profiles: []benchResult{
		{Profile: "stock-qwen", Tasks: []piResult{{Outcome: "pass"}, {Outcome: "timeout"}, {Outcome: "runner_error"}}},
	}}
	if err := printBenchScorecard(&out, r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "restoration: verified") || !strings.Contains(out.String(), "stock-qwen | 1 / 3 / 24 | 1 / 1 / 0") {
		t.Fatal(out.String())
	}
}

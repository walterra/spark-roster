package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// selftest drives the real Pi runner image and the real relay against a fake
// inference endpoint. It loads no model and stops no service. It exists because a
// failure report is only trustworthy if the failure path has been exercised: the
// Mac cannot run it (Docker Desktop cannot connect a container to a host Unix
// socket), so it ships in the checksum-verified binary and runs on the GX10.

const (
	selftestWindow   = 65536
	selftestOutput   = 16384
	selftestDeadline = 90 * time.Second
	// A failure must surface well inside the deadline; reaching it means a hang.
	selftestFailureBound = 60 * time.Second
)

type selftestCase struct {
	name    string
	handler http.HandlerFunc
	verdict func(res piResult, relay []qualRequest, elapsed time.Duration) error
}

type selftestResult struct {
	Name       string        `json:"name"`
	Pass       bool          `json:"pass"`
	Problem    string        `json:"problem,omitempty"`
	Outcome    string        `json:"outcome"`
	Grade      string        `json:"grade,omitempty"`
	Diagnostic string        `json:"diagnostic,omitempty"`
	Relay      []qualRequest `json:"relay"`
	ElapsedMS  int64         `json:"elapsed_ms"`
}

func fakeChunk(delta map[string]any, finish any) string {
	b, _ := json.Marshal(map[string]any{
		"id": "selftest", "object": "chat.completion.chunk", "model": "selftest",
		"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
	})
	return "data: " + string(b) + "\n\n"
}

func fakeUsage() string {
	b, _ := json.Marshal(map[string]any{
		"id": "selftest", "object": "chat.completion.chunk", "model": "selftest", "choices": []any{},
		"usage": map[string]int{"prompt_tokens": 12, "completion_tokens": 2, "total_tokens": 14},
	})
	return "data: " + string(b) + "\n\n"
}

func selftestCases() []selftestCase {
	last := func(relay []qualRequest) (qualRequest, error) {
		if len(relay) == 0 {
			return qualRequest{}, errors.New("relay recorded no request")
		}
		return relay[len(relay)-1], nil
	}
	return []selftestCase{
		{
			// A well-formed stream must pass through untouched. The task is not
			// solved, so the grade is incorrect_output, not a runner failure.
			name: "complete-stream",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(200)
				io.WriteString(w, fakeChunk(map[string]any{"role": "assistant", "content": "Done."}, nil))
				io.WriteString(w, fakeChunk(map[string]any{}, "stop"))
				io.WriteString(w, fakeUsage())
				io.WriteString(w, "data: [DONE]\n\n")
			},
			verdict: func(res piResult, relay []qualRequest, _ time.Duration) error {
				l, err := last(relay)
				if err != nil {
					return err
				}
				switch {
				case res.Outcome != "fail" || res.Grade != "incorrect_output":
					return fmt.Errorf("want outcome fail/incorrect_output, got %s/%s; diagnostic %q", res.Outcome, res.Grade, res.Diagnostic)
				case l.End != "complete" || l.Finish != "stop" || !l.SawDone || l.Status != 200:
					return fmt.Errorf("relay did not record a complete stream: %+v", l)
				case l.Cap != selftestOutput:
					return fmt.Errorf("relay saw output cap %d, want %d", l.Cap, selftestOutput)
				}
				return nil
			},
		},
		{
			// The failure that cost a qualification run: HTTP 200, then the
			// stream dies. The report must say so, and quickly.
			name: "dropped-stream-after-http-200",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(200)
				io.WriteString(w, fakeChunk(map[string]any{"role": "assistant"}, nil))
				w.(http.Flusher).Flush()
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					conn.Close()
				}
			},
			verdict: func(res piResult, relay []qualRequest, elapsed time.Duration) error {
				l, err := last(relay)
				if err != nil {
					return err
				}
				switch {
				case res.Outcome == "pass" || res.Outcome == "timeout":
					return fmt.Errorf("a dropped stream must fail fast, got outcome %s after %s", res.Outcome, elapsed)
				case elapsed > selftestFailureBound:
					return fmt.Errorf("failure took %s, treating as a hang", elapsed)
				case l.Status != 200 || l.End != "upstream_read_error" || l.Error == "":
					return fmt.Errorf("relay did not diagnose the dropped stream: %+v", l)
				case !strings.Contains(res.Diagnostic, "end=upstream_read_error") || !strings.Contains(res.Diagnostic, "ended: false"):
					return fmt.Errorf("report omits the cause: %q", res.Diagnostic)
				}
				return nil
			},
		},
		{
			name: "upstream-rejects-request",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "context length exceeded", 400)
			},
			verdict: func(res piResult, relay []qualRequest, elapsed time.Duration) error {
				l, err := last(relay)
				if err != nil {
					return err
				}
				switch {
				case res.Outcome == "pass" || res.Outcome == "timeout":
					return fmt.Errorf("a rejected request must fail fast, got outcome %s after %s", res.Outcome, elapsed)
				case l.Status != 400 || l.End != "upstream_status":
					return fmt.Errorf("relay did not record the rejection: %+v", l)
				case !strings.Contains(res.Diagnostic, "context length exceeded"):
					return fmt.Errorf("report omits the upstream message: %q", res.Diagnostic)
				}
				return nil
			},
		},
	}
}

func selftestTask() piTask {
	return piTask{ID: "selftest", Prompt: "Make the exported function return 1.", Starter: "module.exports = () => 0;", Cases: json.RawMessage(`[[]]`), Want: json.RawMessage(`[1]`)}
}

func runSelftest(ctx context.Context, out io.Writer) error {
	image, err := benchRunnerImage(ctx)
	if err != nil {
		return err
	}
	var failed []string
	for _, c := range selftestCases() {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return err
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/chat/completions" {
				http.NotFound(w, r)
				return
			}
			c.handler(w, r)
		})}
		go srv.Serve(l)
		port := uint16(l.Addr().(*net.TCPAddr).Port)

		var mu sync.Mutex
		var relay []qualRequest
		observe := func(r qualRequest) { mu.Lock(); relay = append(relay, r); mu.Unlock() }

		runCtx, cancel := context.WithTimeout(ctx, selftestDeadline)
		started := time.Now()
		res, runErr := runPiTaskWithLimits(runCtx, image, "selftest", "", port, selftestTask(), 1, selftestWindow, selftestOutput, observe)
		elapsed := time.Since(started)
		cancel()
		srv.Close()

		mu.Lock()
		records := append([]qualRequest(nil), relay...)
		mu.Unlock()
		r := selftestResult{Name: c.name, Outcome: res.Outcome, Grade: res.Grade, Diagnostic: res.Diagnostic, Relay: records, ElapsedMS: elapsed.Milliseconds()}
		if runErr != nil {
			r.Problem = "infrastructure error: " + runErr.Error()
		} else if v := c.verdict(res, records, elapsed); v != nil {
			r.Problem = v.Error()
		} else {
			r.Pass = true
		}
		b, _ := json.Marshal(r)
		fmt.Fprintln(out, string(b))
		if !r.Pass {
			failed = append(failed, c.name)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("selftest failed: %s", strings.Join(failed, ", "))
	}
	fmt.Fprintln(out, "selftest passed: failure reports name the cause on the real runner and relay")
	return nil
}

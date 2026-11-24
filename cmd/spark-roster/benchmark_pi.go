package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed pi-bench/Dockerfile pi-bench/package*.json pi-bench/runner.cjs pi-bench/tasks.json
var benchAssets embed.FS

const benchUID = "1000:1000"
const benchContainer = "spark-roster-pi-bench-task"
const benchLabel = "io.spark-roster.benchmark=pi-task"
const taskLimit = 10 * time.Minute
const benchRepeats = 3

type piTask struct {
	ID, Prompt, Starter string
	Cases, Want         json.RawMessage
	Immutable           bool
	Attachment          string `json:"-"`
}
type piResult struct {
	Diagnostic string `json:"diagnostic,omitempty"`
	// Relay is the host relay's record of each chat request in this attempt: what the
	// upstream answered and why each exchange ended.
	Relay            []qualRequest `json:"relay,omitempty"`
	Task             string        `json:"task"`
	Repetition       int           `json:"repetition"`
	Outcome          string        `json:"outcome"`
	WallMS           int64         `json:"agent_wall_ms"`
	Requests         int64         `json:"inference_requests"`
	ToolCalls        int           `json:"tool_calls"`
	PromptTokens     int           `json:"reported_prompt_tokens"`
	PeakPrompt       int           `json:"peak_reported_prompt_tokens,omitempty"`
	Compactions      int           `json:"compactions,omitempty"`
	CompletionTokens int           `json:"reported_completion_tokens"`
	Grade            string        `json:"grade,omitempty"`
	// Evidence is kept for every attempt that did not pass.
	Evidence *piEvidence `json:"evidence,omitempty"`
}

func benchInputs() (string, []piTask, error) {
	h := sha256.New()
	for _, name := range []string{"Dockerfile", "package.json", "package-lock.json", "runner.cjs", "tasks.json"} {
		b, err := benchAssets.ReadFile("pi-bench/" + name)
		if err != nil {
			return "", nil, err
		}
		fmt.Fprintf(h, "%s\x00%d\x00", name, len(b))
		h.Write(b)
	}
	b, err := benchAssets.ReadFile("pi-bench/tasks.json")
	if err != nil {
		return "", nil, err
	}
	var tasks []piTask
	err = json.Unmarshal(b, &tasks)
	return fmt.Sprintf("spark-roster-pi-bench:%x", h.Sum(nil)[:16]), tasks, err
}

func stageBenchImage(ctx context.Context, out io.Writer) error {
	tag, _, err := benchInputs()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "spark-roster-pi-build-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	for _, name := range []string{"Dockerfile", "package.json", "package-lock.json", "runner.cjs"} {
		b, e := benchAssets.ReadFile("pi-bench/" + name)
		if e != nil {
			return e
		}
		if e = os.WriteFile(filepath.Join(dir, name), b, 0600); e != nil {
			return e
		}
	}
	return command(ctx, out, "", "docker", "build", "--platform=linux/arm64", "-t", tag, dir)
}

func benchRunnerImage(ctx context.Context) (string, error) {
	tag, _, err := benchInputs()
	if err != nil {
		return "", err
	}
	s, err := output(ctx, "docker", "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Id}}", tag)
	fields := strings.Fields(s)
	if err != nil || len(fields) != 2 || fields[0] != "linux/arm64" || !strings.HasPrefix(fields[1], "sha256:") {
		return "", errors.New("Pi runner image missing or wrong architecture; run benchmark stage")
	}
	return fields[1], nil
}

// No host network, credentials, Docker socket or writable runtime files are exposed.
// Only the Pi run gets a unix relay restricted to one chat endpoint. The grader
// receives test inputs separately; expected answers never enter either sandbox.
func benchDockerArgs(work string, ro bool) []string {
	mount := "type=bind,src=" + work + ",dst=/work"
	if ro {
		mount += ",readonly"
	}
	return []string{"run", "--rm", "-i", "--name", benchContainer, "--label", benchLabel, "--pull=never", "--network=none", "--user=" + benchUID, "--cap-drop=ALL", "--security-opt=no-new-privileges", "--read-only", "--memory=1g", "--memory-swap=1g", "--pids-limit=96", "--cpus=2", "--tmpfs=/tmp:rw,nosuid,nodev,size=128m", "--mount", mount, "--workdir=/work", "--entrypoint=node"}
}

func cleanupBenchContainer(ctx context.Context) error {
	ids, err := output(ctx, "docker", "ps", "-a", "--filter", "name=^/"+benchContainer+"$", "--filter", "label="+benchLabel, "--format", "{{.ID}}")
	if err != nil || ids == "" {
		return err
	}
	if _, err = output(ctx, "docker", "rm", "-f", ids); err != nil {
		// --rm may have won the race.
		left, e := output(ctx, "docker", "ps", "-a", "--filter", "name=^/"+benchContainer+"$", "--filter", "label="+benchLabel, "--format", "{{.ID}}")
		if e != nil || left != "" {
			return err
		}
	}
	return nil
}

// relayReadTimeout bounds reading a request. It is a variable only so a test can
// shorten it; production keeps 15 s.
var relayReadTimeout = 15 * time.Second

// relayDrainBody is a test knob so a test can recreate the relay as it was before the
// body drain. Production always reads the request body to its end.
var relayDrainBody = true

// benchRelay forwards Pi's chat requests to the active endpoint. qual selects the
// qualification limits. observe, when set, receives one record per request saying
// how the upstream ended; a stream that stops before a terminal event is aborted so
// the client sees an error instead of a clean short response.
func benchRelay(dir string, port uint16, key, model string, qual bool, observe func(qualRequest)) (func(), *atomic.Int64, error) {
	stop, _, requests, err := benchRelayTeardown(dir, port, key, model, qual, observe)
	return stop, requests, err
}

// benchRelayTeardown is benchRelay plus markTeardown. The harness calls it before it
// cancels or kills the client so that the resulting client_closed is not blamed on the
// client. stop marks teardown, closes the server, and waits briefly for in-flight
// requests to record how they ended.
func benchRelayTeardown(dir string, port uint16, key, model string, qual bool, observe func(qualRequest)) (stop, markTeardown func(), requests *atomic.Int64, err error) {
	path := filepath.Join(dir, "inference.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = os.Chmod(path, 0666); err != nil {
		l.Close()
		return nil, nil, nil, err
	}
	requests = new(atomic.Int64)
	var teardownAt atomic.Int64 // unix nanoseconds, 0 until teardown begins
	var inflight sync.WaitGroup
	tr := &http.Transport{DisableKeepAlives: true}
	ms := func(t time.Time) *int64 { v := time.Since(t).Milliseconds(); return &v }
	server := &http.Server{ReadHeaderTimeout: 3 * time.Second, ReadTimeout: relayReadTimeout, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inflight.Add(1)
		defer inflight.Done()
		started := time.Now()
		if r.Method != "POST" || r.URL.RequestURI() != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		limit := int64(20)
		if qual {
			limit = qualPiRequests
		}
		if requests.Add(1) > limit {
			http.Error(w, "turn budget exhausted", http.StatusTooManyRequests)
			return
		}
		defer r.Body.Close()
		var payload map[string]json.RawMessage
		limited := http.MaxBytesReader(w, r.Body, 4<<20)
		d := json.NewDecoder(limited)
		if d.Decode(&payload) != nil || payload == nil {
			http.Error(w, "invalid request", 400)
			return
		}
		// Read the body to its end before the upstream call. net/http clears the
		// read deadline and starts watching for a client disconnect only at EOF.
		// Until then the server's ReadTimeout stays armed, and on the first response
		// write net/http tries to discard the unread body, fails on the expired
		// deadline, and cancels this request's context mid-stream.
		if relayDrainBody {
			if _, err := io.Copy(io.Discard, limited); err != nil {
				http.Error(w, "invalid request", 400)
				return
			}
		}
		var cap int
		if json.Unmarshal(payload["max_tokens"], &cap) != nil {
			_ = json.Unmarshal(payload["max_completion_tokens"], &cap)
		}
		rec := qualRequest{Cap: cap, BodyReadMS: time.Since(started).Milliseconds()}
		defer func() {
			rec.ElapsedMS = time.Since(started).Milliseconds()
			if at := teardownAt.Load(); at != 0 {
				v := (at - started.UnixNano()) / int64(time.Millisecond)
				rec.TeardownMS = &v
			}
			if observe != nil {
				observe(rec)
			}
		}()
		payload["model"], _ = json.Marshal(model)
		p, _ := json.Marshal(payload)
		rec.Bytes = len(p)
		requestCtx := r.Context()
		if qual {
			var cancel context.CancelFunc
			requestCtx, cancel = context.WithTimeout(requestCtx, qualDirectBudget)
			defer cancel()
		}
		q, err := http.NewRequestWithContext(requestCtx, "POST", fmt.Sprintf("http://127.0.0.1:%d/v1/chat/completions", port), bytes.NewReader(p))
		if err != nil {
			rec.End = "invalid_upstream_request"
			http.Error(w, "invalid upstream", 400)
			return
		}
		q.Header.Set("Content-Type", "application/json")
		if key != "" {
			q.Header.Set("Authorization", "Bearer "+key)
		}
		response, err := tr.RoundTrip(q)
		if err != nil {
			rec.Status = 502
			if r.Context().Err() != nil {
				rec.End = "client_closed"
			} else {
				rec.End = "upstream_unreachable"
				rec.Error = err.Error()
			}
			http.Error(w, "inference unavailable", 502)
			return
		}
		defer response.Body.Close()
		rec.HeadersMS = ms(started)
		rec.Status = response.StatusCode
		if response.StatusCode != 200 {
			// A short bounded snippet: enough to read "context length exceeded".
			snippet, _ := io.ReadAll(io.LimitReader(response.Body, 256))
			rec.End = "upstream_status"
			rec.Error = strings.TrimSpace(string(snippet))
			http.Error(w, "inference rejected request", response.StatusCode)
			return
		}
		streaming := strings.Contains(response.Header.Get("Content-Type"), "text/event-stream")
		w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
		w.WriteHeader(200)
		buf := make([]byte, 16<<10)
		var sse qualSSE
		for {
			n, e := response.Body.Read(buf)
			if n > 0 {
				if rec.FirstByteMS == nil {
					rec.FirstByteMS = ms(started)
				}
				rec.LastByteMS = ms(started)
				rec.ResponseBytes += n
				sse.add(buf[:n])
				rec.Finish, rec.SawDone, rec.BadEvents = sse.finish, sse.done, sse.bad
				if _, werr := w.Write(buf[:n]); werr != nil {
					rec.End, rec.Error = "client_closed", werr.Error()
					return
				}
				_ = http.NewResponseController(w).Flush()
			}
			if e == nil {
				continue
			}
			switch {
			case e == io.EOF && !(streaming && !sse.terminal()):
				rec.End = "complete"
				return
			case e == io.EOF:
				rec.End = "eof_without_terminal"
			case r.Context().Err() != nil:
				// The client may really have gone, or net/http may have cancelled
				// the context itself. Either way a clean end would look like a
				// finished stream, so abort and let the record say which it was.
				rec.End = "client_closed"
			default:
				rec.End, rec.Error = "upstream_read_error", e.Error()
			}
			// Do not let a truncated stream look finished to the client.
			panic(http.ErrAbortHandler)
		}
	})}
	go func() { _ = server.Serve(l) }()
	markTeardown = func() { teardownAt.CompareAndSwap(0, time.Now().UnixNano()) }
	stop = func() {
		markTeardown()
		_ = server.Close()
		done := make(chan struct{})
		go func() { inflight.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
		tr.CloseIdleConnections()
	}
	return stop, markTeardown, requests, nil
}

func sandboxRoot() (string, error) {
	dir, err := os.MkdirTemp("/tmp", "spark-roster-pi-")
	if err != nil {
		return "", err
	}
	return dir, nil
}

func collectPiEvents(r io.Reader, res *piResult) (bool, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 8192), 2<<20)
	end := false
	for scanner.Scan() {
		var e struct {
			Type    string
			Message struct {
				Role, StopReason, ErrorMessage string
				Usage                          struct{ Input, Output, CacheRead, CacheWrite int }
			}
		}
		if json.Unmarshal(scanner.Bytes(), &e) != nil {
			continue
		}
		switch e.Type {
		case "tool_execution_start":
			res.ToolCalls++
		case "message_end":
			if e.Message.Role == "assistant" {
				prompt := e.Message.Usage.Input + e.Message.Usage.CacheRead + e.Message.Usage.CacheWrite
				res.PromptTokens += prompt
				res.PeakPrompt = max(res.PeakPrompt, prompt)
				res.CompletionTokens += e.Message.Usage.Output
				if e.Message.StopReason == "error" || e.Message.StopReason == "aborted" {
					return false, fmt.Errorf("Pi %s: %s", e.Message.StopReason, e.Message.ErrorMessage)
				}
			}
		case "auto_compaction_end":
			res.Compactions++
		case "agent_end":
			end = true
		}
	}
	return end, scanner.Err()
}

// runPiTaskScored runs one scored attempt at the context-aligned settings: the full
// 262,144-token window and a 16,384-token output cap.
func runPiTaskScored(ctx context.Context, image, model, key string, port uint16, task piTask, rep int) (piResult, error) {
	return runPiTaskWithLimits(ctx, image, model, key, port, task, rep, scoredWindow, scoredOutput, nil)
}

func runPiTask(ctx context.Context, image, model, key string, port uint16, task piTask, rep int) (piResult, error) {
	return runPiTaskWithLimits(ctx, image, model, key, port, task, rep, 32768, 4096, nil)
}

func runPiTaskWithLimits(ctx context.Context, image, model, key string, port uint16, task piTask, rep, window, maxTokens int, observe func(qualRequest)) (res piResult, infra error) {
	res = piResult{Task: task.ID, Repetition: rep, Outcome: "runner_error"}
	dir, err := sandboxRoot()
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(dir)
	work, relay := filepath.Join(dir, "work"), filepath.Join(dir, "relay")
	if err = os.Mkdir(work, 0777); err != nil {
		return res, err
	}
	if err = os.Chmod(work, 0777); err != nil {
		return res, err
	}
	if err = os.Mkdir(relay, 0755); err != nil {
		return res, err
	}
	if err = os.WriteFile(filepath.Join(work, "solution.cjs"), []byte(task.Starter+"\n"), 0666); err != nil {
		return res, err
	}
	if err = os.Chmod(filepath.Join(work, "solution.cjs"), 0666); err != nil {
		return res, err
	}
	var graded *piEvidence
	defer func() {
		if res.Outcome == "pass" {
			return
		}
		ev := graded
		if ev == nil {
			ev = &piEvidence{}
		}
		ev.Solution, ev.SolutionBytes = readSolution(work)
		res.Evidence = ev
	}()
	clean := func() error {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return cleanupBenchContainer(c)
	}
	var relayMu sync.Mutex
	var relayLog []qualRequest
	stop, markTeardown, requests, err := benchRelayTeardown(relay, port, key, model, observe != nil, func(r qualRequest) {
		relayMu.Lock()
		relayLog = append(relayLog, r)
		relayMu.Unlock()
		if observe != nil {
			observe(r)
		}
	})
	if err != nil {
		return res, err
	}
	defer stop()
	prompt := "Modify solution.cjs in the workspace. Export only the requested function via module.exports. Use Node.js standard library only. You can run your own checks. The grader loads the exported function.\n\n" + task.Prompt
	args := append(benchDockerArgs(work, false), "--mount", "type=bind,src="+relay+",dst=/relay,readonly", image, "/opt/bench/runner.cjs", "agent", model, prompt, fmt.Sprint(window), fmt.Sprint(maxTokens))
	attachment := "-"
	if task.Attachment != "" {
		if err := os.WriteFile(filepath.Join(work, "qual-context.txt"), []byte(task.Attachment), 0644); err != nil {
			return res, err
		}
		attachment = "/work/qual-context.txt"
	}
	args = append(args, attachment)
	limit := taskLimit
	if observe != nil {
		limit = qualPiBudget
		args = append(args, fmt.Sprint(qualDirectBudget.Milliseconds()))
	}
	start := time.Now()
	phase, cancel := context.WithTimeout(ctx, limit)
	// Any end of the attempt that the harness causes (deadline, its own cancel) happens
	// through phase; mark it first so the relay does not blame the client.
	go func() { <-phase.Done(); markTeardown() }()
	cmd := exec.CommandContext(phase, "docker", args...)
	var startupLog bytes.Buffer
	cmd.Stderr = &benchDiagnostic{buf: &startupLog}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return res, err
	}
	if err = cmd.Start(); err != nil {
		cancel()
		return res, err
	}
	end, parseErr := collectPiEvents(stdout, &res)
	if parseErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	timed := errors.Is(phase.Err(), context.DeadlineExceeded)
	cancel()
	stop()
	res.WallMS = time.Since(start).Milliseconds()
	res.Requests = requests.Load()
	// stop() above waited for in-flight requests to record how they ended.
	relayMu.Lock()
	res.Relay = append([]qualRequest(nil), relayLog...)
	relayMu.Unlock()
	if err = clean(); err != nil {
		return res, err
	}
	switch {
	case ctx.Err() != nil:
		return res, ctx.Err()
	case timed:
		res.Outcome = "timeout"
		return res, nil
	case res.Requests > 20:
		res.Outcome = "turn_limit"
		return res, nil
	case waitErr != nil || parseErr != nil || !end || res.Requests == 0:
		res.Diagnostic = piFailureDiagnostic(parseErr, waitErr, end, startupLog.String(), key)
		relayMu.Lock()
		if n := len(relayLog); n > 0 {
			l := relayLog[n-1]
			res.Diagnostic = boundedRedacted(fmt.Sprintf("%s; relay: end=%s status=%d finish=%q done=%t bad_events=%d bytes=%d headers_ms=%s first_byte_ms=%s last_byte_ms=%s body_read_ms=%d teardown_ms=%s elapsed_ms=%d error=%s", res.Diagnostic, l.End, l.Status, l.Finish, l.SawDone, l.BadEvents, l.ResponseBytes, fmtMS(l.HeadersMS), fmtMS(l.FirstByteMS), fmtMS(l.LastByteMS), l.BodyReadMS, fmtMS(l.TeardownMS), l.ElapsedMS, l.Error), key)
		}
		relayMu.Unlock()
		if res.Requests == 0 && waitErr != nil && startupLog.Len() > 0 {
			return res, fmt.Errorf("Pi startup: %s", strings.TrimSpace(startupLog.String()))
		}
		return res, nil
	}
	res.Grade, graded, err = gradePiTaskDetail(ctx, image, work, task)
	if err != nil {
		return res, err
	}
	res.Outcome = "fail"
	if res.Grade == "pass" {
		res.Outcome = "pass"
	}
	return res, nil
}

type benchDiagnostic struct{ buf *bytes.Buffer }

func (d *benchDiagnostic) Write(b []byte) (int, error) {
	if room := 1024 - d.buf.Len(); room > 0 {
		d.buf.Write(b[:min(room, len(b))])
	}
	return len(b), nil
}

func sameJSON(got, want []byte) bool {
	var a, b any
	return json.Unmarshal(got, &a) == nil && json.Unmarshal(want, &b) == nil && reflect.DeepEqual(a, b)
}

// piFailureDiagnostic keeps what the run knew when it failed: the event-parser
// error, the process error, whether a terminal event arrived, and bounded
// stderr. The key is redacted and the result bounded.
func piFailureDiagnostic(parseErr, waitErr error, ended bool, stderr, key string) string {
	return boundedRedacted(fmt.Sprintf("events: %v; process: %v; ended: %t; stderr: %s", parseErr, waitErr, ended, strings.TrimSpace(stderr)), key)
}

func boundedRedacted(d, key string) string {
	if key != "" {
		d = strings.ReplaceAll(d, key, "[redacted]")
	}
	return d[:min(len(d), 2048)]
}

func fmtMS(p *int64) string {
	if p == nil {
		return "none"
	}
	return fmt.Sprint(*p)
}

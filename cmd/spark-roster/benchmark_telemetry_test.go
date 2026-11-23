package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeCollector struct {
	mu      sync.Mutex
	bodies  map[string][]string
	status  int
	partial string
}

func (f *fakeCollector) handler(w http.ResponseWriter, r *http.Request) {
	b := make([]byte, 1<<20)
	n, _ := r.Body.Read(b)
	f.mu.Lock()
	f.bodies[r.URL.Path] = append(f.bodies[r.URL.Path], string(b[:n]))
	f.mu.Unlock()
	if f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	w.WriteHeader(200)
	w.Write([]byte(`{"partialSuccess":` + f.partial + `}`))
}

func newFakeCollector(status int, partial string) (*fakeCollector, *httptest.Server) {
	f := &fakeCollector{bodies: map[string][]string{}, status: status, partial: partial}
	if partial == "" {
		f.partial = "{}"
	}
	return f, httptest.NewServer(http.HandlerFunc(f.handler))
}

func localEvents(t *testing.T, dir string) []benchEvent {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if len(files) != 1 {
		t.Fatalf("expected one trace file, got %v", files)
	}
	raw, _ := os.ReadFile(files[0])
	var out []benchEvent
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e benchEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("bad trace line %q: %v", line, err)
		}
		out = append(out, e)
	}
	return out
}

func TestTelemetryWritesLocalTraceAndExportsLogsAndSpans(t *testing.T) {
	f, srv := newFakeCollector(0, "")
	defer srv.Close()
	dir := t.TempDir()
	tr, err := openBenchTrace(dir, "test", time.Now(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := benchFields(tr.context(context.Background()), map[string]any{"profile": "stock-qwen"})
	_ = benchPhase(ctx, "probe", func(context.Context) error { return nil })
	if err := tr.close(); err != nil {
		t.Fatal(err)
	}
	events := localEvents(t, dir)
	if len(events) < 2 || events[len(events)-1].Name != "probe.end" {
		t.Fatalf("local trace incomplete: %+v", events)
	}
	if events[0].Fields["profile"] != "stock-qwen" {
		t.Fatalf("inherited field lost: %+v", events[0])
	}
	if len(f.bodies["/v1/logs"]) == 0 || len(f.bodies["/v1/traces"]) == 0 {
		t.Fatalf("collector missed signals: %v", f.bodies)
	}
}

func TestTelemetryRecordsErrorKindOnFailedPhase(t *testing.T) {
	_, srv := newFakeCollector(0, "")
	defer srv.Close()
	dir := t.TempDir()
	tr, _ := openBenchTrace(dir, "test", time.Now(), srv.URL)
	ctx := tr.context(context.Background())
	_ = benchPhase(ctx, "launch", func(context.Context) error { return context.DeadlineExceeded })
	_ = tr.close()
	var end benchEvent
	for _, e := range localEvents(t, dir) {
		if e.Name == "launch.end" {
			end = e
		}
	}
	if end.Fields["error_kind"] != "deadline" {
		t.Fatalf("error kind not recorded: %+v", end.Fields)
	}
}

// The run must keep a readable local record when Elastic is unreachable.
func TestTelemetryCollectorDownKeepsLocalTrace(t *testing.T) {
	_, srv := newFakeCollector(0, "")
	url := srv.URL
	srv.Close()
	dir := t.TempDir()
	tr, _ := openBenchTrace(dir, "test", time.Now(), url)
	ctx := tr.context(context.Background())
	benchEventAt(ctx, "run.start", nil)
	err := tr.close()
	if err == nil {
		t.Fatal("export failure not reported")
	}
	if events := localEvents(t, dir); len(events) != 1 || events[0].Name != "run.start" {
		t.Fatalf("local trace lost when collector down: %+v", events)
	}
}

func TestTelemetryReportsRejectedAndNon200(t *testing.T) {
	for name, tc := range map[string]struct {
		status  int
		partial string
	}{
		"server error":      {500, ""},
		"partial rejection": {0, `{"rejectedLogRecords":3}`},
	} {
		t.Run(name, func(t *testing.T) {
			_, srv := newFakeCollector(tc.status, tc.partial)
			defer srv.Close()
			tr, _ := openBenchTrace(t.TempDir(), "test", time.Now(), srv.URL)
			benchEventAt(tr.context(context.Background()), "x", nil)
			if err := tr.close(); err == nil {
				t.Fatal("rejection not surfaced")
			}
		})
	}
}

func TestBenchErrorKindClassifies(t *testing.T) {
	for want, err := range map[string]error{"none": nil, "canceled": context.Canceled, "operation_failed": errors.New("x")} {
		if got := benchErrorKind(err); got != want {
			t.Errorf("%v: got %q want %q", err, got, want)
		}
	}
}

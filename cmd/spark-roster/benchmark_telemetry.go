package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const benchOTLP = "http://127.0.0.1:4318"

type benchTraceKey struct{}
type benchTrace struct {
	id, endpoint string
	file         *os.File
	queue        chan benchEvent
	done         chan struct{}
	mu           sync.Mutex
	localErr     error
	exportErr    error // written by the exporter and read after done closes
}
type benchEvent struct {
	At     time.Time      `json:"timestamp"`
	Trace  string         `json:"trace_id"`
	Name   string         `json:"event"`
	Fields map[string]any `json:"attributes,omitempty"`
	Start  time.Time      `json:"start,omitempty"`
	Span   string         `json:"span_id,omitempty"`
	Parent string         `json:"parent_span_id,omitempty"`
}
type benchScope struct {
	trace  *benchTrace
	fields map[string]any
	span   string
}

func openBenchTrace(dir, kind string, started time.Time, endpoint string) (*benchTrace, error) {
	hash := sha256.Sum256([]byte(kind + started.UTC().Format(time.RFC3339Nano)))
	id := hex.EncodeToString(hash[:16])
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, id+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	t := &benchTrace{id: id, endpoint: endpoint, file: f, queue: make(chan benchEvent, 128), done: make(chan struct{})}
	go t.export()
	return t, nil
}

func (t *benchTrace) context(ctx context.Context) context.Context {
	return context.WithValue(ctx, benchTraceKey{}, benchScope{trace: t})
}
func benchFields(ctx context.Context, fields map[string]any) context.Context {
	s, ok := ctx.Value(benchTraceKey{}).(benchScope)
	if !ok {
		return ctx
	}
	s.fields = mergeFields(s.fields, fields)
	return context.WithValue(ctx, benchTraceKey{}, s)
}
func mergeFields(a, b map[string]any) map[string]any {
	m := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		m[k] = v
	}
	for k, v := range b {
		m[k] = v
	}
	return m
}
func benchEventAt(ctx context.Context, name string, fields map[string]any) {
	if s, ok := ctx.Value(benchTraceKey{}).(benchScope); ok {
		s.trace.record(benchEvent{At: time.Now().UTC(), Trace: s.trace.id, Name: name, Fields: mergeFields(s.fields, fields), Span: s.span})
	}
}
func benchPhase(ctx context.Context, name string, run func(context.Context) error) error {
	s, ok := ctx.Value(benchTraceKey{}).(benchScope)
	if !ok {
		return run(ctx)
	}
	start := time.Now().UTC()
	parent := s.span
	hash := sha256.Sum256([]byte(s.trace.id + name + start.Format(time.RFC3339Nano)))
	s.span = hex.EncodeToString(hash[:8])
	ctx = context.WithValue(ctx, benchTraceKey{}, s)
	benchEventAt(ctx, name+".start", nil)
	err := run(ctx)
	end := time.Now().UTC()
	s.trace.record(benchEvent{At: end, Start: start, Name: name + ".end", Trace: s.trace.id, Span: s.span, Parent: parent, Fields: mergeFields(s.fields, map[string]any{"elapsed_ms": end.Sub(start).Milliseconds(), "error_kind": benchErrorKind(err)})})
	return err
}
func benchErrorKind(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	default:
		return "operation_failed"
	}
}
func (t *benchTrace) record(e benchEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file == nil {
		return
	}
	if err := json.NewEncoder(t.file).Encode(e); err != nil {
		t.localErr = err
	}
	select {
	case t.queue <- e:
	default:
		t.localErr = errors.Join(t.localErr, errors.New("OTLP queue full; see local trace"))
	}
}
func (t *benchTrace) close() error {
	t.mu.Lock()
	close(t.queue)
	err := errors.Join(t.localErr, t.file.Sync(), t.file.Close())
	t.file = nil
	t.mu.Unlock()
	select {
	case <-t.done:
		return errors.Join(err, t.exportErr)
	case <-time.After(3 * time.Second):
		return errors.Join(err, errors.New("OTLP flush incomplete; see local trace"))
	}
}
func (t *benchTrace) export() {
	defer close(t.done)
	c := &http.Client{Timeout: time.Second, Transport: &http.Transport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer c.CloseIdleConnections()
	for e := range t.queue {
		if t.endpoint == "" {
			continue
		}
		if err := sendBenchOTLP(c, t.endpoint, "logs", e); err != nil {
			t.exportErr = err
			continue
		}
		if !e.Start.IsZero() {
			if err := sendBenchOTLP(c, t.endpoint, "traces", e); err != nil {
				t.exportErr = err
			}
		}
	}
}
func otlpBenchAttrs(fields map[string]any) []map[string]any {
	attrs := make([]map[string]any, 0, len(fields))
	for k, v := range fields {
		var val map[string]any
		switch x := v.(type) {
		case bool:
			val = map[string]any{"boolValue": x}
		case int:
			val = map[string]any{"intValue": strconv.Itoa(x)}
		case int64:
			val = map[string]any{"intValue": strconv.FormatInt(x, 10)}
		case uint64:
			val = map[string]any{"intValue": strconv.FormatUint(x, 10)}
		default:
			val = map[string]any{"stringValue": fmt.Sprint(v)}
		}
		attrs = append(attrs, map[string]any{"key": k, "value": val})
	}
	return attrs
}
func sendBenchOTLP(c *http.Client, endpoint, signal string, e benchEvent) error {
	ns := func(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }
	attrs := otlpBenchAttrs(mergeFields(e.Fields, map[string]any{"spark.run.id": e.Trace, "spark.event": e.Name}))
	item := map[string]any{"timeUnixNano": ns(e.At), "traceId": e.Trace, "body": map[string]string{"stringValue": e.Name}, "attributes": attrs}
	if e.Span != "" {
		item["spanId"] = e.Span
	}
	resource := map[string]any{"attributes": otlpBenchAttrs(map[string]any{"service.name": "spark-roster", "data_stream.dataset": "sparkroster.benchmark"})}
	scope := map[string]any{"scope": map[string]string{"name": "spark-roster.benchmark"}}
	outer, key := "resourceLogs", "scopeLogs"
	if signal == "traces" {
		outer, key = "resourceSpans", "scopeSpans"
		delete(item, "timeUnixNano")
		delete(item, "body")
		item["name"] = e.Name
		item["startTimeUnixNano"] = ns(e.Start)
		item["endTimeUnixNano"] = ns(e.At)
		item["kind"] = 1
		if e.Parent != "" {
			item["parentSpanId"] = e.Parent
		}
		if e.Fields["error_kind"] != "none" {
			item["status"] = map[string]int{"code": 2}
		}
		scope["spans"] = []any{item}
	} else {
		scope["logRecords"] = []any{item}
	}
	b, err := json.Marshal(map[string]any{outer: []any{map[string]any{"resource": resource, key: []any{scope}}}})
	if err != nil {
		return err
	}
	r, err := c.Post(endpoint+"/v1/"+signal, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("OTLP %s HTTP %d", signal, r.StatusCode)
	}
	var reply struct {
		PartialSuccess struct{ RejectedLogRecords, RejectedSpans json.RawMessage }
	}
	if err = json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&reply); err != nil {
		return err
	}
	for _, n := range []json.RawMessage{reply.PartialSuccess.RejectedLogRecords, reply.PartialSuccess.RejectedSpans} {
		if len(n) > 0 && string(n) != "0" && string(n) != `"0"` {
			return errors.New("collector partially rejected OTLP records")
		}
	}
	return nil
}

func startBenchTelemetry(ctx context.Context, kind string, started time.Time, out io.Writer, required bool) (context.Context, func(), error) {
	return startBenchTelemetryIn(ctx, filepath.Join(benchmarkState, "events"), kind, started, out, required)
}

// startBenchTelemetryIn is startBenchTelemetry with the trace directory chosen by the caller.
func startBenchTelemetryIn(ctx context.Context, dir, kind string, started time.Time, out io.Writer, required bool) (context.Context, func(), error) {
	t, err := openBenchTrace(dir, kind, started, benchOTLP)
	if err != nil {
		if required {
			return ctx, func() {}, err
		}
		fmt.Fprintln(out, "telemetry unavailable:", err)
		return ctx, func() {}, nil
	}
	fmt.Fprintln(out, "benchmark trace:", t.id)
	return t.context(ctx), func() {
		if err := t.close(); err != nil {
			fmt.Fprintln(out, "telemetry warning:", err)
		}
	}, nil
}

// checkBenchTelemetry sends one marker event to the collector. It starts no
// workload; a failure means the run would rely on the local trace file alone.
func checkBenchTelemetry(ctx context.Context, out io.Writer) error {
	started := time.Now().UTC()
	dir, err := os.MkdirTemp("", "spark-roster-telemetry-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	t, err := openBenchTrace(dir, "telemetry-check", started, benchOTLP)
	if err != nil {
		return err
	}
	c := t.context(ctx)
	err = benchPhase(c, "telemetry.check", func(context.Context) error { return nil })
	if e := t.close(); e != nil {
		err = errors.Join(err, e)
	}
	if err != nil {
		return fmt.Errorf("telemetry export failed: %w", err)
	}
	fmt.Fprintf(out, "telemetry check sent: trace %s\n", t.id)
	return nil
}

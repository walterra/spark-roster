package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// relayHarness runs the real benchRelay between a Go client on its Unix socket
// and a fake upstream. It needs no Docker, so it runs on any host.
func relayHarness(t *testing.T, upstream http.HandlerFunc) (*http.Client, chan qualRequest) {
	c, ch, _, _ := relayWithControl(t, upstream)
	return c, ch
}

// relayWithControl also returns the relay's stop and markTeardown functions.
func relayWithControl(t *testing.T, upstream http.HandlerFunc) (*http.Client, chan qualRequest, func(), func()) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	port := uint16(up.Listener.Addr().(*net.TCPAddr).Port)
	// macOS limits Unix socket paths to ~104 bytes; t.TempDir() is too long.
	dir, err := os.MkdirTemp("/tmp", "sr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	records := make(chan qualRequest, 8)
	stop, mark, _, err := benchRelayTeardown(dir, port, "", "m", true, func(r qualRequest) { records <- r })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	sock := filepath.Join(dir, "inference.sock")
	return &http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}, records, stop, mark
}

// stallUntilAbandoned holds an upstream response open until the relay abandons it. The
// handler must read the request body first: net/http only notices a closed connection,
// and cancels the request context, once the body has been read to its end. Without
// that, this would wait forever. The bound turns any such mistake into a failure.
func stallUntilAbandoned(w http.ResponseWriter, r *http.Request, onStalled func()) {
	io.Copy(io.Discard, r.Body)
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, sseChunk)
	w.(http.Flusher).Flush()
	if onStalled != nil {
		onStalled()
	}
	select {
	case <-r.Context().Done():
	case <-time.After(10 * time.Second):
	}
}

func postChat(ctx context.Context, c *http.Client) (*http.Response, error) {
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://relay/v1/chat/completions", strings.NewReader(`{"max_tokens":16384,"messages":[]}`))
	return c.Do(req)
}

func nextRecord(t *testing.T, ch chan qualRequest) qualRequest {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("relay recorded nothing")
		return qualRequest{}
	}
}

const sseChunk = `data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}` + "\n\n"
const sseStop = `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n"

func TestRelayCompleteStreamIsRecordedAsComplete(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseChunk, sseStop, "data: [DONE]\n\n")
	})
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(res.Body); err != nil {
		t.Fatalf("complete stream failed on the client: %v", err)
	}
	r := nextRecord(t, ch)
	if r.End != "complete" || r.Finish != "stop" || !r.SawDone || r.Status != 200 || r.Cap != 16384 {
		t.Fatalf("unexpected record: %+v", r)
	}
}

// The failure that cost a full qualification run: HTTP 200, then the upstream
// dies before any terminal event. The client must see an error, not a clean
// short stream, and the record must say why.
func TestRelayDroppedStreamAfterHTTP200(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseChunk)
		w.(http.Flusher).Flush()
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	})
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(res.Body); err == nil {
		t.Fatal("client saw a clean stream although the upstream dropped it")
	}
	r := nextRecord(t, ch)
	if r.Status != 200 || r.End != "upstream_read_error" || r.Error == "" || r.Finish != "" || r.SawDone {
		t.Fatalf("dropped stream not diagnosed: %+v", r)
	}
}

func TestRelayCleanCloseWithoutTerminalEvent(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseChunk) // no finish_reason, no [DONE]
	})
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(res.Body); err == nil {
		t.Fatal("truncated stream was passed on as complete")
	}
	if r := nextRecord(t, ch); r.End != "eof_without_terminal" {
		t.Fatalf("unexpected record: %+v", r)
	}
}

func TestRelayUpstreamErrorStatus(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "context length exceeded", 400)
	})
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 400 {
		t.Fatalf("client status %d", res.StatusCode)
	}
	if r := nextRecord(t, ch); r.Status != 400 || r.End != "upstream_status" || !strings.Contains(r.Error, "context length exceeded") {
		t.Fatalf("unexpected record: %+v", r)
	}
}

func TestRelayUpstreamUnreachable(t *testing.T) {
	dead, _ := net.Listen("tcp4", "127.0.0.1:0")
	port := uint16(dead.Addr().(*net.TCPAddr).Port)
	dead.Close()
	dir, _ := os.MkdirTemp("/tmp", "sr")
	defer os.RemoveAll(dir)
	records := make(chan qualRequest, 1)
	stop, _, err := benchRelay(dir, port, "", "m", true, func(r qualRequest) { records <- r })
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(dir, "inference.sock"))
	}}}
	res, err := postChat(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 502 {
		t.Fatalf("client status %d", res.StatusCode)
	}
	if r := nextRecord(t, records); r.End != "upstream_unreachable" || r.Error == "" || r.Status != 502 {
		t.Fatalf("unexpected record: %+v", r)
	}
}

// A stalled upstream is cut by the client going away; the record must say who
// ended it rather than blaming the upstream.
func TestRelayClientDisconnectDuringStall(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) { stallUntilAbandoned(w, r, nil) })
	ctx, cancel := context.WithCancel(context.Background())
	res, err := postChat(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	res.Body.Read(buf) // first event arrived, now stall
	cancel()
	if r := nextRecord(t, ch); r.End != "client_closed" || r.Finish != "" || r.TeardownMS != nil {
		t.Fatalf("stall/disconnect misattributed: %+v", r)
	}
}

func TestRelayCountsUnparseableEventsButKeepsStreaming(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {not json\n\n", sseChunk, sseStop, "data: [DONE]\n\n")
	})
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(res.Body)
	r := nextRecord(t, ch)
	if r.BadEvents != 1 || r.Finish != "stop" || r.End != "complete" {
		t.Fatalf("malformed event mishandled: %+v", r)
	}
}

func TestRelayRecordsWhereTheTimeWent(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		time.Sleep(300 * time.Millisecond) // headers and first event arrive together, late
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseChunk)
		w.(http.Flusher).Flush()
		time.Sleep(250 * time.Millisecond)
		fmt.Fprint(w, sseStop, "data: [DONE]\n\n")
	})
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(res.Body)
	r := nextRecord(t, ch)
	if r.HeadersMS == nil || r.FirstByteMS == nil || r.LastByteMS == nil {
		t.Fatalf("missing timings: %+v", r)
	}
	if *r.HeadersMS < 250 || *r.FirstByteMS < *r.HeadersMS || *r.LastByteMS < *r.FirstByteMS+200 {
		t.Fatalf("timings do not match the upstream's behavior: headers=%d first=%d last=%d", *r.HeadersMS, *r.FirstByteMS, *r.LastByteMS)
	}
	want := len(sseChunk) + len(sseStop) + len("data: [DONE]\n\n")
	if r.ResponseBytes != want || r.TeardownMS != nil || r.End != "complete" {
		t.Fatalf("unexpected record: %+v (want %d response bytes)", r, want)
	}
}

// Whether the engine sends response headers before the first token is an open
// question about SGLang. The record must tell the two apart.
func TestRelayTellsEarlyHeadersFromAFirstByteThatComesLate(t *testing.T) {
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(450 * time.Millisecond)
		fmt.Fprint(w, sseChunk, sseStop, "data: [DONE]\n\n")
	})
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(res.Body)
	r := nextRecord(t, ch)
	if r.HeadersMS == nil || r.FirstByteMS == nil || *r.HeadersMS > 250 || *r.FirstByteMS < 400 {
		t.Fatalf("early headers not distinguished from a late first byte: %+v", r)
	}
}

// The record must separate "the harness tore this down" from "the client went away".
func TestRelayMarksHarnessInducedTeardown(t *testing.T) {
	stalled := make(chan struct{})
	c, ch, _, mark := relayWithControl(t, func(w http.ResponseWriter, r *http.Request) {
		stallUntilAbandoned(w, r, func() { close(stalled) })
	})
	ctx, cancel := context.WithCancel(context.Background())
	res, err := postChat(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Read(make([]byte, 16))
	<-stalled
	mark()   // the harness decides to cancel the attempt ...
	cancel() // ... and the client connection goes away
	r := nextRecord(t, ch)
	if r.End != "client_closed" || r.TeardownMS == nil || *r.TeardownMS < 0 || *r.TeardownMS > r.ElapsedMS {
		t.Fatalf("harness teardown not recorded with a usable time: %+v", r)
	}
}

// stop must not return before in-flight requests have recorded how they ended. The
// observer is slow on purpose: without the wait inside stop, the record would not be
// there yet when stop returns, so this fails deterministically instead of by race.
func TestRelayStopDeliversTheRecordBeforeReturning(t *testing.T) {
	stalled := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stallUntilAbandoned(w, r, func() { close(stalled) })
	}))
	t.Cleanup(up.Close)
	dir, err := os.MkdirTemp("/tmp", "sr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	records := make(chan qualRequest, 1)
	stop, _, _, err := benchRelayTeardown(dir, uint16(up.Listener.Addr().(*net.TCPAddr).Port), "", "m", true, func(r qualRequest) {
		time.Sleep(300 * time.Millisecond)
		records <- r
	})
	if err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "inference.sock")
	c := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	res, err := postChat(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Read(make([]byte, 16))
	<-stalled
	stop()
	select {
	case r := <-records:
		if r.TeardownMS == nil || r.End != "client_closed" {
			t.Fatalf("unexpected record after stop: %+v", r)
		}
	default:
		t.Fatal("stop returned before the in-flight request recorded its end")
	}
}

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
	stop, _, err := benchRelay(dir, port, "", "m", true, func(r qualRequest) { records <- r })
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
	}}, records
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
	release := make(chan struct{})
	c, ch := relayHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseChunk)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	res, err := postChat(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	res.Body.Read(buf) // first event arrived, now stall
	cancel()
	if r := nextRecord(t, ch); r.End != "client_closed" || r.Finish != "" {
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

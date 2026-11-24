package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Hypothesis under test: the host relay's ReadTimeout stays armed while a chunked
// request body is not read to EOF, so a first response byte later than the timeout
// truncates the stream. Each case runs the real relay with the timeout lowered and
// an upstream that delays its first byte.

const deadlineBody = `{"max_tokens":16384,"messages":[]}`

// lengthHidden stops net/http from learning the body length, so the client sends
// Transfer-Encoding: chunked, as Node's req.pipe does for Pi's request.
type lengthHidden struct{ io.Reader }

// slowUpstream models a long prefill followed by a long decode: nothing for `delay`,
// then one event, then the rest after a further pause. A response that arrives in one
// segment hides truncation, because nothing remains to be cut off.
func slowUpstream(delay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		time.Sleep(delay)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseChunk)
		w.(http.Flusher).Flush()
		time.Sleep(400 * time.Millisecond)
		fmt.Fprint(w, sseStop, "data: [DONE]\n\n")
	}
}

func deadlineHarness(t *testing.T, readTimeout time.Duration, up http.HandlerFunc) (sock string, records chan qualRequest) {
	t.Helper()
	old := relayReadTimeout
	relayReadTimeout = readTimeout
	t.Cleanup(func() { relayReadTimeout = old })
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)
	dir, err := os.MkdirTemp("/tmp", "sr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	records = make(chan qualRequest, 4)
	stop, _, err := benchRelay(dir, uint16(srv.Listener.Addr().(*net.TCPAddr).Port), "", "m", true, func(r qualRequest) { records <- r })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	return filepath.Join(dir, "inference.sock"), records
}

func goClient(sock string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

type outcome struct {
	readErr  error
	bytes    int
	sawFinal bool
	record   qualRequest
}

func exchange(t *testing.T, sock string, records chan qualRequest, chunked bool) outcome {
	t.Helper()
	var body io.Reader = strings.NewReader(deadlineBody)
	if chunked {
		body = lengthHidden{body}
	}
	req, _ := http.NewRequest("POST", "http://relay/v1/chat/completions", body)
	if chunked {
		req.ContentLength = -1
	}
	res, err := goClient(sock).Do(req)
	if err != nil {
		t.Fatalf("request failed before any response: %v", err)
	}
	data, rerr := io.ReadAll(res.Body)
	o := outcome{readErr: rerr, bytes: len(data), sawFinal: strings.Contains(string(data), "[DONE]")}
	select {
	case o.record = <-records:
	case <-time.After(5 * time.Second):
		t.Fatal("relay recorded nothing")
	}
	return o
}

func TestRelayReadDeadline(t *testing.T) {
	const timeout = 600 * time.Millisecond
	cases := []struct {
		name    string
		chunked bool
		delay   time.Duration
	}{
		{"chunked body, first byte after the read timeout", true, 1500 * time.Millisecond},
		{"content-length body, first byte after the read timeout", false, 1500 * time.Millisecond},
		{"chunked body, first byte before the read timeout", true, 100 * time.Millisecond},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sock, records := deadlineHarness(t, timeout, slowUpstream(c.delay))
			o := exchange(t, sock, records, c.chunked)
			t.Logf("client: err=%v bytes=%d sawDone=%t | relay: end=%q finish=%q done=%t error=%q elapsed=%dms",
				o.readErr, o.bytes, o.sawFinal, o.record.End, o.record.Finish, o.record.SawDone, o.record.Error, o.record.ElapsedMS)
			if o.readErr != nil || !o.sawFinal || o.record.End != "complete" {
				t.Errorf("slow first byte truncated the stream: client err=%v sawDone=%t, relay end=%q", o.readErr, o.sawFinal, o.record.End)
			}
		})
	}
}

// The real client is Node, not Go: confirm with the exact client the container
// relay uses. Skipped when node is absent.
func TestRelayReadDeadlineWithNodeClient(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	sock, records := deadlineHarness(t, 600*time.Millisecond, slowUpstream(1500*time.Millisecond))
	script := `const http=require('http');
const req=http.request({socketPath:process.argv[1],path:'/v1/chat/completions',method:'POST',headers:{'content-type':'application/json'}},res=>{
  let n=0,done=false;res.on('data',d=>{n+=d.length;if(String(d).includes('[DONE]'))done=true});
  res.on('end',()=>{console.log(JSON.stringify({ended:'end',bytes:n,done,complete:res.complete}));process.exit(0)});
  res.on('error',e=>{console.log(JSON.stringify({ended:'error',code:e.code,bytes:n,done}));process.exit(0)});
  res.on('close',()=>{if(!res.complete){console.log(JSON.stringify({ended:'closed_early',bytes:n,done}));process.exit(0)}});
});
req.on('error',e=>{console.log(JSON.stringify({ended:'request_error',code:e.code}));process.exit(0)});
require('stream').Readable.from([Buffer.from(` + "`" + deadlineBody + "`" + `)]).pipe(req);
setTimeout(()=>{console.log(JSON.stringify({ended:'hang'}));process.exit(0)},6000);`
	out, err := exec.Command(node, "-e", script, sock).CombinedOutput()
	if err != nil {
		t.Fatalf("node client: %v\n%s", err, out)
	}
	var rec qualRequest
	select {
	case rec = <-records:
	case <-time.After(5 * time.Second):
		t.Fatal("relay recorded nothing")
	}
	t.Logf("node client: %s | relay: end=%q finish=%q done=%t elapsed=%dms", strings.TrimSpace(string(out)), rec.End, rec.Finish, rec.SawDone, rec.ElapsedMS)
	if !strings.Contains(string(out), `"ended":"end"`) || rec.End != "complete" {
		t.Errorf("slow first byte truncated the stream for the Node client: %s relay end=%q", strings.TrimSpace(string(out)), rec.End)
	}
}

// Real request shape: Pi sends a large body with Content-Length, the Node container
// relay re-sends it with req.pipe (chunked), and the Go relay reads it. This runs the
// actual runner.cjs relay in front of the actual Go relay on one host.
func TestRelayReadDeadlineRealChain(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	runner, _ := filepath.Abs("pi-bench/runner.cjs")
	for _, size := range []int{40, 100 << 10, 757272} {
		t.Run(fmt.Sprintf("body=%d", size), func(t *testing.T) {
			sock, records := deadlineHarness(t, 600*time.Millisecond, slowUpstream(1500*time.Millisecond))
			script := `const http=require('http');const {startRelay}=require(process.argv[1]);
(async()=>{const relay=await startRelay(process.argv[2],0);
const size=Number(process.argv[3]);const pad='x'.repeat(Math.max(0,size-60));
const body=JSON.stringify({max_tokens:16384,messages:[{role:'user',content:pad}]});
const req=http.request({host:'127.0.0.1',port:relay.address().port,path:'/v1/chat/completions',method:'POST',headers:{'content-type':'application/json','content-length':Buffer.byteLength(body)}},res=>{
 let n=0,done=false;res.on('data',d=>{n+=d.length;if(String(d).includes('[DONE]'))done=true});
 const fin=o=>{console.log(JSON.stringify({...o,bytes:n,done,bodyBytes:Buffer.byteLength(body)}));process.exit(0)};
 res.on('end',()=>fin({ended:'end',complete:res.complete}));res.on('error',e=>fin({ended:'error',code:e.code}));
 res.on('close',()=>{if(!res.complete)fin({ended:'closed_early'})});});
req.on('error',e=>{console.log(JSON.stringify({ended:'request_error',code:e.code}));process.exit(0)});
req.end(body);setTimeout(()=>{console.log(JSON.stringify({ended:'hang'}));process.exit(0)},8000);})();`
			out, err := exec.Command(node, "-e", script, runner, sock, fmt.Sprint(size)).CombinedOutput()
			if err != nil {
				t.Fatalf("node: %v\n%s", err, out)
			}
			rec := <-records
			t.Logf("client via container relay: %s | go relay: end=%q done=%t elapsed=%dms", strings.TrimSpace(string(out)), rec.End, rec.SawDone, rec.ElapsedMS)
			if !strings.Contains(string(out), `"ended":"end"`) || rec.End != "complete" {
				t.Errorf("slow first byte truncated the stream through the real chain (%d byte body)", size)
			}
		})
	}
}

// If the read deadline ever cancels a request's context while its client is still
// connected, the failure must be loud. Emulate the relay as it was before the body
// drain and check the client sees an error, not a clean end that looks finished.
func TestRelayDeadlineCancelIsLoudNotSilent(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	old := relayDrainBody
	relayDrainBody = false
	t.Cleanup(func() { relayDrainBody = old })
	runner, _ := filepath.Abs("pi-bench/runner.cjs")
	sock, records := deadlineHarness(t, 600*time.Millisecond, slowUpstream(1500*time.Millisecond))
	script := `const http=require('http');const {startRelay}=require(process.argv[1]);
(async()=>{const relay=await startRelay(process.argv[2],0);
const body=JSON.stringify({max_tokens:16384,messages:[{role:'user',content:'hi'}]});
const req=http.request({host:'127.0.0.1',port:relay.address().port,path:'/v1/chat/completions',method:'POST',headers:{'content-type':'application/json','content-length':Buffer.byteLength(body)}},res=>{
 let n=0,done=false;res.on('data',d=>{n+=d.length;if(String(d).includes('[DONE]'))done=true});
 const fin=o=>{console.log(JSON.stringify({...o,bytes:n,done}));process.exit(0)};
 res.on('end',()=>fin({ended:'end',complete:res.complete}));res.on('error',e=>fin({ended:'error',code:e.code}));
 res.on('close',()=>{if(!res.complete)fin({ended:'closed_early'})});});
req.on('error',e=>{console.log(JSON.stringify({ended:'request_error',code:e.code}));process.exit(0)});
req.end(body);setTimeout(()=>{console.log(JSON.stringify({ended:'hang'}));process.exit(0)},8000);})();`
	out, err := exec.Command(node, "-e", script, runner, sock).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	rec := <-records
	t.Logf("unfixed relay, client: %s | relay end=%q", strings.TrimSpace(string(out)), rec.End)
	if rec.End != "client_closed" {
		t.Fatalf("the pre-fix mechanism no longer reproduces (relay end=%q): this Go version changed net/http's body handling, so re-derive the bug before trusting the relay", rec.End)
	}
	if strings.Contains(string(out), `"ended":"end"`) && strings.Contains(string(out), `"complete":true`) {
		t.Fatalf("a cancelled context ended the stream cleanly, indistinguishable from a finished one: %s", strings.TrimSpace(string(out)))
	}
}

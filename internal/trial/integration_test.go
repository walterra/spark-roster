package trial

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/walterra/spark-roster/internal/host"
)

func TestSystemdWorkloadsAndRestore(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("candidate-start-fails=%t", fail), func(t *testing.T) {
			states := map[string]string{"engine.service": "active", "proxy.service": "active", "pi.service": "inactive"}
			var ops []string
			bind := func(id string, units ...string) Workload {
				return Workload{ID: id, Units: units, Probe: func(context.Context) error { ops = append(ops, "probe "+id); return nil },
					ExitCheck:     func(context.Context) error { ops = append(ops, "exit "+id); return nil },
					ArtifactCheck: func(context.Context) error { ops = append(ops, "artifacts "+id); return nil }}
			}
			b, err := NewSystemd(bind("stock", "engine.service", "proxy.service"), bind("pi", "pi.service"), func(context.Context) (uint64, error) { return 100, nil })
			if err != nil {
				t.Fatal(err)
			}
			b.inspect = func(_ context.Context, name string) (host.Unit, error) {
				return host.Unit{Unit: name, LoadState: "loaded", ActiveState: states[name]}, nil
			}
			b.control = func(_ context.Context, action, name string, queued bool) error {
				ops = append(ops, action+" "+name)
				if queued != (action == "start") {
					t.Errorf("unexpected queue setting for %s", action)
				}
				if action == "stop" {
					states[name] = "inactive"
					return nil
				}
				states[name] = "active"
				if fail && name == "pi.service" {
					return errors.New("failed after candidate became active")
				}
				return nil
			}
			done := make(chan struct{})
			err = Run(context.Background(), b, settings(), done, func(e Event) {
				if e.Phase == "operator_handoff" {
					close(done)
				}
			})
			if fail != (err != nil) {
				t.Fatalf("unexpected result: %v", err)
			}
			want := []string{"exit pi", "artifacts stock", "artifacts pi", "probe stock", "stop proxy.service", "stop engine.service", "exit stock", "start pi.service"}
			if !fail {
				want = append(want, "probe pi")
			}
			want = append(want, "stop pi.service", "exit pi", "start engine.service", "start proxy.service", "probe stock")
			if !reflect.DeepEqual(ops, want) {
				t.Fatalf("operations: %v", ops)
			}
		})
	}
}

func TestSystemdRefusesSharedUnit(t *testing.T) {
	ok := func(context.Context) error { return nil }
	stock := Workload{ID: "stock", Units: []string{"same.service"}, Probe: ok, ExitCheck: ok, ArtifactCheck: ok}
	pi := stock
	pi.ID = "pi"
	if _, err := NewSystemd(stock, pi, func(context.Context) (uint64, error) { return 0, nil }); err == nil {
		t.Fatal("shared unit accepted")
	}
}

func TestGenerationResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		pass       bool
	}{
		{"ok", `{"choices":[{"finish_reason":"stop","message":{"content":" OK\n"}}]}`, 200, true},
		{"no answer", `{"choices":[]}`, 200, false},
		{"wrong answer secret", `{"choices":[{"finish_reason":"stop","message":{"content":"private-fixture"}}]}`, 200, false},
		{"redirect", ``, 302, false},
		{"oversized", strings.Repeat("x", 65537), 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != "POST" || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer private-fixture" {
					t.Error("unexpected probe request")
				}
				if tc.status == 302 {
					w.Header().Set("Location", "/redirected")
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()
			port, err := strconv.ParseUint(strings.Split(srv.Listener.Addr().String(), ":")[1], 10, 16)
			if err != nil {
				t.Fatal(err)
			}
			err = CheckGeneration(context.Background(), uint16(port), "test-model", "private-fixture")
			if tc.pass != (err == nil) || err != nil && strings.Contains(err.Error(), "private-fixture") || requests.Load() != 1 {
				t.Fatalf("result %v, requests %d", err, requests.Load())
			}
		})
	}
}

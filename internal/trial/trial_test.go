package trial

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fake struct {
	ops      []string
	fail     string
	lowAt    int
	readings int
}

func (f *fake) op(ctx context.Context, op string) error {
	f.ops = append(f.ops, op)
	if err := ctx.Err(); err != nil {
		return err
	}
	if op == f.fail {
		return errors.New("injected failure")
	}
	return nil
}
func (f *fake) Preflight(c context.Context, _, _ string) error { return f.op(c, "preflight") }
func (f *fake) Stop(c context.Context, s string) error         { return f.op(c, "stop "+s) }
func (f *fake) Start(c context.Context, s string) error        { return f.op(c, "start "+s) }
func (f *fake) Probe(c context.Context, s string) error        { return f.op(c, "probe "+s) }
func (f *fake) Available(c context.Context) (uint64, error) {
	f.readings++
	n := uint64(100)
	if f.readings == f.lowAt {
		n = 2
	}
	return n, f.op(c, "memory")
}
func settings() Settings {
	return Settings{Stock: "stock", Candidate: "pi", StartMin: 60, RestoreMin: 70, Reserve: 10, StartupTimeout: time.Second, TestWindow: time.Second, RecoveryTimeout: time.Second, Poll: time.Millisecond, ProbeTimeout: time.Second}
}

func TestRun(t *testing.T) {
	cases := []struct {
		name, fail, want string
		lowAt            int
		cancel           bool
		ops              []string
	}{
		{name: "handoff and restore", ops: []string{"stop stock", "start pi", "probe pi", "stop pi", "start stock", "probe stock"}},
		{name: "failed candidate start restores", fail: "start pi", want: "start candidate", ops: []string{"stop stock", "start pi", "stop pi", "start stock", "probe stock"}},
		{name: "refuse to overlap during restore", fail: "stop pi", want: "candidate exit not confirmed", ops: []string{"stop stock", "start pi", "probe pi", "stop pi"}},
		{name: "insufficient memory after stop", lowAt: 1, want: "post_stop_memory", ops: []string{"stop stock", "start stock", "probe stock"}},
		{name: "startup memory breach", lowAt: 2, want: "startup_memory", ops: []string{"stop stock", "start pi", "stop pi", "start stock", "probe stock"}},
		{name: "cancel still restores", cancel: true, want: "context canceled", ops: []string{"stop stock", "start pi", "probe pi", "stop pi", "start stock", "probe stock"}},
		{name: "failed stock preflight leaves host untouched", fail: "probe stock", want: "preflight stock generation"},
		{name: "failed preflight leaves host untouched", fail: "preflight", want: "preflight"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fake{fail: tc.fail, lowAt: tc.lowAt}
			done := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var phases []string
			err := Run(ctx, f, settings(), done, func(e Event) {
				phases = append(phases, e.Phase)
				if e.Phase == "operator_handoff" {
					if tc.cancel {
						cancel()
					} else {
						close(done)
					}
				}
			})
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(f.ops[:1], []string{"preflight"}) {
				t.Fatalf("unexpected preflight: %v", f.ops)
			}
			var ops []string
			for _, op := range f.ops {
				if op != "memory" {
					ops = append(ops, op)
				}
			}
			pre := []string{"preflight", "probe stock"}
			if tc.fail == "preflight" {
				pre = pre[:1]
			}
			if !reflect.DeepEqual(ops, append(pre, tc.ops...)) {
				t.Fatalf("unexpected sequence: %v", ops)
			}
			if tc.want == "" && (!strings.Contains(strings.Join(phases, ","), "operator_handoff,operator_complete") || phases[len(phases)-1] != "restored") {
				t.Fatalf("unexpected phases: %v", phases)
			}
		})
	}
}

func TestTimeoutRestores(t *testing.T) {
	f := &fake{}
	s := settings()
	s.TestWindow = time.Nanosecond
	s.Poll = time.Hour
	var phases []string
	if err := Run(context.Background(), f, s, nil, func(e Event) { phases = append(phases, e.Phase) }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(phases[len(phases)-4:], []string{"test_window_expired", "restoring", "restore_memory", "restored"}) {
		t.Fatalf("unexpected phases: %v", phases)
	}
}

func TestCandidateReadinessTimeoutRestoresWithoutHandoff(t *testing.T) {
	f := &fake{fail: "probe pi"}
	s := settings()
	s.StartupTimeout = 20 * time.Millisecond
	s.Poll = time.Hour
	var last string
	err := Run(context.Background(), f, s, nil, func(e Event) {
		last = e.Phase
		if e.Phase == "operator_handoff" {
			t.Error("unready candidate handed to operator")
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) || last != "restored" {
		t.Fatalf("result: %v / %s", err, last)
	}
}

func TestRestoreFailureIsNotSuccess(t *testing.T) {
	f := &fake{fail: "start stock"}
	done := make(chan struct{})
	close(done)
	var last string
	err := Run(context.Background(), f, settings(), done, func(e Event) { last = e.Phase })
	if err == nil || !strings.Contains(err.Error(), "restore failed") || last != "restore_failed" {
		t.Fatalf("result: %v / %s", err, last)
	}
}

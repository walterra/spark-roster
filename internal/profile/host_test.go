package profile

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/walterra/spark-roster/internal/host"
)

func launched(runtime string) Profile {
	p := prof("pi-q8", 1)
	p.Adopt = nil
	p.Launch = &Launch{Runtime: runtime, Args: []string{"--port", "9931"}}
	if runtime == "docker" {
		p.Launch.Image = "lmsysorg/sglang@sha256:" + strings.Repeat("a", 64)
	} else {
		p.Launch.Exe = "/opt/llama/llama-server"
	}
	return p
}

func TestRenderUnitDocker(t *testing.T) {
	p := launched("docker")
	p.Launch.MemoryLimit = "100g"
	p.Launch.Env = map[string]string{"B": "2", "A": "1"}
	u, err := RenderUnit(p, "walterra", "walterra")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"User=walterra", "Restart=no", "Requires=docker.service",
		`ExecStartPre=-/usr/bin/docker rm -f spark-roster-pi-q8`,
		`ExecStop=-/usr/bin/docker stop -t 20 spark-roster-pi-q8`,
		`"--memory" "100g" "--memory-swap" "100g"`,
		`"-e" "A=1" "-e" "B=2"`, // sorted, so output is reproducible
		`"lmsysorg/sglang@sha256:` + strings.Repeat("a", 64) + `" "--port" "9931"`,
	} {
		if !strings.Contains(u, want) {
			t.Errorf("unit lacks %q:\n%s", want, u)
		}
	}
	u2, _ := RenderUnit(p, "walterra", "walterra")
	if u != u2 {
		t.Fatal("rendering is not deterministic")
	}
}

func TestRenderUnitProcess(t *testing.T) {
	u, err := RenderUnit(launched("process"), "walterra", "walterra")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u, `ExecStart="/opt/llama/llama-server" "--port" "9931"`) || !strings.Contains(u, "KillMode=control-group") || strings.Contains(u, "docker") {
		t.Fatalf("%s", u)
	}
}

func TestRenderUnitEscapesSystemdExpansionAndQuotes(t *testing.T) {
	p := launched("process")
	p.Launch.Args = []string{`50%`, `$HOME`, `a "b" c`, `back\slash`}
	u, err := RenderUnit(p, "walterra", "walterra")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"50%%"`, `"$$HOME"`, `"a \"b\" c"`, `"back\\slash"`} {
		if !strings.Contains(u, want) {
			t.Errorf("missing %s in %s", want, u)
		}
	}
}

func TestRenderUnitRefusesInjectionAndBadInputs(t *testing.T) {
	p := launched("process")
	p.Launch.Args = []string{"ok\nExecStartPost=/bin/evil"}
	if _, err := RenderUnit(p, "walterra", "walterra"); err == nil {
		t.Fatal("newline in an argument accepted")
	}
	if _, err := RenderUnit(launched("process"), "root", "root"); err == nil {
		t.Fatal("root accepted as the service user")
	}
	if _, err := RenderUnit(launched("process"), "bad user", "g"); err == nil {
		t.Fatal("invalid user accepted")
	}
	d := launched("docker")
	d.Launch.MemoryLimit = "100g; rm"
	if _, err := RenderUnit(d, "u", "u"); err == nil {
		t.Fatal("bad memory limit accepted")
	}
	a := prof("a", 1)
	if _, err := RenderUnit(a, "u", "u"); err == nil {
		t.Fatal("adopted profile rendered")
	}
	e := launched("process")
	e.Launch.Env = map[string]string{"A B": "x"}
	if _, err := RenderUnit(e, "u", "u"); err == nil {
		t.Fatal("invalid env name accepted")
	}
}

type fakeFile struct {
	fs.FileInfo
	size int64
	dir  bool
}

func (f fakeFile) Size() int64 { return f.size }
func (f fakeFile) IsDir() bool { return f.dir }

type hostFx struct {
	h        *Host
	controls []string
	outputs  map[string]string
	states   map[string]string
	files    map[string]fakeFile
	genErr   error
}

func newHost() *hostFx {
	x := &hostFx{outputs: map[string]string{}, states: map[string]string{}, files: map[string]fakeFile{}}
	x.h = &Host{
		HFCache: "/hf",
		Control: func(_ context.Context, action, unit string) error {
			x.controls = append(x.controls, action+" "+unit)
			if action == "stop" {
				x.states[unit] = "inactive"
			} else {
				x.states[unit] = "active"
			}
			return nil
		},
		Output: func(_ context.Context, name string, args ...string) (string, error) {
			return x.outputs[name+" "+strings.Join(args, " ")], nil
		},
		Inspect: func(_ context.Context, unit string) (host.Unit, error) {
			s, ok := x.states[unit]
			if !ok {
				return host.Unit{Unit: unit, LoadState: "not-found", ActiveState: "inactive"}, nil
			}
			return host.Unit{Unit: unit, LoadState: "loaded", ActiveState: s}, nil
		},
		Memory:   func(context.Context) (uint64, error) { return 64 << 30, nil },
		Generate: func(context.Context, Profile) error { return x.genErr },
		Stat: func(path string) (os.FileInfo, error) {
			if f, ok := x.files[path]; ok {
				return f, nil
			}
			return nil, os.ErrNotExist
		},
	}
	return x
}

func TestHostStopOrderAndContainerCheck(t *testing.T) {
	x := newHost()
	p := prof("q", 1)
	p.Adopt = &Adopt{Units: []string{"engine.service", "proxy.service"}, Container: "engine"}
	x.states["engine.service"], x.states["proxy.service"] = "active", "active"
	if err := x.h.Stop(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(x.controls, []string{"stop proxy.service", "stop engine.service"}) {
		t.Fatalf("stop order %v", x.controls)
	}
	x.outputs["docker ps --filter name=^/engine$ --format {{.Names}}"] = "engine"
	if err := x.h.Stop(context.Background(), p); err == nil || !strings.Contains(err.Error(), "still running") {
		t.Fatalf("running container not detected: %v", err)
	}
}

func TestHostStopFailsWhenUnitStaysActive(t *testing.T) {
	x := newHost()
	p := prof("q", 1)
	x.states["q.service"] = "active"
	x.h.Control = func(context.Context, string, string) error { return nil } // stop "succeeds" but nothing changes
	if err := x.h.Stop(context.Background(), p); err == nil || !strings.Contains(err.Error(), "expected inactive") {
		t.Fatalf("err=%v", err)
	}
}

func TestHostStartOrderAndProbeNeedsActiveUnitsAndGeneration(t *testing.T) {
	x := newHost()
	p := prof("q", 1)
	p.Adopt.Units = []string{"engine.service", "proxy.service"}
	if err := x.h.Start(context.Background(), p); err != nil || !slices.Equal(x.controls, []string{"start engine.service", "start proxy.service"}) {
		t.Fatalf("err=%v %v", err, x.controls)
	}
	if err := x.h.Probe(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	x.genErr = errors.New("no answer")
	if err := x.h.Probe(context.Background(), p); err == nil {
		t.Fatal("probe ignored a failed generation")
	}
	x.genErr = nil
	x.states["proxy.service"] = "failed"
	if err := x.h.Probe(context.Background(), p); err == nil {
		t.Fatal("probe passed with a failed unit")
	}
}

func TestHostStagedChecksEveryArtifactAndReportsAll(t *testing.T) {
	x := newHost()
	p := prof("q", 1)
	rev := strings.Repeat("b", 40)
	p.Artifacts = []Artifact{
		{Kind: "huggingface", Name: "org/m", Revision: rev},
		{Kind: "url", Name: "/models/x.gguf", URL: "https://x/y", SHA256: strings.Repeat("c", 64), Bytes: 10},
		{Kind: "git", Name: "/src/llama", Revision: rev},
	}
	x.states["q.service"] = "inactive"
	err := x.h.Staged(context.Background(), p)
	if err == nil || strings.Count(err.Error(), ";") != 2 {
		t.Fatalf("want three problems, got %v", err)
	}
	x.files["/hf/models--org--m/snapshots/"+rev] = fakeFile{dir: true}
	x.files["/models/x.gguf"] = fakeFile{size: 10}
	x.outputs["git -C /src/llama rev-parse HEAD"] = rev
	if err := x.h.Staged(context.Background(), p); err != nil {
		t.Fatalf("complete staging rejected: %v", err)
	}
	x.files["/models/x.gguf"] = fakeFile{size: 9}
	if err := x.h.Staged(context.Background(), p); err == nil || !strings.Contains(err.Error(), "size 9, expected 10") {
		t.Fatalf("truncated file accepted: %v", err)
	}
}

func TestHostStagedRequiresGeneratedUnitForLaunchedProfiles(t *testing.T) {
	x := newHost()
	p := launched("process")
	if err := x.h.Staged(context.Background(), p); err == nil || !strings.Contains(err.Error(), "run stage") {
		t.Fatalf("err=%v", err)
	}
	x.states[UnitName("pi-q8")] = "inactive"
	if err := x.h.Staged(context.Background(), p); err != nil {
		t.Fatal(err)
	}
}

func TestSudoControlRefusesUnexpectedRequests(t *testing.T) {
	for _, tc := range [][2]string{{"restart", "a.service"}, {"start", "a.service; reboot"}, {"start", "--now"}, {"enable", "a.service"}} {
		if err := sudoControl(context.Background(), tc[0], tc[1]); err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Errorf("%v accepted: %v", tc, err)
		}
	}
}

// The full chain: Journaled over the real Host with fake system calls.
func TestJournaledOverHostSwitchesAdoptedProfiles(t *testing.T) {
	x := newHost()
	a, b := prof("a", 1), prof("b", 1)
	x.states["a.service"], x.states["b.service"] = "active", "inactive"
	j := jr(t)
	_ = j.SetActive("a")
	o := Options{ReserveBytes: 1 << 30, Poll: time.Millisecond, ProbeTimeout: time.Second, RecoveryTimeout: time.Second}
	if err := Journaled(context.Background(), x.h, j, &a, b, o, nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(x.controls, []string{"stop a.service", "start b.service"}) {
		t.Fatalf("%v", x.controls)
	}
	if st, _ := j.Read(); st.Active != "b" {
		t.Fatalf("%+v", st)
	}
}

package profile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

const goodAdopt = `{"schema_version":1,"kind":"profile","id":"site-qwen","title":"Site Qwen",
"artifacts":[{"kind":"huggingface","name":"org/model","revision":"52d1adc5f38aa5ebf099c29ed7025ba34cfbb854","files":[{"path":"config.json","bytes":10}]}],
"adopt":{"units":["engine.service","proxy.service"],"container":"engine"},
"serving":{"port":30001,"model":"m","ready_timeout":"10m"},
"memory":{"start_min_available_bytes":50000000000,"evidence":"estimated","note":"reviewed floor","recorded_at":"2026-11-24T00:00:00Z"}}`

func TestParseAcceptsAdoptedProfile(t *testing.T) {
	p, err := Parse([]byte(goodAdopt))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(p.Units(), []string{"engine.service", "proxy.service"}) || p.Serving.ReadyTimeout.Duration != 10*time.Minute {
		t.Fatalf("%+v", p)
	}
}

func TestParseRejectsBadProfiles(t *testing.T) {
	mutate := func(old, new string) string { return strings.Replace(goodAdopt, old, new, 1) }
	for name, tc := range map[string]struct{ doc, want string }{
		"branch instead of commit": {mutate("52d1adc5f38aa5ebf099c29ed7025ba34cfbb854", "main"), "40-character commit"},
		"unknown field":            {mutate(`"title":"Site Qwen",`, `"title":"Site Qwen","typo":1,`), "unknown field"},
		"neither adopt nor launch": {mutate(`"adopt":{"units":["engine.service","proxy.service"],"container":"engine"},`, ""), "exactly one of adopt and launch"},
		"bad unit":                 {mutate(`"engine.service"`, `"engine; rm -rf"`), "invalid or duplicate unit"},
		"duplicate unit":           {mutate(`"proxy.service"`, `"engine.service"`), "invalid or duplicate unit"},
		"zero port":                {mutate(`"port":30001`, `"port":0`), "port and a model"},
		"no ready timeout":         {mutate(`"ready_timeout":"10m"`, `"ready_timeout":"0s"`), "ready_timeout"},
		"bad duration":             {mutate(`"10m"`, `10`), "duration"},
		"memory without note":      {mutate(`"note":"reviewed floor"`, `"note":" "`), "memory needs"},
		"bad evidence":             {mutate(`"estimated"`, `"guessed"`), "measured|estimated"},
		"bad id":                   {mutate(`"id":"site-qwen"`, `"id":"Site Qwen"`), "id must match"},
		"trailing data":            {goodAdopt + ` {}`, "trailing data"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestLaunchProfilesNeedPinnedImagesAndOwnUnit(t *testing.T) {
	doc := `{"schema_version":1,"kind":"profile","id":"pi-q8","title":"Pi Q8","artifacts":[],
"launch":{"runtime":"docker","image":"lmsysorg/sglang:latest","args":["serve"]},
"serving":{"port":1,"model":"m","ready_timeout":"1m"},
"memory":{"start_min_available_bytes":1,"evidence":"estimated","note":"n","recorded_at":"2026-11-24T00:00:00Z"}}`
	if _, err := Parse([]byte(doc)); err == nil || !strings.Contains(err.Error(), "pinned by sha256 digest") {
		t.Fatalf("moving tag accepted: %v", err)
	}
	pinned := strings.Replace(doc, "lmsysorg/sglang:latest", "lmsysorg/sglang@sha256:"+strings.Repeat("a", 64), 1)
	p, err := Parse([]byte(pinned))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(p.Units(), []string{"spark-roster-pi-q8.service"}) {
		t.Fatalf("units %v", p.Units())
	}
}

// fake records every host operation in order.
type fake struct {
	log       []string
	available uint64
	stagedErr map[string]error
	stopErr   map[string]error
	startErr  map[string]error
	probe     func(id string, n int) error
	probes    map[string]int
	running   map[string]bool
}

func newFake() *fake {
	return &fake{available: 80 << 30, probes: map[string]int{}, running: map[string]bool{}, stagedErr: map[string]error{}, stopErr: map[string]error{}, startErr: map[string]error{}}
}
func (f *fake) Staged(_ context.Context, p Profile) error {
	f.log = append(f.log, "staged "+p.ID)
	return f.stagedErr[p.ID]
}
func (f *fake) Stop(_ context.Context, p Profile) error {
	f.log = append(f.log, "stop "+p.ID)
	if e := f.stopErr[p.ID]; e != nil {
		return e
	}
	f.running[p.ID] = false
	return nil
}
func (f *fake) Start(_ context.Context, p Profile) error {
	f.log = append(f.log, "start "+p.ID)
	f.running[p.ID] = true // even a failing start may leave something running
	return f.startErr[p.ID]
}
func (f *fake) Probe(_ context.Context, p Profile) error {
	f.probes[p.ID]++
	if f.probe != nil {
		return f.probe(p.ID, f.probes[p.ID])
	}
	return nil
}
func (f *fake) Available(context.Context) (uint64, error) { return f.available, nil }

func prof(id string, startMin int64) Profile {
	return Profile{SchemaVersion: 1, Kind: "profile", ID: id, Title: id,
		Adopt:   &Adopt{Units: []string{id + ".service"}},
		Serving: Serving{Port: 1, Model: "m", ReadyTimeout: Duration{time.Second}},
		Memory:  Memory{StartMinAvailableBytes: startMin, Evidence: "estimated", Note: "n", RecordedAt: time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)}}
}

func opts() Options {
	return Options{ReserveBytes: 10 << 30, Poll: time.Millisecond, ProbeTimeout: time.Second, RecoveryTimeout: time.Second}
}

func TestSwitchSuccessOrdersStopStartProbe(t *testing.T) {
	f := newFake()
	a, b := prof("a", 20<<30), prof("b", 20<<30)
	var phases []string
	if err := Switch(context.Background(), f, &a, b, opts(), func(e Event) { phases = append(phases, e.Phase) }); err != nil {
		t.Fatal(err)
	}
	if want := []string{"staged b", "stop a", "start b"}; !slices.Equal(f.log, want) {
		t.Fatalf("log %v", f.log)
	}
	if f.probes["b"] != 1 || f.probes["a"] != 0 {
		t.Fatalf("probes %v", f.probes)
	}
	if phases[len(phases)-1] != "ready" {
		t.Fatalf("phases %v", phases)
	}
}

func TestSwitchRefusesWhenTargetNotStagedWithoutStoppingAnything(t *testing.T) {
	f := newFake()
	f.stagedErr["b"] = errors.New("weights missing")
	a, b := prof("a", 1), prof("b", 1)
	err := Switch(context.Background(), f, &a, b, opts(), nil)
	if err == nil || !strings.Contains(err.Error(), "not staged") || !slices.Equal(f.log, []string{"staged b"}) {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

func TestSwitchRefusesSameProfile(t *testing.T) {
	f := newFake()
	a := prof("a", 1)
	if err := Switch(context.Background(), f, &a, a, opts(), nil); err == nil || len(f.log) != 0 {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

func TestSwitchRestoresWhenTargetNeverGenerates(t *testing.T) {
	f := newFake()
	f.probe = func(id string, n int) error {
		if id == "b" {
			return errors.New("no answer")
		}
		return nil
	}
	a, b := prof("a", 20<<30), prof("b", 20<<30)
	b.Serving.ReadyTimeout = Duration{30 * time.Millisecond}
	err := Switch(context.Background(), f, &a, b, opts(), nil)
	if err == nil || !strings.Contains(err.Error(), "did not generate") {
		t.Fatalf("err=%v", err)
	}
	if want := []string{"staged b", "stop a", "start b", "stop b", "start a"}; !slices.Equal(f.log, want) {
		t.Fatalf("log %v", f.log)
	}
	if !f.running["a"] || f.running["b"] {
		t.Fatalf("running %v", f.running)
	}
}

func TestSwitchRestoresAfterPartialStart(t *testing.T) {
	f := newFake()
	f.startErr["b"] = errors.New("unit failed after launching a container")
	a, b := prof("a", 1), prof("b", 1)
	err := Switch(context.Background(), f, &a, b, opts(), nil)
	if err == nil || !slices.Equal(f.log, []string{"staged b", "stop a", "start b", "stop b", "start a"}) || f.running["b"] {
		t.Fatalf("err=%v log=%v running=%v", err, f.log, f.running)
	}
}

func TestSwitchNeverStartsRestoreWhenTargetExitUnconfirmed(t *testing.T) {
	f := newFake()
	f.startErr["b"] = errors.New("boom")
	f.stopErr["b"] = errors.New("container still running")
	a, b := prof("a", 1), prof("b", 1)
	err := Switch(context.Background(), f, &a, b, opts(), nil)
	if err == nil || !strings.Contains(err.Error(), "exit not confirmed") || slices.Contains(f.log[3:], "start a") {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

func TestSwitchStopsBeforeStartingWhenSourceWillNotStop(t *testing.T) {
	f := newFake()
	f.stopErr["a"] = errors.New("busy")
	a, b := prof("a", 1), prof("b", 1)
	err := Switch(context.Background(), f, &a, b, opts(), nil)
	if err == nil || slices.Contains(f.log, "start b") {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

func TestSwitchChecksMemoryAfterStopAndRestoresWithoutStartingTarget(t *testing.T) {
	f := newFake()
	f.available = 5 << 30 // below b's 20 GiB start floor
	a, b := prof("a", 1), prof("b", 20<<30)
	err := Switch(context.Background(), f, &a, b, opts(), nil)
	if err == nil || !strings.Contains(err.Error(), "below the") || slices.Contains(f.log, "start b") {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
	// a's own floor is 1 byte, so restore proceeds and a runs again.
	if !f.running["a"] || !strings.HasSuffix(strings.Join(f.log, ","), "start a") {
		t.Fatalf("log=%v running=%v", f.log, f.running)
	}
}

func TestSwitchRestoresOnCancellation(t *testing.T) {
	f := newFake()
	ctx, cancel := context.WithCancel(context.Background())
	f.probe = func(id string, n int) error {
		if id == "b" {
			cancel()
			return errors.New("cancelled")
		}
		return nil
	}
	a, b := prof("a", 1), prof("b", 1)
	err := Switch(ctx, f, &a, b, opts(), nil)
	if err == nil || !f.running["a"] || f.running["b"] {
		t.Fatalf("err=%v running=%v log=%v", err, f.running, f.log)
	}
}

func TestSwitchFromNothingStopsTargetOnFailure(t *testing.T) {
	f := newFake()
	f.startErr["b"] = errors.New("boom")
	b := prof("b", 1)
	if err := Switch(context.Background(), f, nil, b, opts(), nil); err == nil || f.running["b"] {
		t.Fatalf("err=%v running=%v", err, f.running)
	}
}

func TestSwitchRejectsInvalidTargetBeforeTouchingHost(t *testing.T) {
	f := newFake()
	bad := prof("b", 1)
	bad.Serving.Port = 0
	if err := Switch(context.Background(), f, nil, bad, opts(), nil); err == nil || len(f.log) != 0 {
		t.Fatalf("err=%v log=%v", err, f.log)
	}
}

func TestPlanSwitchMatchesSwitchOrder(t *testing.T) {
	a, b := prof("a", 1), prof("b", 1)
	steps := PlanSwitch(&a, b)
	var s []string
	for _, st := range steps {
		s = append(s, fmt.Sprintf("%s:%s", st.Action, st.Profile))
	}
	if len(steps) != 6 || steps[1].Profile != "a" || steps[3].Action != "start" || steps[5].Action != "on failure: confirm exit, restore" {
		t.Fatalf("%v", s)
	}
	if got := PlanSwitch(nil, b); len(got) != 4 {
		t.Fatalf("%v", got)
	}
}

func TestParseRejectsHuggingFaceArtifactWithoutFileList(t *testing.T) {
	doc := strings.Replace(goodAdopt, `,"files":[{"path":"config.json","bytes":10}]`, "", 1)
	if _, err := Parse([]byte(doc)); err == nil || !strings.Contains(err.Error(), "files list") {
		t.Fatalf("err=%v", err)
	}
	for _, bad := range []string{`{"path":"../x","bytes":1}`, `{"path":"/abs","bytes":1}`, `{"path":"a","bytes":-1}`, `{"path":"a","bytes":1,"sha256":"short"}`} {
		d := strings.Replace(goodAdopt, `{"path":"config.json","bytes":10}`, bad, 1)
		if _, err := Parse([]byte(d)); err == nil || !strings.Contains(err.Error(), "invalid file entry") {
			t.Errorf("%s accepted: %v", bad, err)
		}
	}
}

func TestExpandReplacesOnlyKnownVariables(t *testing.T) {
	p := launched("docker")
	p.Launch.Args = []string{"--cache", "{hf_cache}", "--json", `{"a":1}`, "{home}/x"}
	p.Launch.Mounts = []string{"{data}/m:/model:ro"}
	p.Launch.Env = map[string]string{"X": "{home}"}
	p.Serving.KeyFile = "{home}/.config/key"
	p.Artifacts = []Artifact{{Kind: "git", Name: "{data}/llama", Revision: strings.Repeat("a", 40), URL: "https://x/y", Build: [][]string{{"cmake", "-S", "{dir}", "-B", "{dir}/build"}}, Creates: "{data}/llama/build/bin/s"}}
	q, err := Expand(p, Vars{Home: "/h", Data: "/d", HFCache: "/hf"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(q.Launch.Args, []string{"--cache", "/hf", "--json", `{"a":1}`, "/h/x"}) || q.Launch.Mounts[0] != "/d/m:/model:ro" || q.Launch.Env["X"] != "/h" || q.Serving.KeyFile != "/h/.config/key" {
		t.Fatalf("%+v", q.Launch)
	}
	if b := q.Artifacts[0].Build[0]; !slices.Equal(b, []string{"cmake", "-S", "/d/llama", "-B", "/d/llama/build"}) || q.Artifacts[0].Creates != "/d/llama/build/bin/s" {
		t.Fatalf("%v", q.Artifacts[0])
	}
	if p.Launch.Args[1] != "{hf_cache}" {
		t.Fatal("Expand modified its input")
	}
	if _, err := Expand(p, Vars{Home: "relative", Data: "/d", HFCache: "/hf"}); err == nil {
		t.Fatal("relative home accepted")
	}
}

func TestLaunchValidationCoversMountsAndRuntimeMismatch(t *testing.T) {
	p := launched("docker")
	p.Launch.Mounts = []string{"nodest"}
	if p.Validate() == nil {
		t.Fatal("mount without destination accepted")
	}
	p.Launch.Mounts = []string{"/a:/b:wx"}
	if p.Validate() == nil {
		t.Fatal("bad mount mode accepted")
	}
	q := launched("process")
	q.Launch.Mounts = []string{"/a:/b"}
	if q.Validate() == nil {
		t.Fatal("mounts accepted on a process profile")
	}
	g := launched("process")
	g.Artifacts = []Artifact{{Kind: "git", Name: "/s", Revision: strings.Repeat("a", 40), URL: "http://x", Creates: "x"}}
	if g.Validate() == nil {
		t.Fatal("plain http git url accepted")
	}
	g.Artifacts[0].URL = "https://x/y"
	g.Artifacts[0].Build = [][]string{{"make"}}
	g.Artifacts[0].Creates = ""
	if g.Validate() == nil {
		t.Fatal("build without creates accepted")
	}
}

func TestParseRejectsMemoryWithoutRecordedAt(t *testing.T) {
	doc := strings.Replace(goodAdopt, `,"recorded_at":"2026-11-24T00:00:00Z"`, "", 1)
	if _, err := Parse([]byte(doc)); err == nil || !strings.Contains(err.Error(), "recorded_at") {
		t.Fatalf("err=%v", err)
	}
	bad := strings.Replace(goodAdopt, `"kind":"huggingface",`, `"kind":"huggingface","role":"weights",`, 1)
	if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "unknown role") {
		t.Fatalf("err=%v", err)
	}
}

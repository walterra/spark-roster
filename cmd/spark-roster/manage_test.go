package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/walterra/spark-roster/internal/profile"
)

// fakeHost models running state per profile so switches behave like a host.
type fakeHost struct {
	running   map[string]bool
	staged    map[string]error
	startErr  map[string]error
	probeErr  map[string]error
	stopErr   map[string]error
	available uint64
	sudoErr   error
	log       []string
	// Timers and other units outside any profile, for benchmark pauses.
	unitsActive map[string]bool
	unitErr     map[string]error
	// unitStuck lists units that ignore a stop request, as a hung timer would.
	unitStuck map[string]bool
}

func newFakeHost() *fakeHost {
	return &fakeHost{running: map[string]bool{}, staged: map[string]error{}, startErr: map[string]error{}, probeErr: map[string]error{}, stopErr: map[string]error{}, available: 100 << 30}
}
func (f *fakeHost) Staged(_ context.Context, p profile.Profile) error {
	return f.staged[p.ID]
}
func (f *fakeHost) Stop(_ context.Context, p profile.Profile) error {
	f.log = append(f.log, "stop "+p.ID)
	if e := f.stopErr[p.ID]; e != nil {
		return e
	}
	f.running[p.ID] = false
	return nil
}
func (f *fakeHost) Start(_ context.Context, p profile.Profile) error {
	f.log = append(f.log, "start "+p.ID)
	f.running[p.ID] = true
	return f.startErr[p.ID]
}
func (f *fakeHost) Probe(_ context.Context, p profile.Profile) error {
	if e := f.probeErr[p.ID]; e != nil {
		return e
	}
	if !f.running[p.ID] {
		return errors.New("not serving")
	}
	return nil
}
func (f *fakeHost) Available(context.Context) (uint64, error) { return f.available, nil }
func (f *fakeHost) Units(_ context.Context, p profile.Profile) ([]profile.UnitState, error) {
	state := "inactive"
	if f.running[p.ID] {
		state = "active"
	}
	var out []profile.UnitState
	for _, u := range p.Units() {
		out = append(out, profile.UnitState{Unit: u, Load: "loaded", Active: state})
	}
	return out, nil
}
func (f *fakeHost) SudoReady(context.Context) error { return f.sudoErr }

// Unit control for benchmark pauses. Units are "active" until stopped.
func (f *fakeHost) UnitActive(_ context.Context, unit string) (bool, error) {
	if f.unitsActive == nil {
		return false, nil
	}
	return f.unitsActive[unit], nil
}
func (f *fakeHost) ControlUnit(_ context.Context, action, unit string) error {
	f.log = append(f.log, action+" "+unit)
	if e := f.unitErr[action+" "+unit]; e != nil {
		return e
	}
	if f.unitsActive == nil {
		f.unitsActive = map[string]bool{}
	}
	if action == "stop" && f.unitStuck[unit] {
		return nil
	}
	f.unitsActive[unit] = action == "start"
	return nil
}
func (f *fakeHost) ConfirmInactive(_ context.Context, unit string) error {
	if f.unitsActive != nil && f.unitsActive[unit] {
		return errors.New(unit + " is still active")
	}
	return nil
}

type fakeStager struct {
	plan     profile.StagePlan
	planErr  error
	stageErr error
	staged   []string
}

func (s *fakeStager) Plan(_ context.Context, p profile.Profile) (profile.StagePlan, error) {
	pl := s.plan
	pl.Profile = p.ID
	return pl, s.planErr
}
func (s *fakeStager) Stage(_ context.Context, p profile.Profile) error {
	s.staged = append(s.staged, p.ID)
	return s.stageErr
}

type fx struct {
	t         *testing.T
	h         *fakeHost
	s         *fakeStager
	dir       string
	profiles  string
	interact  bool
	stdin     string
	portErr   error
	lockErr   error
	installed bool
	euid      int
	chowned   []string
	// h2, when set, replaces the fake host behind the CLI. It lets a test observe calls.
	h2 hostOps
}

func newFx(t *testing.T) *fx {
	dir := t.TempDir()
	return &fx{t: t, h: newFakeHost(), s: &fakeStager{plan: profile.StagePlan{Enough: true}}, dir: dir, profiles: filepath.Join(dir, "profiles"), euid: 1000}
}

func (x *fx) env() env {
	return env{
		home: x.dir, getenv: func(string) string { return "" }, interactive: x.interact, stdin: strings.NewReader(x.stdin),
		user: "operator", group: "operator", goos: "linux", goarch: "arm64",
		backend: func(paths) (hostOps, stager) {
			if x.h2 != nil {
				return x.h2, x.s
			}
			return x.h, x.s
		},
		portFree: func(uint16) error { return x.portErr },
		lock: func(string) (func(), error) {
			if x.lockErr != nil {
				return nil, x.lockErr
			}
			return func() {}, nil
		},
		unitInstalled: func(string, string) (bool, error) { return x.installed, nil },
		output:        func(context.Context, string, ...string) (string, error) { return "ok", nil },
		available:     func(string) (uint64, error) { return 500 << 30, nil },
		memory:        func(context.Context) (uint64, error) { return 100 << 30, nil },
		euid:          x.euid,
		exe:           func() (string, error) { return "/home/operator/bin/spark-roster", nil },
		sha256:        func(string) (string, error) { return strings.Repeat("ab", 32), nil },
		now:           func() time.Time { return time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC) },
		chown: func(path string, uid, gid int) error {
			x.chowned = append(x.chowned, path)
			return nil
		},
		lookupUser: func(name string) (OperatorInfo, error) {
			if name != "operator" {
				return OperatorInfo{}, errors.New("unknown user")
			}
			return OperatorInfo{Home: x.dir, UID: 1000, GID: 1000}, nil
		},
	}
}

func (x *fx) run(args ...string) (int, string, string) {
	var out, diag bytes.Buffer
	args = append(args, "--data", filepath.Join(x.dir, "data"), "--profiles", x.profiles)
	code, ok := manage(context.Background(), args, &out, &diag, x.env())
	if !ok {
		x.t.Fatalf("%v is not a managed command", args)
	}
	return code, out.String(), diag.String()
}

const a = "qwen3-8-27b-pi-q8"
const b = "inkling-2-mini"

func (x *fx) journalState() profile.State {
	st, err := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}.Read()
	if err != nil {
		x.t.Fatal(err)
	}
	return st
}

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("not one JSON document: %v\n%s", err, s)
	}
	return m
}

func TestListShowsBuiltinsAsTableAndJSON(t *testing.T) {
	x := newFx(t)
	code, out, _ := x.run("list")
	if code != exitOK || !strings.Contains(out, a) || !strings.Contains(out, b) || !strings.Contains(out, "qwen3-8-27b-dflash2") || !strings.Contains(out, "built-in") {
		t.Fatalf("%d\n%s", code, out)
	}
	code, out, _ = x.run("list", "--json")
	doc := decode(t, out)
	if code != exitOK || doc["kind"] != "profile_list" || len(doc["profiles"].([]any)) != 3 {
		t.Fatalf("%d %v", code, doc)
	}
	row := doc["profiles"].([]any)[0].(map[string]any)
	for _, k := range []string{"id", "mode", "port", "model", "memory_floor_bytes", "memory_evidence", "source"} {
		if _, ok := row[k]; !ok {
			t.Errorf("list row lacks %s", k)
		}
	}
}

func TestListIncludesSiteProfilesAndMarksThem(t *testing.T) {
	x := newFx(t)
	if err := os.MkdirAll(x.profiles, 0o755); err != nil {
		t.Fatal(err)
	}
	doc := `{"schema_version":1,"kind":"profile","id":"site-one","title":"Site","artifacts":[],"adopt":{"units":["e.service"]},"serving":{"port":4000,"model":"m","ready_timeout":"1m"},"memory":{"start_min_available_bytes":1,"evidence":"estimated","note":"n","recorded_at":"2026-11-24T00:00:00Z"}}`
	if err := os.WriteFile(filepath.Join(x.profiles, "site-one.json"), []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	_, out, _ := x.run("list")
	if !strings.Contains(out, "site-one") || !strings.Contains(out, "adopt") || !strings.Contains(out, "site") {
		t.Fatalf("%s", out)
	}
}

func TestUsageErrorsExitTwoAndChangeNothing(t *testing.T) {
	x := newFx(t)
	for _, args := range [][]string{{"switch"}, {"switch", a, b}, {"stage"}, {"list", "--dry-run"}, {"doctor", "--yes"}, {"status", "extra"}, {"switch", "no-such-profile", "--yes"}, {"list", "--bogus"}} {
		code, _, diag := x.run(args...)
		if code != exitUsage {
			t.Errorf("%v: exit %d, diag %q", args, code, diag)
		}
	}
	if len(x.h.log) != 0 {
		t.Fatalf("host touched: %v", x.h.log)
	}
	_, _, diag := x.run("switch", "nope", "--yes")
	if !strings.Contains(diag, "unknown profile") || !strings.Contains(diag, a) {
		t.Fatalf("unknown profile message does not list the choices: %q", diag)
	}
}

func TestFlagsMayFollowTheProfile(t *testing.T) {
	x := newFx(t)
	code, out, _ := x.run("switch", a, "--dry-run", "--json")
	if code != exitOK || decode(t, out)["outcome"] != "dry_run" {
		t.Fatalf("%d %s", code, out)
	}
}

func TestStatusReportsStateStagingAndNextStep(t *testing.T) {
	x := newFx(t)
	x.h.staged[b] = errors.New("weights missing")
	code, out, _ := x.run("status", "--json")
	doc := decode(t, out)
	if code != exitOK || len(doc["running"].([]any)) != 0 {
		t.Fatalf("%d %v", code, doc)
	}
	next := doc["next"].([]any)
	if len(next) == 0 || !strings.HasPrefix(next[0].(string), "spark-roster switch ") {
		t.Fatalf("next %v", next)
	}
	for _, p := range doc["profiles"].([]any) {
		row := p.(map[string]any)
		if row["id"] == b && (row["staged"] != false || row["detail"] == "") {
			t.Fatalf("%v", row)
		}
	}
	x.h.running[a] = true
	_, out, _ = x.run("status")
	if !strings.Contains(out, a) || !strings.Contains(out, "running") {
		t.Fatalf("%s", out)
	}
}

func TestSwitchDryRunChangesNothingAndNeedsNoSudo(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	x.h.sudoErr = errors.New("sudo needs a password")
	code, out, _ := x.run("switch", b, "--dry-run")
	if code != exitOK || !strings.Contains(out, "Nothing was changed") || !strings.Contains(out, "stop and confirm exit "+a) {
		t.Fatalf("%d\n%s", code, out)
	}
	if len(x.h.log) != 0 {
		t.Fatalf("dry run touched the host: %v", x.h.log)
	}
	if _, err := os.Stat(filepath.Join(x.dir, "data", "state.json")); err == nil {
		t.Fatal("dry run wrote a journal")
	}
}

func TestSwitchWithoutYesAndWithoutTerminalRefusesBeforeStopping(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	code, out, diag := x.run("switch", b, "--json")
	doc := decode(t, out)
	if code != exitPrecondition || doc["outcome"] != "needs_confirmation" || !strings.Contains(diag+out, "--yes") {
		t.Fatalf("%d %v", code, doc)
	}
	if len(x.h.log) != 0 || !x.h.running[a] {
		t.Fatalf("service touched without confirmation: %v", x.h.log)
	}
}

func TestSwitchInteractivePromptAcceptsOnlyYes(t *testing.T) {
	for stdin, want := range map[string]int{"y\n": exitOK, "yes\n": exitOK, "n\n": exitPrecondition, "\n": exitPrecondition, "sure\n": exitPrecondition} {
		x := newFx(t)
		x.interact, x.stdin = true, stdin
		code, _, _ := x.run("switch", a)
		if code != want {
			t.Errorf("input %q: exit %d want %d", stdin, code, want)
		}
	}
}

func TestSwitchSuccessStopsSourceStartsTargetAndJournals(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	if err := (profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}).SetActive(a); err != nil {
		t.Fatal(err)
	}
	code, out, _ := x.run("switch", b, "--yes", "--json")
	doc := decode(t, out)
	if code != exitOK || doc["outcome"] != "ready" || doc["from"] != a || doc["to"] != b {
		t.Fatalf("%d %v", code, doc)
	}
	if !slices.Equal(x.h.log, []string{"stop " + a, "start " + b}) {
		t.Fatalf("log %v", x.h.log)
	}
	if st := x.journalState(); st.Active != b || st.Switching != nil {
		t.Fatalf("%+v", st)
	}
}

func TestSwitchRefusesUnstagedTargetWithTheStageCommandAndChangesNothing(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	x.h.staged[b] = errors.New("model files missing")
	code, out, _ := x.run("switch", b, "--yes", "--json")
	doc := decode(t, out)
	if code != exitPrecondition || !strings.Contains(doc["error"].(string), "spark-roster stage "+b) || len(x.h.log) != 0 {
		t.Fatalf("%d %v log=%v", code, doc, x.h.log)
	}
}

func TestSwitchRefusesPortInUseAndMissingSudo(t *testing.T) {
	x := newFx(t)
	x.portErr = errors.New("address already in use")
	if code, _, diag := x.run("switch", a, "--yes"); code != exitPrecondition || !strings.Contains(diag, "port 9931 is in use") {
		t.Fatalf("%d %s", code, diag)
	}
	x = newFx(t)
	x.h.sudoErr = errors.New("sudo needs a password. Run `sudo -v`")
	if code, _, diag := x.run("switch", a, "--yes"); code != exitPrecondition || !strings.Contains(diag, "sudo -v") || len(x.h.log) != 0 {
		t.Fatalf("%d %s %v", code, diag, x.h.log)
	}
}

func TestSwitchToTheRunningProfileIsANoOp(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	code, out, _ := x.run("switch", a, "--yes", "--json")
	if code != exitOK || decode(t, out)["outcome"] != "already_active" || len(x.h.log) != 0 {
		t.Fatalf("%d %s %v", code, out, x.h.log)
	}
}

func TestSwitchExitFourWhenTargetFailsAndSourceIsRestored(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	x.h.startErr[b] = errors.New("container failed to start")
	code, out, _ := x.run("switch", b, "--yes", "--json")
	doc := decode(t, out)
	if code != exitRestored || doc["outcome"] != "failed_restored" {
		t.Fatalf("%d %v", code, doc)
	}
	if !x.h.running[a] || x.h.running[b] {
		t.Fatalf("running %v", x.h.running)
	}
	if st := x.journalState(); st.Active != a || st.Switching != nil || st.Unresolved != "" {
		t.Fatalf("%+v", st)
	}
}

func TestSwitchExitFiveWhenRestoreFailsAndRecoverIsOffered(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	x.h.startErr[b] = errors.New("boom")
	x.h.stopErr[b] = errors.New("container still running")
	code, out, _ := x.run("switch", b, "--yes", "--json")
	doc := decode(t, out)
	if code != exitUnsettled || doc["outcome"] != "unsettled" || !slices.Contains(toStrings(doc["next"]), "spark-roster recover") {
		t.Fatalf("%d %v", code, doc)
	}
	// The next switch is refused until recover runs, and says so.
	before := len(x.h.log)
	code, _, diag := x.run("switch", a, "--yes")
	if code != exitPrecondition || len(x.h.log) != before || !strings.Contains(diag, "recover") {
		t.Fatalf("%d %q %v", code, diag, x.h.log[before:])
	}
}

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestSwitchRefusesAmbiguousHostState(t *testing.T) {
	x := newFx(t)
	x.h.running[a], x.h.running[b] = true, true
	code, _, diag := x.run("switch", "qwen3-8-27b-dflash2", "--yes")
	if code != exitPrecondition || !strings.Contains(diag, "more than one profile is running") || len(x.h.log) != 0 {
		t.Fatalf("%d %q %v", code, diag, x.h.log)
	}
	// Journal says one thing, host another.
	x = newFx(t)
	_ = (profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}).SetActive(a)
	x.h.running[b] = true
	if code, _, diag = x.run("switch", "qwen3-8-27b-dflash2", "--yes"); code != exitPrecondition || !strings.Contains(diag, "journal says") {
		t.Fatalf("%d %q", code, diag)
	}
}

func TestSwitchFromNothingRunning(t *testing.T) {
	x := newFx(t)
	code, out, _ := x.run("switch", b, "--yes", "--json")
	if code != exitOK || decode(t, out)["from"] != nil || !slices.Equal(x.h.log, []string{"start " + b}) {
		t.Fatalf("%d %s %v", code, out, x.h.log)
	}
}

func TestSwitchRefusedWhenAnotherOperationHoldsTheLock(t *testing.T) {
	x := newFx(t)
	x.lockErr = errors.New("another SparkRoster operation is running")
	code, _, diag := x.run("switch", a, "--yes")
	if code != exitPrecondition || !strings.Contains(diag, "another SparkRoster operation") || len(x.h.log) != 0 {
		t.Fatalf("%d %q %v", code, diag, x.h.log)
	}
}

func TestStopConfirmsExitAndClearsActive(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	_ = (profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}).SetActive(a)
	if code, _, _ := x.run("stop"); code != exitPrecondition || !x.h.running[a] {
		t.Fatalf("stop without --yes changed state: %d", code)
	}
	if code, out, _ := x.run("stop", "--dry-run"); code != exitOK || !strings.Contains(out, "Would stop "+a) || !x.h.running[a] {
		t.Fatalf("%d %s", code, out)
	}
	code, out, _ := x.run("stop", "--yes", "--json")
	if code != exitOK || decode(t, out)["outcome"] != "stopped" || x.h.running[a] || x.journalState().Active != "" {
		t.Fatalf("%d %s", code, out)
	}
	if code, out, _ := x.run("stop", "--yes", "--json"); code != exitOK || decode(t, out)["outcome"] != "nothing_running" {
		t.Fatalf("%d %s", code, out)
	}
}

func TestStopFailureIsAnErrorAndKeepsTheRecord(t *testing.T) {
	x := newFx(t)
	x.h.running[a] = true
	x.h.stopErr[a] = errors.New("unit would not stop")
	_ = (profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}).SetActive(a)
	if code, _, _ := x.run("stop", "--yes"); code != exitError || x.journalState().Active != a {
		t.Fatalf("code %d active %q", code, x.journalState().Active)
	}
}

func TestRecoverSettlesAnInterruptedSwitch(t *testing.T) {
	x := newFx(t)
	j := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}
	if err := x.writePending(j, a, b); err != nil {
		t.Fatal(err)
	}
	x.h.running[b] = true
	if code, out, _ := x.run("recover", "--dry-run"); code != exitOK || !strings.Contains(out, "Nothing was changed") || len(x.h.log) != 0 {
		t.Fatalf("%d %s", code, out)
	}
	code, out, _ := x.run("recover", "--yes", "--json")
	if code != exitOK || decode(t, out)["outcome"] != "recovered" || !x.h.running[a] || x.h.running[b] {
		t.Fatalf("%d %s running=%v", code, out, x.h.running)
	}
	if st := x.journalState(); st.Active != a || st.Switching != nil {
		t.Fatalf("%+v", st)
	}
	if code, out, _ := x.run("recover", "--yes", "--json"); code != exitOK || decode(t, out)["outcome"] != "nothing_to_recover" {
		t.Fatalf("%d %s", code, out)
	}
}

func (x *fx) writePending(j profile.Journal, from, to string) error {
	// Drive the real journal into the state a killed switch leaves behind.
	x.h.running[from] = true
	p := func(id string) profile.Profile { return x.mustProfile(id) }
	f, t := p(from), p(to)
	x.h.startErr[to] = errors.New("killed")
	x.h.stopErr[to] = errors.New("killed before exit confirmed")
	_ = j.SetActive(from)
	_ = profile.Journaled(context.Background(), x.h, j, &f, t, profile.Options{ReserveBytes: 1, Poll: time.Millisecond, ProbeTimeout: time.Second, RecoveryTimeout: time.Second}, nil)
	delete(x.h.startErr, to)
	delete(x.h.stopErr, to)
	x.h.log = nil
	return nil
}

func (x *fx) mustProfile(id string) profile.Profile {
	set, err := profile.Builtin()
	if err != nil {
		x.t.Fatal(err)
	}
	p, err := profile.Expand(set[id], profile.Vars{Home: x.dir, Data: filepath.Join(x.dir, "data"), HFCache: filepath.Join(x.dir, "hf")})
	if err != nil {
		x.t.Fatal(err)
	}
	return p
}

func TestRecoverFailureExitsFive(t *testing.T) {
	x := newFx(t)
	j := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}
	_ = x.writePending(j, a, b)
	x.h.startErr[a] = errors.New("will not start")
	if code, out, _ := x.run("recover", "--yes", "--json"); code != exitUnsettled || decode(t, out)["outcome"] != "unsettled" {
		t.Fatalf("%d %s", code, out)
	}
}

func TestStageDryRunReportsPlanWithoutDownloading(t *testing.T) {
	x := newFx(t)
	x.s.plan = profile.StagePlan{Enough: true, NeedBytes: 50 << 30, ImageBytes: 16 << 30, Artifacts: []profile.ArtifactPlan{{Kind: "image", Name: "lmsysorg/sglang"}}}
	code, out, _ := x.run("stage", b, "--dry-run", "--json")
	doc := decode(t, out)
	if code != exitOK || doc["outcome"] != "dry_run" || len(x.s.staged) != 0 {
		t.Fatalf("%d %v staged=%v", code, doc, x.s.staged)
	}
	if doc["plan"].(map[string]any)["need_bytes"].(float64) != float64(50<<30) {
		t.Fatalf("%v", doc["plan"])
	}
}

func TestStageRefusesWhenDiskTooSmallAndDownloadsNothing(t *testing.T) {
	x := newFx(t)
	x.s.plan = profile.StagePlan{Enough: false, Shortfall: "needs 50 GiB; 10 GiB free"}
	code, _, diag := x.run("stage", b)
	if code != exitPrecondition || !strings.Contains(diag, "10 GiB free") || len(x.s.staged) != 0 {
		t.Fatalf("%d %q", code, diag)
	}
}

func TestStageLaunchedProfileNeedsOperatorToInstallUnit(t *testing.T) {
	x := newFx(t)
	code, out, _ := x.run("stage", a, "--json")
	doc := decode(t, out)
	if code != exitPrecondition || doc["outcome"] != "operator_step_required" || !slices.Equal(x.s.staged, []string{a}) {
		t.Fatalf("%d %v", code, doc)
	}
	cmds := toStrings(doc["next_commands"])
	if len(cmds) != 2 || !strings.HasPrefix(cmds[0], "sudo install ") || cmds[1] != "sudo systemctl daemon-reload" {
		t.Fatalf("%v", cmds)
	}
	unit, err := os.ReadFile(doc["unit_file"].(string))
	if err != nil || !strings.Contains(string(unit), "User=operator") {
		t.Fatalf("%v %s", err, unit)
	}
	x.installed = true
	if code, out, _ = x.run("stage", a, "--json"); code != exitOK || decode(t, out)["outcome"] != "staged" {
		t.Fatalf("%d %s", code, out)
	}
}

func TestStageFailureExitsOne(t *testing.T) {
	x := newFx(t)
	x.s.stageErr = errors.New("SHA-256 mismatch")
	if code, _, diag := x.run("stage", b); code != exitError || !strings.Contains(diag, "SHA-256 mismatch") {
		t.Fatalf("%d %q", code, diag)
	}
}

func TestDoctorReportsEveryCheckAndExitsThreeOnFailure(t *testing.T) {
	x := newFx(t)
	code, out, _ := x.run("doctor", "--json")
	doc := decode(t, out)
	if code != exitOK || doc["status"] != "ok" {
		t.Fatalf("%d %v", code, doc)
	}
	names := map[string]bool{}
	for _, c := range doc["checks"].([]any) {
		names[c.(map[string]any)["name"].(string)] = true
	}
	for _, want := range []string{"platform", "user", "docker", "nvidia-smi", "systemd", "sudo", "memory", "journal", "profiles"} {
		if !names[want] {
			t.Errorf("doctor lacks %s", want)
		}
	}
	e := x.env()
	e.goos = "darwin"
	e.output = func(context.Context, string, ...string) (string, error) { return "", errors.New("not found") }
	var o, d bytes.Buffer
	code, _ = manage(context.Background(), []string{"doctor", "--data", filepath.Join(x.dir, "d"), "--profiles", x.profiles}, &o, &d, e)
	if code != exitPrecondition || !strings.Contains(o.String(), "fix:") || !strings.Contains(o.String(), "FAIL") {
		t.Fatalf("%d\n%s", code, o.String())
	}
}

func TestDoctorFlagsRootAndUnsettledJournal(t *testing.T) {
	x := newFx(t)
	e := x.env()
	e.user = "root"
	var o, d bytes.Buffer
	if code, _ := manage(context.Background(), []string{"doctor", "--json", "--data", filepath.Join(x.dir, "d"), "--profiles", x.profiles}, &o, &d, e); code != exitPrecondition {
		t.Fatalf("root accepted: %d", code)
	}
	x = newFx(t)
	j := profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}
	_ = x.writePending(j, a, b)
	if code, out, _ := x.run("doctor"); code != exitPrecondition || !strings.Contains(out, "spark-roster recover") {
		t.Fatalf("%d %s", code, out)
	}
}

func TestVersionNeedsNoMachine(t *testing.T) {
	var out, diag bytes.Buffer
	if code, ok := runManage([]string{"version"}, &out, &diag); !ok || code != exitOK || !strings.HasPrefix(out.String(), "spark-roster ") {
		t.Fatalf("%v %d %q", ok, code, out.String())
	}
}

func TestRunDispatchesManagedCommandsAndKeepsLegacyOnes(t *testing.T) {
	var out, diag bytes.Buffer
	if code := run([]string{"version"}, &out, &diag, time.Now()); code != 0 || !strings.Contains(out.String(), "spark-roster") {
		t.Fatalf("%d %q", code, out.String())
	}
	out.Reset()
	if code := run([]string{"--help"}, &out, &diag, time.Now()); code != 0 || !strings.Contains(out.String(), "switch") || !strings.Contains(out.String(), "doctor") {
		t.Fatalf("help does not mention the new commands: %q", out.String())
	}
	out.Reset()
	if code := run([]string{"pi-q8", "--help"}, &out, &diag, time.Now()); code != 0 || !strings.Contains(out.String(), "pi-q8") {
		t.Fatalf("legacy command lost: %d %q", code, out.String())
	}
}

// The CLI must refuse on its own when the journal and the host disagree, before the
// engine's later guard and even for --dry-run, which never reaches the engine.
func TestJournalAndHostDisagreementRefusedAtTheCLIEvenForDryRun(t *testing.T) {
	x := newFx(t)
	_ = (profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}).SetActive(a)
	x.h.running[b] = true
	code, out, _ := x.run("switch", "qwen3-8-27b-dflash2", "--dry-run", "--json")
	doc := decode(t, out)
	if code != exitPrecondition || !strings.Contains(doc["error"].(string), "journal says") || !strings.Contains(doc["error"].(string), "is running") {
		t.Fatalf("%d %v", code, doc)
	}
	if code, _, _ := x.run("stop", "--dry-run"); code != exitPrecondition {
		t.Fatalf("stop accepted a journal that disagrees with the host: %d", code)
	}
}

func TestJournalActiveButNothingRunningIsRefusedWithAWayOut(t *testing.T) {
	x := newFx(t)
	_ = (profile.Journal{Path: filepath.Join(x.dir, "data", "state.json")}).SetActive(a)
	code, out, _ := x.run("switch", b, "--dry-run", "--json")
	doc := decode(t, out)
	if code != exitPrecondition || !strings.Contains(doc["error"].(string), "none of its units are running") || !strings.Contains(doc["error"].(string), "spark-roster stop") {
		t.Fatalf("%d %v", code, doc)
	}
	if len(x.h.log) != 0 {
		t.Fatalf("host touched: %v", x.h.log)
	}
	// `stop` is the way out the error message names, so it must really clear the record.
	if code, out, _ := x.run("stop", "--dry-run"); code != exitOK || !strings.Contains(out, "Would clear") || x.journalState().Active != a {
		t.Fatalf("dry run: %d %s", code, out)
	}
	if code, _, _ := x.run("stop"); code != exitPrecondition || x.journalState().Active != a {
		t.Fatalf("stop without --yes changed the record: %d", code)
	}
	code, out, _ = x.run("stop", "--yes", "--json")
	if code != exitOK || decode(t, out)["outcome"] != "cleared_stale_record" || x.journalState().Active != "" {
		t.Fatalf("%d %s", code, out)
	}
	if len(x.h.log) != 0 {
		t.Fatalf("clearing a stale record touched the host: %v", x.h.log)
	}
	// With the record gone the way is open again.
	if code, _, _ := x.run("switch", b, "--yes"); code != exitOK {
		t.Fatalf("switch after clearing: %d", code)
	}
}

// A journal edited by hand, or written by a future version, may mark a switch
// unresolved without a pending record. Status must report it, not panic.
func TestStatusSurvivesAnUnresolvedJournalWithoutAPendingSwitch(t *testing.T) {
	x := newFx(t)
	if err := os.MkdirAll(filepath.Join(x.dir, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	doc := `{"schema_version":1,"kind":"state","unresolved":"restore failed","updated_at":"2026-11-25T00:00:00Z"}`
	if err := os.WriteFile(filepath.Join(x.dir, "data", "state.json"), []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, _ := x.run("status", "--json")
	got := decode(t, out)
	if code != exitOK || !strings.Contains(got["unsettled"].(string), "restore failed") {
		t.Fatalf("%d %v", code, got)
	}
}

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/walterra/spark-roster/internal/profile"
)

// Exit codes. Scripts and agents rely on these, so they are part of the interface.
const (
	exitOK           = 0
	exitError        = 1 // the operation failed
	exitUsage        = 2 // bad arguments
	exitPrecondition = 3 // nothing was changed; something has to happen first
	exitRestored     = 4 // a switch failed and the previous profile was restored and verified
	exitUnsettled    = 5 // host state is unknown; run `spark-roster recover`
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const manageUsage = `Usage: spark-roster <command> [options]

Run and switch inference profiles on a GB10 Spark.

  list                 show the available profiles
  status               show each profile's state and which one is active
  doctor               check this machine and say how to fix what is missing
  stage PROFILE        download and verify a profile's model, image and build
  switch PROFILE       stop the running profile, start PROFILE, restore on failure
  stop                 stop the active profile
  recover              settle a switch that was interrupted
  version              print the version

Options (every command): --json for machine-readable output.
State-changing commands: --dry-run shows the plan, --yes skips the prompt.
Exit codes: 0 ok, 1 error, 2 usage, 3 precondition not met (nothing changed),
4 switch failed and the previous profile was restored, 5 host state unknown (run recover).
`

type hostOps interface {
	profile.Backend
	Units(context.Context, profile.Profile) ([]profile.UnitState, error)
	SudoReady(context.Context) error
	UnitActive(ctx context.Context, unit string) (bool, error)
	ControlUnit(ctx context.Context, action, unit string) error
	ConfirmInactive(ctx context.Context, unit string) error
}

type stager interface {
	Plan(context.Context, profile.Profile) (profile.StagePlan, error)
	Stage(context.Context, profile.Profile) error
}

type paths struct{ Home, Data, HFCache, Profiles string }

// env holds everything the commands take from the machine, so tests can supply fakes.
type env struct {
	home          string
	getenv        func(string) string
	interactive   bool
	stdin         io.Reader
	user, group   string
	goos, goarch  string
	backend       func(paths) (hostOps, stager)
	portFree      func(uint16) error
	lock          func(dir string) (func(), error)
	unitInstalled func(name, rendered string) (bool, error)
	output        func(ctx context.Context, name string, args ...string) (string, error)
	available     func(path string) (uint64, error)
	memory        func(ctx context.Context) (uint64, error)
	// Used by the benchmark commands.
	euid   int
	exe    func() (string, error)
	sha256 func(path string) (string, error)
	now    func() time.Time
	// chown hands a file the worker wrote as root to the operator it acts for.
	chown func(path string, uid, gid int) error
	// lookupUser resolves the operator named on a worker's command line.
	lookupUser func(name string) (OperatorInfo, error)
}

type app struct {
	env
	paths    paths
	profiles map[string]profile.Profile // expanded
	raw      map[string]profile.Profile // as written, for reports
	source   map[string]string
	// chownPath is set when a root worker acts for an operator.
	chownPath func(path string) error
	host      hostOps
	stager    stager
	journal   profile.Journal
	json      bool
	out, err  io.Writer
}

// manage runs the profile commands. It returns false when args[0] is not one.
func manage(ctx context.Context, args []string, out, diag io.Writer, e env) (int, bool) {
	if code, ok := manageBenchmark(ctx, args, out, diag, e); ok {
		return code, true
	}
	switch args[0] {
	case "list", "status", "doctor", "stage", "switch", "stop", "recover", "version", "--version":
	default:
		return 0, false
	}
	if args[0] == "version" || args[0] == "--version" {
		fmt.Fprintln(out, versionString())
		return exitOK, true
	}
	name := args[0]
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(diag)
	asJSON := fs.Bool("json", false, "write one JSON document to stdout")
	dry := fs.Bool("dry-run", false, "show what would happen without changing anything")
	yes := fs.Bool("yes", false, "do not ask for confirmation")
	profilesDir := fs.String("profiles", "", "directory of site profiles (default $SPARK_ROSTER_PROFILES or ~/.config/spark-roster/profiles)")
	dataDir := fs.String("data", "", "directory for weights, builds and state (default $SPARK_ROSTER_DATA or ~/.local/share/spark-roster)")
	operator := fs.String("operator", "", "act for this user (used by the root-run recovery hook)")
	flagArgs, positional := splitArgs(args[1:], "profiles", "data", "operator")
	if err := fs.Parse(flagArgs); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK, true
		}
		return exitUsage, true
	}
	wantArg := map[string]int{"stage": 1, "switch": 1}[name]
	if len(positional) != wantArg {
		fmt.Fprintf(diag, "error: %s takes %d profile argument(s), got %d\n%s", name, wantArg, len(positional), manageUsage)
		return exitUsage, true
	}
	if (*dry || *yes) && name != "stage" && name != "switch" && name != "stop" && name != "recover" {
		fmt.Fprintf(diag, "error: --dry-run and --yes do not apply to %s\n", name)
		return exitUsage, true
	}
	if *operator != "" && name != "recover" {
		fmt.Fprintln(diag, "error: --operator applies only to recover, run by the benchmark's systemd recovery hook")
		return exitUsage, true
	}
	a, code := buildAppFor(e, *asJSON, *profilesDir, *dataDir, *operator, out, diag)
	if code != exitOK {
		return code, true
	}
	switch name {
	case "list":
		return a.list(), true
	case "status":
		return a.status(ctx), true
	case "doctor":
		return a.doctor(ctx), true
	case "stage":
		return a.stage(ctx, positional[0], *dry), true
	case "switch":
		return a.switchTo(ctx, positional[0], *dry, *yes), true
	case "stop":
		return a.stop(ctx, *dry, *yes), true
	default:
		return a.recover(ctx, *dry, *yes), true
	}
}

// splitArgs lets flags appear before or after the profile id. valueFlags names
// the flags that take a separate value ("--data DIR"), so the value is not
// mistaken for a profile id.
func splitArgs(args []string, valueFlags ...string) (flags, positional []string) {
	takesValue := map[string]bool{}
	for _, f := range valueFlags {
		takesValue["--"+f], takesValue["-"+f] = true, true
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case strings.HasPrefix(a, "-") && a != "-":
			flags = append(flags, a)
			if takesValue[a] && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		default:
			positional = append(positional, a)
		}
	}
	return
}

// resolveHome picks the home directory: $HOME when it is an absolute path, otherwise
// the account database's entry. Either can be empty. That is fine for a root worker
// that acts for an operator, and an error for everything else (see buildAppFor).
func resolveHome(envHome, accountHome string) string {
	for _, h := range []string{envHome, accountHome} {
		if filepath.IsAbs(h) {
			return h
		}
	}
	return ""
}

// buildAppFor resolves the directories, loads and expands the profiles and
// connects the host. Every command that touches profiles starts here. With an
// operator it acts for that user: paths come from their home, not root's, and
// files it writes are handed to them.
func buildAppFor(e env, asJSON bool, profilesDir, dataDir, operator string, out, diag io.Writer) (*app, int) {
	var op *OperatorInfo
	if operator != "" {
		if e.lookupUser == nil {
			fmt.Fprintln(diag, "error: cannot resolve the operator on this platform")
			return nil, exitError
		}
		info, err := e.lookupUser(operator)
		if err != nil {
			fmt.Fprintf(diag, "error: operator %q: %v\n", operator, err)
			return nil, exitUsage
		}
		if info.UID == 0 || !filepath.IsAbs(info.Home) {
			fmt.Fprintf(diag, "error: operator %q must be a regular user with an absolute home directory\n", operator)
			return nil, exitUsage
		}
		op = &info
		e.home, e.user = info.Home, operator
	}
	if e.home == "" {
		fmt.Fprintln(diag, "error: cannot find a home directory: $HOME is unset and the account has none. Set $HOME, or pass --operator USER")
		return nil, exitUsage
	}
	p := paths{Home: e.home}
	p.Data = firstNonEmpty(dataDir, e.getenv("SPARK_ROSTER_DATA"), filepath.Join(e.home, ".local", "share", "spark-roster"))
	p.Profiles = firstNonEmpty(profilesDir, e.getenv("SPARK_ROSTER_PROFILES"), filepath.Join(e.home, ".config", "spark-roster", "profiles"))
	p.HFCache = firstNonEmpty(e.getenv("HF_HUB_CACHE"), joinIf(e.getenv("HF_HOME"), "hub"), filepath.Join(e.home, ".cache", "huggingface", "hub"))
	a := &app{env: e, paths: p, json: asJSON, out: out, err: diag, journal: profile.Journal{Path: filepath.Join(p.Data, "state.json")}}
	if op != nil && e.chown != nil {
		a.chownPath = func(path string) error { return e.chown(path, op.UID, op.GID) }
		a.journal.Chown = a.chownPath
	}
	if code := a.load(); code != exitOK {
		return nil, code
	}
	a.host, a.stager = e.backend(p)
	return a, exitOK
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

func joinIf(base, elem string) string {
	if base == "" {
		return ""
	}
	return filepath.Join(base, elem)
}

func versionString() string {
	v := version
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 12 {
				v += " (" + s.Value[:12] + ")"
			}
		}
	}
	return "spark-roster " + v + " " + runtime.GOOS + "/" + runtime.GOARCH
}

func (a *app) load() int {
	set, err := profile.Load(a.paths.Profiles)
	if err != nil {
		fmt.Fprintf(a.err, "error: %v\n", err)
		return exitError
	}
	builtin, _ := profile.Builtin()
	vars := profile.Vars{Home: a.paths.Home, Data: a.paths.Data, HFCache: a.paths.HFCache}
	a.profiles, a.raw, a.source = map[string]profile.Profile{}, map[string]profile.Profile{}, map[string]string{}
	for id, p := range set {
		q, err := profile.Expand(p, vars)
		if err == nil {
			err = q.Validate()
		}
		if err != nil {
			fmt.Fprintf(a.err, "error: profile %s: %v\n", id, err)
			return exitError
		}
		a.profiles[id], a.raw[id] = q, p
		a.source[id] = "site"
		if _, ok := builtin[id]; ok {
			a.source[id] = "built-in"
		}
	}
	return exitOK
}

func (a *app) emit(v any) int {
	if a.json {
		enc := json.NewEncoder(a.out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(v); err != nil {
			fmt.Fprintf(a.err, "error: %v\n", err)
			return exitError
		}
	}
	return exitOK
}

func (a *app) ids() []string { return profile.IDs(a.profiles) }

func (a *app) lookup(id string) (profile.Profile, int) {
	p, ok := a.profiles[id]
	if !ok {
		fmt.Fprintf(a.err, "error: unknown profile %q. Available: %s\n", id, strings.Join(a.ids(), ", "))
		return profile.Profile{}, exitUsage
	}
	return p, exitOK
}

func gibStr(b int64) string { return fmt.Sprintf("%.0f GiB", float64(b)/(1<<30)) }

type listRow struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Mode        string `json:"mode"` // launch | adopt
	Source      string `json:"source"`
	Port        uint16 `json:"port"`
	Model       string `json:"model"`
	ContextTok  int    `json:"context_tokens,omitempty"`
	MemoryFloor int64  `json:"memory_floor_bytes"`
	Evidence    string `json:"memory_evidence"`
}

func (a *app) list() int {
	var rows []listRow
	for _, id := range a.ids() {
		p := a.profiles[id]
		mode := "launch"
		if p.Adopt != nil {
			mode = "adopt"
		}
		rows = append(rows, listRow{id, p.Title, mode, a.source[id], p.Serving.Port, p.Serving.Model, p.ContextTokens, p.Memory.StartMinAvailableBytes, p.Memory.Evidence})
	}
	if a.json {
		return a.emit(map[string]any{"schema_version": 1, "kind": "profile_list", "profiles": rows})
	}
	tw := tabwriter.NewWriter(a.out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tMODE\tPORT\tCONTEXT\tMEMORY FLOOR\tSOURCE\tTITLE")
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s (%s)\t%s\t%s\n", r.ID, r.Mode, r.Port, r.ContextTok, gibStr(r.MemoryFloor), r.Evidence, r.Source, r.Title)
	}
	tw.Flush()
	return exitOK
}

type statusRow struct {
	ID     string              `json:"id"`
	State  string              `json:"state"` // running | stopped | partial | missing
	Staged bool                `json:"staged"`
	Detail string              `json:"detail,omitempty"`
	Units  []profile.UnitState `json:"units"`
}

type statusDoc struct {
	SchemaVersion  int         `json:"schema_version"`
	Kind           string      `json:"kind"`
	Active         string      `json:"active,omitempty"`
	Running        []string    `json:"running"`
	Unsettled      string      `json:"unsettled,omitempty"`
	Benchmark      string      `json:"benchmark,omitempty"`
	AvailableBytes uint64      `json:"available_bytes,omitempty"`
	Profiles       []statusRow `json:"profiles"`
	Next           []string    `json:"next,omitempty"`
}

func (a *app) observe(ctx context.Context) (statusDoc, error) {
	st, err := a.journal.Read()
	if err != nil {
		return statusDoc{}, err
	}
	doc := statusDoc{SchemaVersion: 1, Kind: "status", Active: st.Active, Running: []string{}}
	if st.Bench != nil {
		doc.Benchmark = fmt.Sprintf("benchmark %s started %s; baseline %s", st.Bench.ID, st.Bench.StartedAt.Format(time.RFC3339), orNone(st.Bench.Baseline))
	}
	if st.Switching != nil || st.Unresolved != "" {
		doc.Unsettled = "an earlier switch was not settled"
		if st.Switching != nil {
			doc.Unsettled = fmt.Sprintf("a switch from %q to %q was not settled", st.Switching.From, st.Switching.To)
		}
		if st.Unresolved != "" {
			doc.Unsettled += ": " + st.Unresolved
		}
		doc.Next = append(doc.Next, "spark-roster recover")
	}
	if m, err := a.host.Available(ctx); err == nil {
		doc.AvailableBytes = m
	}
	for _, id := range a.ids() {
		p := a.profiles[id]
		units, err := a.host.Units(ctx, p)
		if err != nil {
			return statusDoc{}, fmt.Errorf("inspect %s: %w", id, err)
		}
		row := statusRow{ID: id, State: profile.StateOf(units), Units: units}
		if err := a.host.Staged(ctx, p); err == nil {
			row.Staged = true
		} else {
			row.Detail = err.Error()
		}
		if row.State == "running" || row.State == "partial" {
			doc.Running = append(doc.Running, id)
		}
		doc.Profiles = append(doc.Profiles, row)
	}
	return doc, nil
}

func (a *app) status(ctx context.Context) int {
	doc, err := a.observe(ctx)
	if err != nil {
		fmt.Fprintf(a.err, "error: %v\n", err)
		return exitError
	}
	if doc.Unsettled == "" && len(doc.Running) == 0 {
		for _, r := range doc.Profiles {
			if r.Staged {
				doc.Next = append(doc.Next, "spark-roster switch "+r.ID)
				break
			}
		}
		if len(doc.Next) == 0 && len(doc.Profiles) > 0 {
			doc.Next = append(doc.Next, "spark-roster stage "+doc.Profiles[0].ID)
		}
	}
	if a.json {
		return a.emit(doc)
	}
	tw := tabwriter.NewWriter(a.out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "PROFILE\tSTATE\tSTAGED\tACTIVE")
	for _, r := range doc.Profiles {
		mark := ""
		if r.ID == doc.Active {
			mark = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\t%t\t%s\n", r.ID, r.State, r.Staged, mark)
	}
	tw.Flush()
	if doc.AvailableBytes > 0 {
		fmt.Fprintf(a.out, "\nHost memory available: %.1f GiB\n", float64(doc.AvailableBytes)/(1<<30))
	}
	if doc.Benchmark != "" {
		fmt.Fprintf(a.out, "\nbenchmark: %s\n", doc.Benchmark)
	}
	if doc.Unsettled != "" {
		fmt.Fprintf(a.out, "\nwarning: %s\n", doc.Unsettled)
	}
	for _, n := range doc.Next {
		fmt.Fprintf(a.out, "next: %s\n", n)
	}
	return exitOK
}

// running returns the one profile that is running, nil when none, or an error
// when the host and the journal disagree or several run at once.
func (a *app) running(ctx context.Context) (*profile.Profile, error) {
	st, err := a.journal.Read()
	if err != nil {
		return nil, err
	}
	p, _, err := profile.Current(ctx, a.host, st, a.profiles)
	return p, err
}

type progress struct {
	a      *app
	events []profile.Event
	moved  bool // the host was changed
}

func (p *progress) emit(e profile.Event) {
	p.events = append(p.events, e)
	if e.Phase == "stopping" || e.Phase == "starting" {
		p.moved = true
	}
	if p.a.json {
		b, _ := json.Marshal(e)
		fmt.Fprintln(p.a.err, string(b))
		return
	}
	line := "  " + strings.ReplaceAll(e.Phase, "_", " ")
	if e.Profile != "" {
		line += " " + e.Profile
	}
	if e.Available != nil {
		line += fmt.Sprintf(" (%.1f GiB available)", float64(*e.Available)/(1<<30))
	}
	if e.Error != "" {
		line += ": " + e.Error
	}
	fmt.Fprintln(p.a.err, line)
}

type opDoc struct {
	SchemaVersion int             `json:"schema_version"`
	Kind          string          `json:"kind"`
	From          string          `json:"from,omitempty"`
	To            string          `json:"to,omitempty"`
	Outcome       string          `json:"outcome"`
	DryRun        bool            `json:"dry_run,omitempty"`
	Error         string          `json:"error,omitempty"`
	ExitCode      int             `json:"exit_code"`
	Steps         []profile.Step  `json:"steps,omitempty"`
	Events        []profile.Event `json:"events,omitempty"`
	Next          []string        `json:"next,omitempty"`
}

func (a *app) finish(d opDoc, code int) int {
	d.SchemaVersion, d.ExitCode = 1, code
	if a.json {
		a.emit(d)
		return code
	}
	if d.Error != "" {
		fmt.Fprintf(a.err, "error: %s\n", d.Error)
	}
	for _, n := range d.Next {
		fmt.Fprintf(a.err, "next: %s\n", n)
	}
	return code
}

// confirm asks before downtime. Without a terminal and without --yes it refuses, so a script never
// stops a service by accident.
func (a *app) confirm(question string, yes bool) bool {
	if yes {
		return true
	}
	if !a.interactive {
		return false
	}
	fmt.Fprintf(a.err, "%s [y/N] ", question)
	line, _ := bufio.NewReader(a.stdin).ReadString('\n')
	return strings.EqualFold(strings.TrimSpace(line), "y") || strings.EqualFold(strings.TrimSpace(line), "yes")
}

func (a *app) options(from *profile.Profile, to profile.Profile) profile.Options {
	// Poll and probe timing follow the existing benchmark switch code: it polls every 4 s and
	// gives each generation probe 8 s, then keeps trying until the profile's ready_timeout.
	recovery := 5 * time.Minute
	if from != nil {
		recovery += from.Serving.ReadyTimeout.Duration
	}
	return profile.Options{ReserveBytes: 10 << 30, Poll: 4 * time.Second, ProbeTimeout: 8 * time.Second, RecoveryTimeout: recovery, Resume: a.resumeUnits}
}

// resumeUnits starts units a benchmark paused, after the baseline serves again.
func (a *app) resumeUnits(ctx context.Context, units []string) error {
	var errs []error
	for _, u := range units {
		errs = append(errs, a.host.ControlUnit(ctx, "start", u))
	}
	return errors.Join(errs...)
}

func (a *app) switchTo(ctx context.Context, id string, dry, yes bool) int {
	to, code := a.lookup(id)
	if code != exitOK {
		return code
	}
	doc := opDoc{Kind: "switch", To: id, DryRun: dry}
	from, err := a.running(ctx)
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	if from != nil {
		doc.From = from.ID
		if from.ID == to.ID {
			doc.Outcome = "already_active"
			if !a.json {
				fmt.Fprintf(a.out, "%s is already running\n", id)
			}
			return a.finish(doc, exitOK)
		}
	}
	// Read-only preflight: everything that can be checked without stopping anything.
	var problems []string
	if err := a.host.Staged(ctx, to); err != nil {
		problems = append(problems, fmt.Sprintf("%s is not staged: %v. Run `spark-roster stage %s`", id, err, id))
	}
	if from == nil || from.Serving.Port != to.Serving.Port {
		if err := a.portFree(to.Serving.Port); err != nil {
			problems = append(problems, fmt.Sprintf("port %d is in use by something that is not %s: %v", to.Serving.Port, id, err))
		}
	}
	if !dry {
		if err := a.host.SudoReady(ctx); err != nil {
			problems = append(problems, err.Error())
		}
	}
	doc.Steps = profile.PlanSwitch(from, to)
	if len(problems) > 0 {
		doc.Outcome, doc.Error = "refused", strings.Join(problems, "; ")
		return a.finish(doc, exitPrecondition)
	}
	if dry {
		doc.Outcome = "dry_run"
		if !a.json {
			fmt.Fprintf(a.out, "Plan to switch to %s", id)
			if from != nil {
				fmt.Fprintf(a.out, " from %s (stopped for the duration)", from.ID)
			}
			fmt.Fprintln(a.out, ":")
			for i, s := range doc.Steps {
				fmt.Fprintf(a.out, "  %d. %s %s\n", i+1, s.Action, s.Profile)
			}
			fmt.Fprintf(a.out, "Host memory must stay above a %s reserve; %s needs %s available once the source is stopped.\n", gibStr(10<<30), id, gibStr(to.Memory.StartMinAvailableBytes))
			fmt.Fprintln(a.out, "Nothing was changed.")
		}
		return a.finish(doc, exitOK)
	}
	question := fmt.Sprintf("Start %s", id)
	if from != nil {
		question = fmt.Sprintf("Stop %s (it will be unavailable) and start %s", from.ID, id)
	}
	if !a.confirm(question+"?", yes) {
		doc.Outcome = "needs_confirmation"
		doc.Error = "this changes running services. Re-run with --yes, or with --dry-run to see the plan"
		return a.finish(doc, exitPrecondition)
	}
	unlock, err := a.lock(a.paths.Data)
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	defer unlock()
	prog := &progress{a: a}
	deadline := to.Serving.ReadyTimeout.Duration + 10*time.Minute
	run, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	err = profile.Journaled(run, a.host, a.journal, from, to, a.options(from, to), prog.emit)
	doc.Events = prog.events
	switch {
	case err == nil:
		doc.Outcome = "ready"
		if !a.json {
			fmt.Fprintf(a.out, "%s is serving on port %d (model %s)\n", id, to.Serving.Port, to.Serving.Model)
		}
		return a.finish(doc, exitOK)
	case errors.Is(err, profile.ErrRestoreFailed) || errors.Is(err, profile.ErrExitUnconfirmed):
		doc.Outcome, doc.Error = "unsettled", err.Error()
		doc.Next = []string{"spark-roster recover"}
		return a.finish(doc, exitUnsettled)
	case !prog.moved:
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	default:
		doc.Outcome, doc.Error = "failed_restored", err.Error()
		if from != nil {
			doc.Next = []string{"spark-roster status"}
		}
		return a.finish(doc, exitRestored)
	}
}

func (a *app) stop(ctx context.Context, dry, yes bool) int {
	doc := opDoc{Kind: "stop", DryRun: dry}
	from, err := a.running(ctx)
	if err != nil && a.staleRecord(ctx) {
		// The journal names a profile that is not running. Stopping nothing is
		// harmless, and clearing the record is the remedy the error message gives.
		return a.clearStale(ctx, doc, dry, yes)
	}
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	if from == nil {
		doc.Outcome = "nothing_running"
		if !a.json {
			fmt.Fprintln(a.out, "no profile is running")
		}
		return a.finish(doc, exitOK)
	}
	doc.From = from.ID
	doc.Steps = []profile.Step{{Action: "stop and confirm exit", Profile: from.ID, Units: from.Units()}}
	if dry {
		doc.Outcome = "dry_run"
		if !a.json {
			fmt.Fprintf(a.out, "Would stop %s (units: %s). Nothing was changed.\n", from.ID, strings.Join(from.Units(), ", "))
		}
		return a.finish(doc, exitOK)
	}
	if err := a.host.SudoReady(ctx); err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	if !a.confirm(fmt.Sprintf("Stop %s?", from.ID), yes) {
		doc.Outcome, doc.Error = "needs_confirmation", "this stops a running service. Re-run with --yes"
		return a.finish(doc, exitPrecondition)
	}
	unlock, err := a.lock(a.paths.Data)
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	defer unlock()
	st, err := a.journal.Read()
	if err == nil && (st.Switching != nil || st.Unresolved != "") {
		err = errors.New("an earlier switch is unsettled; run recover first")
	}
	if err == nil && st.Bench != nil {
		err = fmt.Errorf("a benchmark is running or was interrupted (%s); wait for it, or run recover if it died", st.Describe())
	}
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	stop, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := a.host.Stop(stop, *from); err != nil {
		doc.Outcome, doc.Error = "failed", err.Error()
		return a.finish(doc, exitError)
	}
	if err := a.journal.SetActive(""); err != nil {
		doc.Outcome, doc.Error = "failed", err.Error()
		return a.finish(doc, exitError)
	}
	doc.Outcome = "stopped"
	if !a.json {
		fmt.Fprintf(a.out, "%s stopped and its exit confirmed\n", from.ID)
	}
	return a.finish(doc, exitOK)
}

// staleRecord reports a journal that names an active profile when no profile is
// running and no switch is pending.
func (a *app) staleRecord(ctx context.Context) bool {
	doc, err := a.observe(ctx)
	if err != nil || doc.Unsettled != "" {
		return false
	}
	return doc.Active != "" && len(doc.Running) == 0
}

func (a *app) clearStale(ctx context.Context, doc opDoc, dry, yes bool) int {
	st, _ := a.journal.Read()
	doc.From = st.Active
	if dry {
		doc.Outcome = "dry_run"
		if !a.json {
			fmt.Fprintf(a.out, "The journal names %s as active but nothing is running. Would clear that record. Nothing was changed.\n", st.Active)
		}
		return a.finish(doc, exitOK)
	}
	if !a.confirm(fmt.Sprintf("Clear the stale record naming %s as active?", st.Active), yes) {
		doc.Outcome, doc.Error = "needs_confirmation", "re-run with --yes to clear the record"
		return a.finish(doc, exitPrecondition)
	}
	unlock, err := a.lock(a.paths.Data)
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	defer unlock()
	if err := a.journal.SetActive(""); err != nil {
		doc.Outcome, doc.Error = "failed", err.Error()
		return a.finish(doc, exitError)
	}
	doc.Outcome = "cleared_stale_record"
	if !a.json {
		fmt.Fprintf(a.out, "cleared the record naming %s as active; nothing was running\n", st.Active)
	}
	return a.finish(doc, exitOK)
}

func (a *app) recover(ctx context.Context, dry, yes bool) int {
	doc := opDoc{Kind: "recover", DryRun: dry}
	st, err := a.journal.Read()
	if err != nil {
		doc.Outcome, doc.Error = "failed", err.Error()
		return a.finish(doc, exitError)
	}
	if st.Switching == nil && st.Bench == nil {
		doc.Outcome = "nothing_to_recover"
		if !a.json {
			fmt.Fprintln(a.out, "no interrupted switch or benchmark to recover")
		}
		return a.finish(doc, exitOK)
	}
	want := ""
	switch {
	case st.Bench != nil:
		want = st.Bench.Baseline
		doc.From, doc.To = st.Describe(), want
	default:
		want = st.Switching.From
		doc.From, doc.To = st.Switching.From, st.Switching.To
	}
	if dry {
		doc.Outcome = "dry_run"
		if !a.json {
			fmt.Fprintf(a.out, "Would stop every profile the interrupted work left running, confirm each exited, then restore %s and wait for a generation", orNone(want))
			if st.Bench != nil && len(st.Bench.Paused) > 0 {
				fmt.Fprintf(a.out, ", then start %s again", strings.Join(st.Bench.Paused, ", "))
			}
			fmt.Fprintln(a.out, ". Nothing was changed.")
		}
		return a.finish(doc, exitOK)
	}
	if err := a.host.SudoReady(ctx); err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	if !a.confirm(fmt.Sprintf("Stop what is running and restore %s?", orNone(want)), yes) {
		doc.Outcome, doc.Error = "needs_confirmation", "recovery stops and starts services. Re-run with --yes"
		return a.finish(doc, exitPrecondition)
	}
	unlock, err := a.lock(a.paths.Data)
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return a.finish(doc, exitPrecondition)
	}
	defer unlock()
	prog := &progress{a: a}
	o := a.options(nil, profile.Profile{})
	if p, ok := a.profiles[want]; ok {
		o = a.options(&p, p)
	}
	run, cancel := context.WithTimeout(ctx, o.RecoveryTimeout+5*time.Minute)
	defer cancel()
	if err := profile.Recover(run, a.host, a.journal, a.profiles, o, prog.emit); err != nil {
		doc.Outcome, doc.Error, doc.Events = "unsettled", err.Error(), prog.events
		return a.finish(doc, exitUnsettled)
	}
	doc.Outcome, doc.Events = "recovered", prog.events
	if !a.json {
		fmt.Fprintf(a.out, "recovered: %s is serving\n", orNone(want))
	}
	return a.finish(doc, exitOK)
}

func orNone(s string) string {
	if s == "" {
		return "(nothing)"
	}
	return s
}

type stageDoc struct {
	SchemaVersion int                `json:"schema_version"`
	Kind          string             `json:"kind"`
	Profile       string             `json:"profile"`
	Outcome       string             `json:"outcome"` // staged | operator_step_required | refused | failed | dry_run
	Plan          *profile.StagePlan `json:"plan,omitempty"`
	UnitFile      string             `json:"unit_file,omitempty"`
	Commands      []string           `json:"next_commands,omitempty"`
	Error         string             `json:"error,omitempty"`
}

func (a *app) stage(ctx context.Context, id string, dry bool) int {
	p, code := a.lookup(id)
	if code != exitOK {
		return code
	}
	doc := stageDoc{SchemaVersion: 1, Kind: "stage", Profile: id}
	report := func(code int) int {
		if a.json {
			a.emit(doc)
		} else if doc.Error != "" {
			fmt.Fprintf(a.err, "error: %s\n", doc.Error)
		}
		return code
	}
	plan, err := a.stager.Plan(ctx, p)
	if err != nil {
		doc.Outcome, doc.Error = "failed", err.Error()
		return report(exitError)
	}
	doc.Plan = &plan
	if !a.json {
		fmt.Fprintf(a.out, "Staging %s: %.1f GiB to download, %.1f GiB of images to pull\n", id, float64(plan.NeedBytes)/(1<<30), float64(plan.ImageBytes)/(1<<30))
		for _, ap := range plan.Artifacts {
			state := "missing"
			if ap.Present {
				state = "present"
			}
			fmt.Fprintf(a.out, "  %-12s %-9s %s\n", ap.Kind, state, ap.Name)
		}
	}
	if !plan.Enough {
		doc.Outcome, doc.Error = "refused", plan.Shortfall
		return report(exitPrecondition)
	}
	if dry {
		doc.Outcome = "dry_run"
		if !a.json {
			fmt.Fprintln(a.out, "Nothing was changed.")
		}
		return report(exitOK)
	}
	unlock, err := a.lock(a.paths.Data)
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return report(exitPrecondition)
	}
	defer unlock()
	if err := a.stager.Stage(ctx, p); err != nil {
		doc.Outcome, doc.Error = "failed", err.Error()
		return report(exitError)
	}
	if p.Launch != nil {
		rendered, err := profile.RenderUnit(p, a.user, a.group)
		if err != nil {
			doc.Outcome, doc.Error = "failed", err.Error()
			return report(exitError)
		}
		installed, err := a.unitInstalled(profile.UnitName(id), rendered)
		if err != nil {
			doc.Outcome, doc.Error = "failed", err.Error()
			return report(exitError)
		}
		if !installed {
			path, cmds, err := profile.InstallUnit(p, a.user, a.group, filepath.Join(a.paths.Data, "units"))
			if err != nil {
				doc.Outcome, doc.Error = "failed", err.Error()
				return report(exitError)
			}
			doc.Outcome, doc.UnitFile, doc.Commands = "operator_step_required", path, cmds
			if !a.json {
				fmt.Fprintf(a.out, "Artifacts are staged. Install the unit (review %s first):\n", path)
				for _, c := range cmds {
					fmt.Fprintf(a.out, "  %s\n", c)
				}
				fmt.Fprintf(a.out, "Then run: spark-roster switch %s\n", id)
			}
			return report(exitPrecondition)
		}
	}
	doc.Outcome = "staged"
	if !a.json {
		fmt.Fprintf(a.out, "%s is staged and verified. Next: spark-roster switch %s\n", id, id)
	}
	return report(exitOK)
}

type check struct {
	Name   string `json:"name"`
	Status string `json:"status"` // ok | warn | fail
	Detail string `json:"detail"`
	Fix    string `json:"fix,omitempty"`
}

func (a *app) doctor(ctx context.Context) int {
	var checks []check
	add := func(name, status, detail, fix string) { checks = append(checks, check{name, status, detail, fix}) }
	if a.goos == "linux" && a.goarch == "arm64" {
		add("platform", "ok", "linux/arm64", "")
	} else {
		add("platform", "fail", a.goos+"/"+a.goarch, "SparkRoster runs on the GB10 itself (Linux ARM64). Use `list` and `plan` elsewhere.")
	}
	if a.user == "root" || a.user == "" {
		add("user", "fail", "running as root", "Run as your normal user. SparkRoster calls sudo only for systemctl start and stop.")
	} else {
		add("user", "ok", a.user, "")
	}
	for _, c := range []struct {
		name, cmd string
		args      []string
		fix       string
	}{
		{"docker", "docker", []string{"info", "--format", "{{.ServerVersion}}"}, "Install Docker and add your user to the docker group: sudo usermod -aG docker $USER, then log in again."},
		{"nvidia-smi", "nvidia-smi", []string{"--query-gpu=name", "--format=csv,noheader"}, "The NVIDIA driver is missing or not loaded."},
		{"systemd", "systemctl", []string{"--version"}, "SparkRoster manages services through systemd."},
	} {
		if out, err := a.output(ctx, c.cmd, c.args...); err != nil {
			add(c.name, "fail", err.Error(), c.fix)
		} else {
			add(c.name, "ok", firstLine(out), "")
		}
	}
	if err := a.host.SudoReady(ctx); err != nil {
		add("sudo", "warn", err.Error(), "Run `sudo -v` in this terminal before switching.")
	} else {
		add("sudo", "ok", "passwordless sudo available for this session", "")
	}
	if m, err := a.memory(ctx); err != nil {
		add("memory", "warn", err.Error(), "")
	} else {
		add("memory", "ok", fmt.Sprintf("%.1f GiB available", float64(m)/(1<<30)), "")
	}
	for _, d := range []struct{ name, path string }{{"disk (data)", a.paths.Data}, {"disk (model cache)", a.paths.HFCache}} {
		free, err := a.available(existingDir(d.path))
		switch {
		case err != nil:
			add(d.name, "warn", err.Error(), "")
		case free < 100<<30:
			add(d.name, "warn", fmt.Sprintf("%.0f GiB free at %s", float64(free)/(1<<30), d.path), "The built-in profiles need roughly 20 to 60 GiB each plus images.")
		default:
			add(d.name, "ok", fmt.Sprintf("%.0f GiB free at %s", float64(free)/(1<<30), d.path), "")
		}
	}
	if st, err := a.journal.Read(); err != nil {
		add("journal", "fail", err.Error(), "Inspect or move "+a.journal.Path)
	} else if st.Switching != nil || st.Unresolved != "" {
		add("journal", "fail", "an earlier switch was not settled", "Run `spark-roster recover`.")
	} else {
		add("journal", "ok", "no unsettled switch", "")
	}
	add("profiles", "ok", fmt.Sprintf("%d profiles (%s)", len(a.profiles), strings.Join(a.ids(), ", ")), "")
	worst := "ok"
	for _, c := range checks {
		if c.Status == "fail" {
			worst = "fail"
		} else if c.Status == "warn" && worst == "ok" {
			worst = "warn"
		}
	}
	if a.json {
		a.emit(map[string]any{"schema_version": 1, "kind": "doctor", "status": worst, "checks": checks})
	} else {
		for _, c := range checks {
			fmt.Fprintf(a.out, "%-5s %-20s %s\n", strings.ToUpper(c.Status), c.Name, c.Detail)
			if c.Fix != "" && c.Status != "ok" {
				fmt.Fprintf(a.out, "      fix: %s\n", c.Fix)
			}
		}
	}
	if worst == "fail" {
		return exitPrecondition
	}
	return exitOK
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func existingDir(p string) string {
	for {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		parent := filepath.Dir(p)
		if parent == p {
			return p
		}
		p = parent
	}
}

var _ = sort.Strings

// offlineEnv serves `list` where there is no host to inspect.
func offlineEnv() env {
	home := resolveHome(os.Getenv("HOME"), "")
	return env{home: home, getenv: os.Getenv, stdin: os.Stdin, goos: runtime.GOOS, goarch: runtime.GOARCH,
		backend: func(paths) (hostOps, stager) { return nil, nil }}
}

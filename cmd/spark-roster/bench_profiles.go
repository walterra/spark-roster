package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/walterra/spark-roster/internal/profile"
)

// Seams for tests. The defaults are the real measurements.
var (
	runTask     = runPiTaskScored
	runnerImage = benchRunnerImage
)

// OperatorInfo identifies the unprivileged user a root-run worker acts for.
type OperatorInfo struct {
	Home     string
	UID, GID int
}

// Budget is the watchdog ceiling of a benchmark run and where it comes from.
// Every part is a limit the code itself enforces, not a forecast of runtime.
type Budget struct {
	Attempts   time.Duration `json:"attempts"` // tasks x repetitions x per-attempt limit, per profile
	Speed      time.Duration `json:"speed"`    // six streaming probes per profile
	Switches   time.Duration `json:"switches"` // each profile that has to be started: its ready_timeout plus slack
	Restore    time.Duration `json:"restore"`  // bringing the baseline back
	Margin     time.Duration `json:"margin"`
	Total      time.Duration `json:"total"`
	TasksEach  int           `json:"tasks_per_profile"`
	Candidates int           `json:"profiles_started"`
}

const (
	benchSwitchSlack  = 5 * time.Minute  // stop, confirm exit and the probe loop's last poll
	benchSpeedProbes  = 6                // three prompt sizes, cold and warm
	benchProbeLimit   = 2 * time.Minute  // benchCompletion's own timeout
	benchBudgetMargin = 15 * time.Minute // cleanup, telemetry flush, report writes
)

// benchBudget derives the ceiling from the profiles' own limits. A run that
// reaches it has stalled; the ceiling is not a runtime estimate.
func benchBudget(set map[string]profile.Profile, order []string, baseline string, gate bool) Budget {
	tasks := 8
	if _, all, err := benchInputs(); err == nil {
		tasks = len(all)
	}
	reps := benchRepeats
	if gate {
		tasks, reps = 1, 1
	}
	b := Budget{TasksEach: tasks * reps, Margin: benchBudgetMargin}
	cur := baseline
	for _, id := range order {
		p := set[id]
		b.Attempts += time.Duration(tasks*reps) * taskLimit
		b.Speed += benchSpeedProbes * benchProbeLimit
		if id != cur {
			b.Switches += p.Serving.ReadyTimeout.Duration + benchSwitchSlack
			b.Candidates++
			cur = id
		}
	}
	b.Restore = benchSwitchSlack
	if bp, ok := set[baseline]; ok {
		b.Restore = bp.Serving.ReadyTimeout.Duration + benchSwitchSlack
	}
	b.Total = b.Attempts + b.Speed + b.Switches + b.Restore + b.Margin
	return b
}

// systemdCeiling is how long the transient unit may live. The worker's own
// deadline is Total; its restoration then runs on a fresh budget, so the unit
// must outlive both, or systemd would kill the restore half way.
func (b Budget) systemdCeiling() time.Duration { return b.Total + b.Restore + 2*benchSwitchSlack }

// stopBudget is how long systemd waits for the ExecStopPost recovery hook.
func (b Budget) stopBudget() time.Duration { return b.Restore + benchSwitchSlack }

// benchPreflight lists everything that would stop a run, without changing
// anything. An empty result means the run can start.
func (a *app) benchPreflight(ctx context.Context, order []string, baseline string) []string {
	if len(order) == 0 {
		return []string{"name at least one profile to benchmark"}
	}
	var problems []string
	seen := map[string]bool{}
	for _, id := range order {
		if _, ok := a.profiles[id]; !ok {
			problems = append(problems, fmt.Sprintf("unknown profile %q. Available: %s", id, strings.Join(a.ids(), ", ")))
		}
		if seen[id] {
			problems = append(problems, fmt.Sprintf("profile %q is listed twice", id))
		}
		seen[id] = true
	}
	if len(problems) > 0 {
		return problems
	}
	st, err := a.journal.Read()
	if err != nil {
		return []string{err.Error()}
	}
	if st.Unsettled() {
		return []string{fmt.Sprintf("an earlier switch or benchmark is unsettled (%s). Run `spark-roster recover`", st.Describe())}
	}
	cur, _, err := profile.Current(ctx, a.host, st, a.profiles)
	if err != nil {
		return []string{err.Error()}
	}
	have := ""
	if cur != nil {
		have = cur.ID
	}
	if have != baseline {
		return []string{fmt.Sprintf("the profile serving now is %s, not %s. A benchmark restores the profile that was serving when it started", orNone(have), orNone(baseline))}
	}
	for _, id := range order {
		p := a.profiles[id]
		if err := a.host.Staged(ctx, p); err != nil {
			problems = append(problems, fmt.Sprintf("%s is not staged: %v. Run `spark-roster stage %s`", id, err, id))
		}
		if id != baseline {
			if err := a.portFree(p.Serving.Port); err != nil {
				problems = append(problems, fmt.Sprintf("port %d (%s) is in use by something that is not %s: %v", p.Serving.Port, id, id, err))
			}
		}
	}
	if _, err := runnerImage(ctx); err != nil {
		problems = append(problems, err.Error())
	}
	return problems
}

// pausedUnits returns the units that are active now and that a profile in the
// run asks to have stopped while requests are timed.
func (a *app) pausedUnits(ctx context.Context, order []string, baseline string) ([]string, error) {
	var want []string
	add := func(p profile.Profile) {
		for _, u := range p.PauseDuringBenchmark {
			if !containsStr(want, u) {
				want = append(want, u)
			}
		}
	}
	for _, id := range order {
		add(a.profiles[id])
	}
	if bp, ok := a.profiles[baseline]; ok {
		add(bp)
	}
	var active []string
	for _, u := range want {
		ok, err := a.host.UnitActive(ctx, u)
		if err != nil {
			return nil, fmt.Errorf("inspect %s: %w", u, err)
		}
		if ok {
			active = append(active, u)
		}
	}
	return active, nil
}

func containsStr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

type benchWorkerOpts struct {
	Order    []string
	Baseline string
	Gate     bool
	Operator string
}

// benchWorker runs the scored comparison. It owns the host until it returns:
// it pauses what must not compete for the GPU, switches through the profiles
// in order, then always settles back to the baseline and resumes what it
// paused, whether the run finished, failed or was cancelled.
func (a *app) benchWorker(ctx context.Context, w benchWorkerOpts, out io.Writer) (rep benchReport, runErr error) {
	if problems := a.benchPreflight(ctx, w.Order, w.Baseline); len(problems) > 0 {
		return rep, errors.New(strings.Join(problems, "; "))
	}
	imageID, err := runnerImage(ctx)
	if err != nil {
		return rep, err
	}
	suite, tasks, err := benchInputs()
	if err != nil {
		return rep, err
	}
	mode, reps := "benchmark", benchRepeats
	if w.Gate {
		mode, reps, tasks = "gate", 1, tasks[:1]
	}
	started := a.now().UTC()
	id := started.Format("20060102T150405Z")
	defs := map[string]profile.Profile{}
	for _, pid := range w.Order {
		defs[pid] = a.raw[pid]
	}
	if bp, ok := a.raw[w.Baseline]; ok {
		defs[w.Baseline] = bp
	}
	rep = benchReport{
		Schema: 3, Started: started, Operator: w.Operator, Run: "running", Restoration: "pending",
		PiImage: imageID, Suite: suite, TaskCount: len(tasks), TaskLimitSec: int(taskLimit / time.Second), Repetitions: reps, Mode: mode,
		PiContext: scoredWindow, PiOutputCap: scoredOutput, Baseline: w.Baseline, Tool: versionString(),
		Host: map[string]string{"platform": a.goos + "/" + a.goarch}, Defs: defs,
	}
	save := func() error { return a.writeBenchFile(id, rep) }
	paused, err := a.pausedUnits(ctx, w.Order, w.Baseline)
	if err != nil {
		return rep, err
	}
	rep.Paused = paused
	budget := benchBudget(a.profiles, w.Order, w.Baseline, w.Gate)
	tctx, flush, _ := startBenchTelemetryIn(ctx, filepath.Join(a.paths.Data, "benchmarks", "events"), mode, started, out, false)
	defer flush()
	benchEventAt(tctx, "run.start", map[string]any{"mode": mode, "profiles": strings.Join(w.Order, ","), "baseline": w.Baseline, "work_timeout_ms": budget.Total.Milliseconds(), "pi_context": scoredWindow, "pi_output_cap": scoredOutput, "pi_image": imageID, "suite": suite})
	defer func() {
		benchEventAt(tctx, "run.end", map[string]any{"outcome": rep.Run, "error_kind": benchErrorKind(runErr)})
	}()
	// The report and the journal record come first, so a kill at any later point is recoverable.
	if err = save(); err != nil {
		return rep, err
	}
	if w.Baseline != "" {
		if err = a.journal.SetActive(w.Baseline); err != nil {
			return rep, err
		}
	}
	if err = a.journal.StartBench(profile.BenchRecord{ID: id, Baseline: w.Baseline, Paused: paused}); err != nil {
		return rep, err
	}
	defer func() {
		rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget.Restore+benchSwitchSlack)
		defer cancel()
		o := a.options(nil, profile.Profile{})
		if bp, ok := a.profiles[w.Baseline]; ok {
			o = a.options(&bp, bp)
		}
		rerr := profile.Recover(rc, a.host, a.journal, a.profiles, o, func(e profile.Event) { fmt.Fprintf(out, "restore: %s %s\n", e.Phase, e.Profile) })
		switch {
		case rerr != nil:
			rep.Restoration, rep.Run = "failed", "failed"
			runErr = errors.Join(runErr, fmt.Errorf("restoration failed: %w", rerr))
		default:
			now := a.now().UTC()
			rep.Restoration, rep.RestoredAt = "verified", &now
		}
		if rep.Run == "running" {
			rep.Run = "completed"
			if runErr != nil {
				rep.Run = "failed"
			}
		}
		runErr = errors.Join(runErr, save())
	}()
	for _, u := range paused {
		if err = a.host.ControlUnit(ctx, "stop", u); err != nil {
			return rep, err
		}
		if err = a.host.ConfirmInactive(ctx, u); err != nil {
			return rep, err
		}
	}
	st, _ := a.journal.Read()
	cur, _, err := profile.Current(ctx, a.host, st, a.profiles)
	if err != nil {
		return rep, err
	}
	for i, pid := range w.Order {
		p := a.profiles[pid]
		key := ""
		if p.Serving.KeyFile != "" {
			b, kerr := os.ReadFile(p.Serving.KeyFile)
			if kerr != nil {
				return rep, fmt.Errorf("read key file for %s: %w", pid, kerr)
			}
			key = strings.TrimSpace(string(b))
		}
		fmt.Fprintln(out, "benchmark profile:", pid)
		res := benchResult{Profile: pid}
		rep.Profiles = append(rep.Profiles, res)
		var start func(context.Context) error
		if cur == nil || cur.ID != pid {
			from := cur
			start = func(c context.Context) error {
				o := a.options(from, p)
				o.AllowBenchmark = true
				return profile.Journaled(c, a.host, a.journal, from, p, o, (&progress{a: a}).emit)
			}
		}
		perr := benchPhase(benchFields(tctx, map[string]any{"profile": pid}), "profile", func(c context.Context) error {
			return measureBenchProfile(c, &res, p.Serving.Port, p.Serving.Model, key, start, func(phase context.Context) error {
				for n := 1; n <= reps; n++ {
					for _, task := range tasks {
						if cerr := phase.Err(); cerr != nil {
							return fmt.Errorf("run stopped before %s %s #%d: %w", pid, task.ID, n, cerr)
						}
						r, e := runTask(phase, imageID, p.Serving.Model, key, p.Serving.Port, task, n)
						fmt.Fprintf(out, "%s %s #%d: %s\n", pid, task.ID, n, r.Outcome)
						res.Tasks = append(res.Tasks, r)
						rep.Profiles[i] = res
						if serr := save(); serr != nil {
							return serr
						}
						if e != nil {
							return e
						}
					}
				}
				// A deadline or cancellation that lands during the last attempt must not
				// pass for a finished profile.
				return phase.Err()
			}, started.Format(time.RFC3339Nano))
		})
		if perr != nil {
			res.Error = perr.Error()
			rep.Profiles[i] = res
			rep.Run = "failed"
			return rep, perr
		}
		rep.Profiles[i] = res
		cur = &p
	}
	if cerr := ctx.Err(); cerr != nil {
		return rep, fmt.Errorf("run did not finish: %w", cerr)
	}
	return rep, nil
}

func (a *app) writeBenchFile(id string, r benchReport) error {
	dir := filepath.Join(a.paths.Data, "benchmarks")
	for _, name := range []string{id + ".json", "latest.json"} {
		if err := writeJSONAtomic(dir, 0o755, 0o644, name, r); err != nil {
			return err
		}
		if a.chownPath != nil {
			if err := errors.Join(a.chownPath(dir), a.chownPath(filepath.Join(dir, name))); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- launch -----------------------------------------------------------------

type launchDoc struct {
	SchemaVersion int      `json:"schema_version"`
	Kind          string   `json:"kind"`
	Outcome       string   `json:"outcome"` // ready | refused
	Profiles      []string `json:"profiles"`
	Baseline      string   `json:"baseline"`
	Gate          bool     `json:"gate"`
	Binary        string   `json:"binary,omitempty"`
	SHA256        string   `json:"sha256,omitempty"`
	Budget        *Budget  `json:"budget,omitempty"`
	Downtime      []string `json:"downtime"`
	Script        string   `json:"script,omitempty"`
	Commands      []string `json:"commands,omitempty"`
	Report        string   `json:"report,omitempty"`
	Error         string   `json:"error,omitempty"`
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

func joinQuoted(argv []string) string {
	out := make([]string, len(argv))
	for i, w := range argv {
		out[i] = shQuote(w)
	}
	return strings.Join(out, " ")
}

func (a *app) benchLaunch(ctx context.Context, order []string, gate bool) int {
	doc := launchDoc{SchemaVersion: 1, Kind: "benchmark_launch", Profiles: order, Gate: gate, Downtime: []string{}}
	finish := func(code int) int {
		if a.json {
			a.emit(doc)
		} else if doc.Error != "" {
			for _, line := range strings.Split(doc.Error, "; ") {
				fmt.Fprintf(a.err, "error: %s\n", line)
			}
		}
		return code
	}
	st, err := a.journal.Read()
	var cur *profile.Profile
	if err == nil {
		cur, _, err = profile.Current(ctx, a.host, st, a.profiles)
	}
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return finish(exitPrecondition)
	}
	if cur != nil {
		doc.Baseline = cur.ID
	}
	if problems := a.benchPreflight(ctx, order, doc.Baseline); len(problems) > 0 {
		doc.Outcome, doc.Error = "refused", strings.Join(problems, "; ")
		return finish(exitPrecondition)
	}
	exe, err := a.exe()
	if err == nil {
		doc.Binary = exe
		doc.SHA256, err = a.sha256(exe)
	}
	if err != nil {
		doc.Outcome, doc.Error = "refused", err.Error()
		return finish(exitError)
	}
	b := benchBudget(a.profiles, order, doc.Baseline, gate)
	doc.Budget = &b
	// The baseline is only stopped when some other profile has to run. Benchmarking the
	// baseline alone leaves it serving and pauses only the units its profile lists.
	others := false
	for _, id := range order {
		if id != doc.Baseline {
			others = true
			doc.Downtime = append(doc.Downtime, id+" is started and stopped during the run")
		}
	}
	if doc.Baseline != "" && others {
		doc.Downtime = append([]string{doc.Baseline + " is stopped while the other profiles run and is restored at the end"}, doc.Downtime...)
	}
	if !others && doc.Baseline != "" {
		doc.Downtime = append(doc.Downtime, "no model downtime: "+doc.Baseline+" keeps serving and is measured in place")
	}
	if paused, perr := a.pausedUnits(ctx, order, doc.Baseline); perr == nil && len(paused) > 0 {
		doc.Downtime = append(doc.Downtime, strings.Join(paused, ", ")+" is stopped during the run and started again after")
	}
	dst := "/usr/local/sbin/spark-roster-" + doc.SHA256
	common := []string{"--operator", a.user, "--data", a.paths.Data, "--profiles", a.paths.Profiles}
	worker := append(append([]string{dst, "benchmark", "worker"}, common...), "--baseline", doc.Baseline)
	if gate {
		worker = append(worker, "--gate")
	}
	worker = append(worker, order...)
	hook := append([]string{dst, "recover", "--yes"}, common...)
	ceiling := int64(b.systemdCeiling() / time.Second)
	stop := int64(b.stopBudget() / time.Second)
	props := []string{"--unit=spark-roster-benchmark", "--property=Type=exec",
		fmt.Sprintf("--property=RuntimeMaxSec=%ds", ceiling), fmt.Sprintf("--property=TimeoutStopSec=%ds", stop),
		"--property=ExecStopPost=" + profile.QuoteExec(hook)}
	doc.Commands = []string{
		"printf '%s  %s\\n' " + doc.SHA256 + " " + shQuote(exe) + " | sha256sum -c",
		"sudo install -o root -g root -m 0755 " + shQuote(exe) + " " + shQuote(dst),
		"sudo systemd-run " + joinQuoted(props) + " " + joinQuoted(worker),
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "sha=%s\nsrc=%s\ndst=%s\n", doc.SHA256, shQuote(exe), shQuote(dst))
	sb.WriteString("run_args=(\n")
	for _, p := range props {
		fmt.Fprintf(&sb, "  %s\n", shQuote(p))
	}
	sb.WriteString(")\nworker=(\n  \"$dst\" benchmark worker")
	for _, w := range worker[3:] {
		fmt.Fprintf(&sb, " %s", shQuote(w))
	}
	sb.WriteString("\n)\n")
	sb.WriteString("printf '%s  %s\\n' \"$sha\" \"$src\" | sha256sum -c &&\n  sudo install -o root -g root -m 0755 \"$src\" \"$dst\" &&\n  sudo systemd-run \"${run_args[@]}\" \"${worker[@]}\"\n")
	doc.Script = sb.String()
	doc.Report = filepath.Join(a.paths.Data, "benchmarks", "latest.json")
	doc.Outcome = "ready"
	if a.json {
		return finish(exitOK)
	}
	fmt.Fprintf(a.out, "Benchmark of %s", strings.Join(order, ", "))
	if gate {
		fmt.Fprint(a.out, " (gate: one task, one repetition)")
	}
	fmt.Fprintln(a.out, ".")
	for _, d := range doc.Downtime {
		fmt.Fprintf(a.out, "downtime: %s\n", d)
	}
	fmt.Fprintf(a.out, "ceiling %s = attempts %s + speed probes %s + starting profiles %s + restoring the baseline %s + margin %s. It is a watchdog derived from enforced limits, not an estimate.\n",
		b.Total, b.Attempts, b.Speed, b.Switches, b.Restore, b.Margin)
	fmt.Fprintf(a.out, "\nRun this in a terminal (it asks for your sudo password):\n\n%s\n", doc.Script)
	fmt.Fprintf(a.out, "Follow:  journalctl -fu spark-roster-benchmark.service\nReport:  spark-roster benchmark report %s\nIf the run dies, systemd restores the baseline. By hand: spark-roster recover\n", doc.Report)
	return exitOK
}

// ---- worker and report commands ---------------------------------------------

// benchmarkManaged says whether `benchmark ...` is one of the profile-based
// subcommands. The older fixed-profile subcommands keep their own handler.
func benchmarkManaged(args []string) bool {
	if len(args) < 2 || args[0] != "benchmark" {
		return false
	}
	switch args[1] {
	case "launch", "worker", "compare":
		return true
	case "report":
		return len(args) >= 3 // without a file it still prints the older latest report
	}
	return false
}

func manageBenchmark(ctx context.Context, args []string, out, diag io.Writer, e env) (int, bool) {
	if !benchmarkManaged(args) {
		return 0, false
	}
	sub := args[1]
	fs := flag.NewFlagSet("benchmark "+sub, flag.ContinueOnError)
	fs.SetOutput(diag)
	asJSON := fs.Bool("json", false, "write one JSON document to stdout")
	gate := fs.Bool("gate", false, "one task and one repetition per profile")
	baseline := fs.String("baseline", "", "profile expected to be serving (worker)")
	operator := fs.String("operator", "", "the user a root-run worker acts for")
	profilesDir := fs.String("profiles", "", "directory of site profiles")
	dataDir := fs.String("data", "", "directory for weights, builds and state")
	flagArgs, positional := splitArgs(args[2:], "baseline", "operator", "profiles", "data")
	if err := fs.Parse(flagArgs); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK, true
		}
		return exitUsage, true
	}
	switch sub {
	case "report":
		if len(positional) != 1 {
			fmt.Fprintln(diag, "error: benchmark report takes one report file")
			return exitUsage, true
		}
		return benchReportFile(positional[0], out, diag), true
	case "compare":
		if len(positional) != 2 {
			fmt.Fprintln(diag, "error: benchmark compare takes two report files")
			return exitUsage, true
		}
		return benchCompareFiles(positional[0], positional[1], *asJSON, out, diag), true
	}
	if len(positional) == 0 {
		fmt.Fprintf(diag, "error: benchmark %s needs at least one profile id. Run `spark-roster list`\n", sub)
		return exitUsage, true
	}
	if sub == "launch" && (*operator != "" || *baseline != "") {
		fmt.Fprintln(diag, "error: --operator and --baseline belong to the root-run worker; launch works them out itself")
		return exitUsage, true
	}
	if sub == "launch" && e.euid == 0 {
		fmt.Fprintln(diag, "error: run launch as your normal user. It prints the sudo commands to run")
		return exitUsage, true
	}
	a, code := buildAppFor(e, *asJSON, *profilesDir, *dataDir, *operator, out, diag)
	if code != exitOK {
		return code, true
	}
	if sub == "launch" {
		return a.benchLaunch(ctx, positional, *gate), true
	}
	if e.euid != 0 || *operator == "" {
		fmt.Fprintln(diag, "error: the benchmark worker runs as root for an operator; use `spark-roster benchmark launch` to get the command")
		return exitUsage, true
	}
	if e.getenv("INVOCATION_ID") == "" {
		fmt.Fprintln(diag, "error: run the worker under systemd with the recovery hook that `benchmark launch` prints")
		return exitUsage, true
	}
	unlock, err := a.lock(a.paths.Data)
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return exitPrecondition, true
	}
	defer unlock()
	// The lock file was just created by root. The operator's own commands open it too.
	if a.chownPath != nil {
		for _, name := range []string{"lock"} {
			if err := a.chownPath(filepath.Join(a.paths.Data, name)); err != nil && !os.IsNotExist(err) {
				fmt.Fprintf(diag, "error: hand the lock file to %s: %v\n", *operator, err)
				return exitError, true
			}
		}
	}
	budget := benchBudget(a.profiles, positional, *baseline, *gate)
	run, cancel := context.WithTimeout(ctx, budget.Total)
	defer cancel()
	rep, err := a.benchWorker(run, benchWorkerOpts{Order: positional, Baseline: *baseline, Gate: *gate, Operator: *operator}, out)
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		if rep.Restoration == "failed" {
			return exitUnsettled, true
		}
		return exitError, true
	}
	fmt.Fprintf(out, "benchmark completed; baseline %s verified. Report: %s\n", orNone(rep.Baseline), filepath.Join(a.paths.Data, "benchmarks", "latest.json"))
	return exitOK, true
}

func readReportFile(path string) (benchReport, error) {
	var r benchReport
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&r); err != nil {
		return r, fmt.Errorf("%s: %w", path, err)
	}
	if r.Schema != 2 && r.Schema != 3 {
		return r, fmt.Errorf("%s: not a benchmark report (schema %d)", path, r.Schema)
	}
	return r, nil
}

func benchReportFile(path string, out, diag io.Writer) int {
	r, err := readReportFile(path)
	if err == nil {
		err = printBenchScorecard(out, r)
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return exitError
	}
	return exitOK
}

// ---- compare ----------------------------------------------------------------

type profileDiff struct {
	Profile    string         `json:"profile"`
	In         string         `json:"in"` // both | first | second
	PassA      int            `json:"pass_first"`
	AttemptsA  int            `json:"attempts_first"`
	PassB      int            `json:"pass_second"`
	AttemptsB  int            `json:"attempts_second"`
	TaskDeltas map[string]int `json:"task_pass_delta,omitempty"` // second minus first, only where it differs
	ReadyMSA   int64          `json:"ready_ms_first,omitempty"`
	ReadyMSB   int64          `json:"ready_ms_second,omitempty"`
	DecodeA    float64        `json:"decode_tok_s_first,omitempty"`
	DecodeB    float64        `json:"decode_tok_s_second,omitempty"`
}

type compareDoc struct {
	SchemaVersion int           `json:"schema_version"`
	Kind          string        `json:"kind"`
	Comparable    bool          `json:"comparable"`
	Differences   []string      `json:"setting_differences"`
	Profiles      []profileDiff `json:"profiles"`
}

func medianF(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	n := len(v)
	return (v[n/2] + v[(n-1)/2]) / 2
}

func summarize(p benchResult) (pass, attempts int, byTask map[string]int, decode float64) {
	byTask = map[string]int{}
	for _, t := range p.Tasks {
		attempts++
		if t.Outcome == "pass" {
			pass++
			byTask[t.Task]++
		}
	}
	var cold []float64
	for _, s := range p.Speed {
		if s.Error == "" && s.Cache == "cold-intended" && s.DecodeEstimate != nil {
			cold = append(cold, *s.DecodeEstimate)
		}
	}
	return pass, attempts, byTask, medianF(cold)
}

// compareReports lines up two runs and says plainly when their settings differ,
// because a different suite, window or output cap makes pass counts incomparable.
func compareReports(a, b benchReport) compareDoc {
	d := compareDoc{SchemaVersion: 1, Kind: "benchmark_compare", Differences: []string{}}
	chk := func(name string, x, y any) {
		if fmt.Sprint(x) != fmt.Sprint(y) {
			d.Differences = append(d.Differences, fmt.Sprintf("%s: %v vs %v", name, x, y))
		}
	}
	chk("suite", a.Suite, b.Suite)
	chk("pi_context", a.PiContext, b.PiContext)
	chk("pi_output_cap", a.PiOutputCap, b.PiOutputCap)
	chk("task_timeout_seconds", a.TaskLimitSec, b.TaskLimitSec)
	chk("repetitions", a.Repetitions, b.Repetitions)
	chk("task_count", a.TaskCount, b.TaskCount)
	chk("mode", firstNonEmpty(a.Mode, "benchmark"), firstNonEmpty(b.Mode, "benchmark"))
	d.Comparable = len(d.Differences) == 0
	index := func(r benchReport) map[string]benchResult {
		m := map[string]benchResult{}
		for _, p := range r.Profiles {
			m[p.Profile] = p
		}
		return m
	}
	ia, ib := index(a), index(b)
	ids := map[string]bool{}
	for id := range ia {
		ids[id] = true
	}
	for id := range ib {
		ids[id] = true
	}
	var sorted []string
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)
	for _, id := range sorted {
		pa, okA := ia[id]
		pb, okB := ib[id]
		row := profileDiff{Profile: id, In: "both"}
		var ta, tb map[string]int
		switch {
		case okA && !okB:
			row.In = "first"
		case okB && !okA:
			row.In = "second"
		}
		if okA {
			row.PassA, row.AttemptsA, ta, row.DecodeA = summarize(pa)
			row.ReadyMSA = pa.SwitchReadyMS
		}
		if okB {
			row.PassB, row.AttemptsB, tb, row.DecodeB = summarize(pb)
			row.ReadyMSB = pb.SwitchReadyMS
		}
		if okA && okB {
			row.TaskDeltas = map[string]int{}
			tasks := map[string]bool{}
			for t := range ta {
				tasks[t] = true
			}
			for t := range tb {
				tasks[t] = true
			}
			for t := range tasks {
				if delta := tb[t] - ta[t]; delta != 0 {
					row.TaskDeltas[t] = delta
				}
			}
		}
		d.Profiles = append(d.Profiles, row)
	}
	return d
}

func benchCompareFiles(pa, pb string, asJSON bool, out, diag io.Writer) int {
	a, err := readReportFile(pa)
	var b benchReport
	if err == nil {
		b, err = readReportFile(pb)
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return exitError
	}
	d := compareReports(a, b)
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if enc.Encode(d) != nil {
			return exitError
		}
		return exitOK
	}
	if d.Comparable {
		fmt.Fprintln(out, "Same suite and settings: pass counts are comparable.")
	} else {
		fmt.Fprintln(out, "warning: the runs used different settings, so pass counts are not directly comparable:")
		for _, x := range d.Differences {
			fmt.Fprintf(out, "  %s\n", x)
		}
	}
	fmt.Fprintf(out, "\n%-28s %-8s %-10s %-10s %s\n", "PROFILE", "IN", "FIRST", "SECOND", "TASK CHANGES (second minus first)")
	for _, r := range d.Profiles {
		changes := []string{}
		for t, v := range r.TaskDeltas {
			changes = append(changes, fmt.Sprintf("%s %+d", t, v))
		}
		sort.Strings(changes)
		fmt.Fprintf(out, "%-28s %-8s %-10s %-10s %s\n", r.Profile, r.In, fmt.Sprintf("%d/%d", r.PassA, r.AttemptsA), fmt.Sprintf("%d/%d", r.PassB, r.AttemptsB), strings.Join(changes, ", "))
	}
	return exitOK
}

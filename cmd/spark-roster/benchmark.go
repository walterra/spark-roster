package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const benchmarkState = "/var/lib/spark-roster-benchmark"
const benchmarkUnit = "spark-roster-benchmark.service"

// 24 attempts per profile (240 min) plus speed, startup, grading and cleanup.
const benchmarkPiLifetime = 285 * time.Minute

// 720 min of attempts plus speed, model transitions, grading and cleanup.
// The transient unit's recovery hook gets its own stop budget.
const benchmarkRunLimit = 855 * time.Minute

type benchResult struct {
	Profile       string       `json:"profile"`
	SwitchReadyMS int64        `json:"switch_to_verified_generation_ms,omitempty"`
	MinAvailable  uint64       `json:"min_sampled_host_available_bytes,omitempty"`
	Speed         []benchSpeed `json:"speed,omitempty"`
	Tasks         []piResult   `json:"tasks,omitempty"`
	Error         string       `json:"error,omitempty"`
}

type benchReport struct {
	Schema        int           `json:"schema_version"`
	Started       time.Time     `json:"started_at"`
	Operator      string        `json:"operator"`
	OriginalImage string        `json:"original_image"`
	Profiles      []benchResult `json:"profiles"`
	Run           string        `json:"run"`
	Restoration   string        `json:"restoration"`
	RestoredAt    *time.Time    `json:"restored_at,omitempty"`
	PiImage       string        `json:"pi_runner_image,omitempty"`
	Suite         string        `json:"suite,omitempty"`
	TaskCount     int           `json:"task_count,omitempty"`
	TaskLimitSec  int           `json:"task_timeout_seconds,omitempty"`
	Repetitions   int           `json:"repetitions,omitempty"`
	Mode          string        `json:"mode,omitempty"`
	// Settings actually applied to servers and Pi, so a report states what was measured.
	ServerContext int  `json:"server_context,omitempty"`
	PiContext     int  `json:"pi_context,omitempty"`
	PiOutputCap   int  `json:"pi_output_cap,omitempty"`
	CanaryActive  bool `json:"canary_was_active,omitempty"`
}

var benchUsername = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,63}$`)

func runBenchmark(args []string, out, diag io.Writer) int {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(out, "Usage: spark-roster benchmark stage|selftest|score-stock|preflight|check|status|report|run OPERATOR|gate OPERATOR|recover OPERATOR|qualify-preflight|qualify OPERATOR|qualify-status|qualify-recover OPERATOR")
		return 0
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" {
		fmt.Fprintln(diag, "error: benchmark requires Linux ARM64")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), benchmarkRunLimit)
	defer cancel()
	if strings.HasPrefix(args[0], "qualify") {
		return runQualify(ctx, args, out, diag)
	}
	var err error
	switch {
	case len(args) == 1 && args[0] == "selftest" && os.Geteuid() != 0:
		// Same lock as run/check: selftest shares the benchmark container name and
		// cleanup label, so it must never overlap a real run.
		var home string
		home, err = os.UserHomeDir()
		if err == nil {
			self, stop := context.WithTimeout(ctx, 10*time.Minute)
			defer stop()
			err = withBenchLock(home, func() error { return runSelftest(self, out) })
		}
	case len(args) == 1 && args[0] == "score-stock" && os.Geteuid() != 0:
		var home string
		home, err = os.UserHomeDir()
		if err == nil {
			err = withBenchLock(home, func() error { return runScoreStock(ctx, home, out) })
		}
	case len(args) == 1 && args[0] == "telemetry-check":
		err = checkBenchTelemetry(ctx, out)
	case len(args) == 1 && args[0] == "stage" && os.Geteuid() != 0:
		stage, stop := context.WithTimeout(ctx, 15*time.Minute)
		defer stop()
		err = stageBenchImage(stage, out)
	case len(args) == 1 && args[0] == "check" && os.Geteuid() != 0:
		var home string
		home, err = os.UserHomeDir()
		if err == nil {
			err = withBenchLock(home, func() error { return checkBenchPi(ctx, home, out) })
		}
	case len(args) == 1 && args[0] == "preflight" && os.Geteuid() != 0:
		var home string
		home, err = os.UserHomeDir()
		if err == nil {
			err = benchPreflight(ctx, home)
		}
		if err == nil {
			fmt.Fprintln(out, "benchmark preflight passed: stock generation, pinned image, staged candidates, free candidate ports")
		}
	case len(args) == 1 && (args[0] == "status" || args[0] == "report"):
		var r benchReport
		if err = readJSON(filepath.Join(benchmarkState, "latest.json"), &r); err == nil {
			if args[0] == "report" {
				err = printBenchScorecard(out, r)
			} else {
				err = json.NewEncoder(out).Encode(r)
			}
		}
	case len(args) == 2 && (args[0] == "run" || args[0] == "gate" || args[0] == "recover") && os.Geteuid() == 0 && benchUsername.MatchString(args[1]):
		var u *user.User
		u, err = user.Lookup(args[1])
		if err != nil {
			break
		}
		if u.Uid == "0" || !filepath.IsAbs(u.HomeDir) || filepath.Base(u.HomeDir) != u.Username {
			err = errors.New("invalid benchmark operator")
			break
		}
		if (args[0] == "run" || args[0] == "gate") && os.Getenv("INVOCATION_ID") == "" {
			err = errors.New("run under systemd with the ExecStopPost recovery hook from the runbook")
			break
		}
		err = withBenchLock(u.HomeDir, func() error {
			if args[0] == "recover" {
				r, c := context.WithTimeout(ctx, 16*time.Minute)
				defer c()
				return recoverBenchmark(r, u, out)
			}
			if args[0] == "gate" {
				g, c := context.WithTimeout(ctx, gateRunLimit)
				defer c()
				return executeBenchmarkMode(g, u, out, true)
			}
			return executeBenchmark(ctx, u, out)
		})
	default:
		fmt.Fprintln(diag, "error: invalid benchmark subcommand, arguments, or privilege; see benchmark --help")
		return 2
	}
	if err != nil {
		fmt.Fprintln(diag, "error:", err)
		return 1
	}
	return 0
}

func benchPaths(home string) (string, string) {
	base := filepath.Join(home, ".local", "share", "spark-roster")
	return filepath.Join(base, "pi-q8"), filepath.Join(base, "inkling-2-mini", inklingRevision)
}

func withBenchLock(home string, fn func() error) error {
	pi, _ := benchPaths(home)
	f, err := os.OpenFile(filepath.Join(pi, "lock"), os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("benchmark operation lock: %w", err)
	}
	return fn()
}

func benchStockIdentity(ctx context.Context) error {
	for _, unit := range stockUnits {
		s, err := output(ctx, "systemctl", "show", "-p", "ActiveState", "--value", unit)
		if err != nil || s != "active" {
			return fmt.Errorf("stock unit not active: %s", unit)
		}
	}
	s, err := output(ctx, "docker", "container", "inspect", "--format", "{{.Image}} {{.State.Running}}", "qwen38-sglang")
	if err != nil || s != stockImage+" true" {
		return errors.New("stock restore image does not match verified baseline")
	}
	return nil
}

func benchPreflight(ctx context.Context, home string) error {
	if _, err := benchRunnerImage(ctx); err != nil {
		return err
	}
	if err := benchStockIdentity(ctx); err != nil {
		return err
	}
	if err := confirmedInactive(ctx, piUnit); err != nil {
		return err
	}
	if s, err := inklingState(ctx); err != nil || s != "absent" {
		return fmt.Errorf("Inkling not confirmed absent: %s: %v", s, err)
	}
	pi, ink := benchPaths(home)
	if err := checkInklingImage(ctx); err != nil {
		return err
	}
	if b, err := os.ReadFile(filepath.Join(ink, "staged")); err != nil || strings.TrimSpace(string(b)) != inklingRevision {
		return errors.New("run inkling stage first")
	}
	if err := checkInklingFiles(ink); err != nil {
		return err
	}
	// Hash the Q8 checkpoint before spending stock downtime. The switch rechecks it.
	if err := verifiedFile(filepath.Join(pi, "Qwen3.8-27B-pi-Q8_0.gguf"), piModelSHA, piModelBytes); err != nil {
		return err
	}
	if s, err := os.Stat(filepath.Join(pi, "llama-"+llamaRevision, piBuildDir, "bin", "llama-server")); err != nil || !s.Mode().IsRegular() || s.Mode().Perm()&0100 == 0 {
		return errors.New("run pi-q8 stage first")
	}
	for _, port := range []uint16{piPort, inklingPort} {
		if err := benchFreePort(port); err != nil {
			return err
		}
	}
	probe, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return stockProbe(probe, home)
}

func writeBenchReport(r benchReport) error { return writeBenchmarkJSON("latest.json", r) }

func writeBenchmarkJSON(name string, r any) error {
	return writeJSONAtomic(benchmarkState, 0755, 0644, name, r)
}

// writeJSONAtomic replaces dir/name with the JSON encoding of v. A reader sees either
// the old file or the new one, never a partial write, and no temp file is left behind.
func writeJSONAtomic(dir string, dirMode, fileMode os.FileMode, name string, v any) error {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".report-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err = json.NewEncoder(f).Encode(v); err != nil {
		return err
	}
	if err = f.Chmod(fileMode); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// gateRunLimit bounds the 256K gate: Pi Q8 and Inkling each get one launch (4 + 9 min
// startup budgets), one speed set (at most 6 requests of 2 min), and one scored task
// (10 min); stock restore is budgeted at 9 min. That sums to 66 min; 90 min leaves margin.
const gateRunLimit = 90 * time.Minute

func executeBenchmark(ctx context.Context, u *user.User, out io.Writer) error {
	return executeBenchmarkMode(ctx, u, out, false)
}

// executeBenchmarkMode runs the scored comparison, or with gate set a short check of
// the candidate profiles at the scored settings: one task, one repetition, no stock run.
func executeBenchmarkMode(ctx context.Context, u *user.User, out io.Writer, gate bool) (runErr error) {
	if err := qualificationSettled(); err != nil {
		return err
	}
	var prior benchReport
	if err := readJSON(filepath.Join(benchmarkState, "latest.json"), &prior); err == nil {
		if prior.Restoration != "verified" {
			return errors.New("previous restoration unresolved; run benchmark recover")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := benchPreflight(ctx, u.HomeDir); err != nil {
		return err
	}
	key, err := os.ReadFile(filepath.Join(u.HomeDir, ".config", "qwen38", "api-key"))
	if err != nil {
		return err
	}
	image, err := benchRunnerImage(ctx)
	if err != nil {
		return err
	}
	suite, tasks, err := benchInputs()
	if err != nil {
		return err
	}
	mode, repeats := "benchmark", benchRepeats
	if gate {
		mode, repeats, tasks = "gate", 1, tasks[:1]
	}
	r := benchReport{Schema: 2, Started: time.Now().UTC(), Operator: u.Username, OriginalImage: stockImage, Run: "running", Restoration: "pending", PiImage: image, Suite: suite, Repetitions: repeats, Mode: mode, TaskCount: len(tasks), TaskLimitSec: int(taskLimit / time.Second), ServerContext: scoredWindow, PiContext: scoredWindow, PiOutputCap: scoredOutput}
	canary, err := qualCanaryState(ctx)
	if err != nil {
		return err
	}
	r.CanaryActive = canary
	ctx, flush, err := startBenchTelemetry(ctx, mode, r.Started, out, true)
	if err != nil {
		return err
	}
	defer flush()
	benchEventAt(ctx, "run.start", map[string]any{"mode": mode, "work_timeout_ms": benchmarkRunLimit.Milliseconds(), "task_timeout_ms": taskLimit.Milliseconds(), "server_stock_context": 262144, "server_candidate_context": scoredWindow, "canary_was_active": canary, "pi_context": scoredWindow, "pi_output_cap": scoredOutput, "pi_image": image, "suite": suite})
	defer func() {
		benchEventAt(ctx, "run.end", map[string]any{"outcome": r.Run, "error_kind": benchErrorKind(runErr)})
	}()
	if err = writeBenchReport(r); err != nil {
		return err
	}
	// The canary would add generation load to timed attempts. The report is written
	// first, so an interruption between here and the end is restored by recover.
	if err = command(ctx, out, "", "systemctl", "stop", canaryTimer, canaryService); err != nil {
		return err
	}
	for _, unit := range []string{canaryTimer, canaryService} {
		if err = confirmedInactive(ctx, unit); err != nil {
			return err
		}
	}
	pi, ink := benchPaths(u.HomeDir)
	targets := []struct {
		id, model, key string
		port           uint16
		switchTo       func(context.Context) error
	}{
		{"stock-qwen", "qwen3.8-27b", strings.TrimSpace(string(key)), 30001, nil},
		{"pi-q8", "qwen3.8-pi", "", piPort, func(c context.Context) error {
			return switchPiQ8Window(c, u.HomeDir, pi, benchmarkPiLifetime, scoredWindow, out)
		}},
		{"inkling-2-mini", inklingModel, "", inklingPort, func(c context.Context) error {
			return handoffBenchmark(c, func(c context.Context) error { return stopPiQ8(c, out) }, func(c context.Context) error { return launchInkling(c, ink, scoredWindow, out) })
		}},
	}
	if gate {
		targets = targets[1:]
	}
	for i, t := range targets {
		fmt.Fprintln(out, "benchmark profile:", t.id)
		profileCtx := benchFields(ctx, map[string]any{"profile": t.id})
		res := benchResult{Profile: t.id}
		r.Profiles = append(r.Profiles, res)
		err = benchPhase(profileCtx, "profile", func(c context.Context) error {
			return measureBenchProfile(c, &res, t.port, t.model, t.key, t.switchTo, func(phase context.Context) error {
				for rep := 1; rep <= repeats; rep++ {
					for _, task := range tasks {
						result, err := runPiTaskScored(phase, image, t.model, t.key, t.port, task, rep)
						fmt.Fprintf(out, "%s %s #%d: %s\n", t.id, task.ID, rep, result.Outcome)
						res.Tasks = append(res.Tasks, result)
						r.Profiles[i] = res
						if err = errors.Join(err, writeBenchReport(r)); err != nil {
							return err
						}
					}
				}
				return nil
			}, r.Started.Format(time.RFC3339Nano))
		})
		benchEventAt(profileCtx, "profile.memory", map[string]any{"min_available_bytes": res.MinAvailable, "switch_ready_ms": res.SwitchReadyMS})
		if err != nil {
			res.Error = err.Error()
			r.Run = "failed"
		}
		r.Profiles[i] = res
		if saveErr := writeBenchReport(r); saveErr != nil {
			return errors.Join(err, saveErr)
		}
		if err != nil {
			return err
		}
	}
	r.Run = "completed"
	return writeBenchReport(r)
}

func handoffBenchmark(ctx context.Context, stop, launch func(context.Context) error) error {
	bounded, cancel := context.WithTimeout(ctx, time.Minute)
	err := stop(bounded)
	cancel()
	if err != nil {
		return fmt.Errorf("confirm Pi Q8 stopped before Inkling: %w", err)
	}
	return launch(ctx)
}

func recoverBenchmark(ctx context.Context, u *user.User, out io.Writer) error {
	var r benchReport
	if err := readJSON(filepath.Join(benchmarkState, "latest.json"), &r); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if r.Restoration == "verified" {
		return nil
	}
	if (r.Schema != 1 && r.Schema != 2) || r.Operator != u.Username || r.OriginalImage != stockImage {
		return errors.New("recovery journal does not match operator and stock image")
	}
	if r.Run == "running" {
		r.Run = "interrupted"
	}
	ctx, flush, _ := startBenchTelemetry(ctx, "benchmark", r.Started, out, false)
	defer flush()
	benchEventAt(ctx, "recovery.start", map[string]any{"prior_outcome": r.Run})
	defer func() { benchEventAt(ctx, "recovery.end", map[string]any{"restoration": r.Restoration}) }()
	// Both candidates must be gone before starting the original stock deployment.
	cleanup, cancel := context.WithTimeout(ctx, 20*time.Second)
	cleanupErr := cleanupBenchContainer(cleanup)
	cancel()
	err := stopInkling(ctx, out)
	if err == nil {
		err = restoreAuthorized(ctx, u.HomeDir, out)
	}
	if err == nil {
		err = benchStockIdentity(ctx)
	}
	if err == nil && r.CanaryActive {
		err = command(ctx, out, "", "systemctl", "start", canaryTimer)
	}
	if err == nil {
		var active bool
		active, err = qualCanaryState(ctx)
		if err == nil && active != r.CanaryActive {
			err = errors.New("canary state not restored")
		}
	}
	if err != nil {
		r.Restoration = "failed"
		return errors.Join(err, writeBenchReport(r))
	}
	now := time.Now().UTC()
	r.RestoredAt = &now
	r.Restoration = "verified"
	fmt.Fprintln(out, "benchmark cleanup: stock image, units and authenticated generation verified")
	return errors.Join(cleanupErr, writeBenchReport(r))
}

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const qualFile = "qualification.json"
const qualOutput = 16384
const canaryTimer = "qwen38-canary.timer"
const canaryService = "qwen38-canary.service"

type qualification struct {
	Started       time.Time     `json:"started_at"`
	Operator      string        `json:"operator"`
	OriginalImage string        `json:"original_image"`
	CanaryActive  bool          `json:"canary_was_active"`
	Run           string        `json:"run"`
	Restoration   string        `json:"restoration"`
	Shared        int           `json:"shared_context,omitempty"`
	PiImage       string        `json:"pi_runner_image"`
	Output        int           `json:"requested_output_tokens"`
	Profiles      []qualProfile `json:"profiles"`
	Error         string        `json:"error,omitempty"`
	PriorStarted  time.Time     `json:"prior_started_at"`
}

type qualProfile struct {
	Name     string         `json:"profile"`
	Verified int            `json:"verified_tier,omitempty"`
	Direct   int            `json:"direct_verified_tier,omitempty"`
	Server   int            `json:"server_context,omitempty"`
	Pi       qualPiEvidence `json:"pi"`
	Probes   []qualProbe    `json:"probes"`
}

type qualPiEvidence struct {
	Diagnostic  string        `json:"diagnostic,omitempty"`
	WallMS      int64         `json:"wall_ms,omitempty"`
	Window      int           `json:"configured_context"`
	Output      int           `json:"observed_output_cap"`
	Wire        []qualRequest `json:"wire_requests"`
	ToolCalls   int           `json:"tool_calls"`
	Requests    int64         `json:"requests"`
	Outcome     string        `json:"outcome"`
	PeakInput   int           `json:"peak_reported_prompt_tokens"`
	Compactions int           `json:"compactions"`
}

func qualifyPi(ctx context.Context, image string, t qualTarget, tier, lines int) (qualPiEvidence, error) {
	var mu sync.Mutex
	var wire []qualRequest
	observe := func(cap, status, size int, finish string) {
		mu.Lock()
		defer mu.Unlock()
		wire = append(wire, qualRequest{cap, size, status, finish})
	}
	marker := fmt.Sprintf("%x", sha256.Sum256([]byte(t.name+time.Now().String())))[:20]
	task := piTask{ID: "qualification-tool", Prompt: "First use the read tool on solution.cjs. Then edit the file to export a function returning the exact value of the seal variable you read. Do not guess.", Starter: "const seal = '" + marker + "';\nmodule.exports = () => null;", Cases: json.RawMessage(`[[]]`), Want: json.RawMessage(fmt.Sprintf(`[%q]`, marker))}
	seal := fmt.Sprintf("%x", sha256.Sum256([]byte(marker)))[:20]
	task.Attachment = qualFixture(lines, seal, [3]string{seal, "", ""})
	task.Prompt = "Use the read tool on solution.cjs. Then edit it to export a function returning the seal variable from that file and verified seal 1 from the attached inventory, joined with a slash. Use the exact values. The file is already attached; do not read it again."
	task.Want = json.RawMessage(fmt.Sprintf(`[%q]`, marker+"/"+seal))
	res, err := runPiTaskWithLimits(ctx, image, t.model, t.key, t.port, task, 1, tier, qualOutput, observe)
	mu.Lock()
	records := append([]qualRequest(nil), wire...)
	mu.Unlock()
	e := qualPiEvidence{Diagnostic: res.Diagnostic, WallMS: res.WallMS, Window: tier, Wire: records, ToolCalls: res.ToolCalls, Requests: res.Requests, Outcome: res.Outcome, PeakInput: res.PeakPrompt, Compactions: res.Compactions}
	for _, r := range records {
		e.Output = max(e.Output, r.Cap)
	}
	if err != nil {
		return e, err
	}
	if e.Outcome != "pass" || e.ToolCalls < 2 || e.Requests < 2 || e.Output < 1 || e.Output > qualOutput {
		return e, fmt.Errorf("Pi tool/grade or output-cap check failed: %+v", e)
	}
	return e, nil
}

type qualProbe struct {
	Server       int         `json:"server_context"`
	Tier         int         `json:"tier"`
	Stage        string      `json:"stage"`
	SizedInput   int         `json:"pretokenized_input,omitempty"`
	Metrics      *benchSpeed `json:"stream,omitempty"`
	MinAvailable uint64      `json:"min_available_bytes"`
	MaxSwapUsed  uint64      `json:"max_swap_used_bytes"`
	Error        string      `json:"error,omitempty"`
}

func qualRead(r *qualification) error { return readJSON(filepath.Join(benchmarkState, qualFile), r) }
func qualificationSettled() error {
	var r qualification
	if err := qualRead(&r); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if r.Restoration != "verified" {
		return errors.New("qualification recovery pending; run benchmark qualify-recover")
	}
	return nil
}

func runQualify(ctx context.Context, args []string, out, diag io.Writer) int {
	var err error
	switch {
	case len(args) == 1 && args[0] == "qualify-status":
		var r qualification
		if err = qualRead(&r); err == nil {
			e := json.NewEncoder(out)
			e.SetIndent("", "  ")
			err = e.Encode(r)
		}
	case len(args) == 1 && (args[0] == "qualify-check" || args[0] == "qualify-diagnose") && os.Geteuid() != 0:
		var home string
		home, err = os.UserHomeDir()
		if err == nil {
			err = withBenchLock(home, func() error {
				image, e := benchRunnerImage(ctx)
				if e != nil {
					return e
				}
				b, e := os.ReadFile(filepath.Join(home, ".config", "qwen38", "api-key"))
				if e != nil {
					return e
				}
				t := qualTarget{name: "stock-qwen", model: "qwen3.8-27b", port: 30001, key: strings.TrimSpace(string(b))}
				if args[0] == "qualify-diagnose" {
					p, e := qualifyPi(ctx, image, t, 262144, (262144-qualOutput-4096)*8*160/(10*5177))
					return errors.Join(e, json.NewEncoder(out).Encode(p))
				}
				p, e := qualifyPi(ctx, image, t, 262144, 160)
				if e == nil && p.Output != qualOutput {
					e = errors.New("short Pi request did not send configured cap")
				}
				if e == nil {
					_, _, n, err := qualTopFixture(ctx, t)
					if err != nil {
						return err
					}
					fmt.Fprintf(diag, "stock near-256K fixture rendered: %d tokens (no generation)\n", n)
				}
				return errors.Join(e, json.NewEncoder(out).Encode(p))
			})
		}
	case len(args) == 1 && args[0] == "qualify-preflight" && os.Geteuid() != 0:
		var home string
		home, err = os.UserHomeDir()
		if err == nil {
			err = withBenchLock(home, func() error { return qualPreflight(ctx, home) })
		}
		if err == nil {
			fmt.Fprintln(out, "qualification preflight: stock generation, candidate artifacts, ports and canary state verified")
		}
	case len(args) == 2 && (args[0] == "qualify" || args[0] == "qualify-recover") && os.Geteuid() == 0 && benchUsername.MatchString(args[1]):
		var u *user.User
		u, err = user.Lookup(args[1])
		if err != nil {
			break
		}
		if u.Uid == "0" || !filepath.IsAbs(u.HomeDir) || filepath.Base(u.HomeDir) != u.Username {
			err = errors.New("invalid qualification operator")
			break
		}
		if args[0] == "qualify" && os.Getenv("INVOCATION_ID") == "" {
			err = errors.New("use the systemd unit and recovery hook from the runbook")
			break
		}
		err = withBenchLock(u.HomeDir, func() error {
			budget := qualWorkBudget
			if args[0] == "qualify-recover" {
				budget = 16 * time.Minute
			}
			phase, cancel := context.WithTimeout(ctx, budget)
			defer cancel()
			if args[0] == "qualify-recover" {
				return recoverQualification(phase, u, out)
			}
			return executeQualification(phase, u, out)
		})
	default:
		err = errors.New("use benchmark qualify-preflight|qualify-check|qualify-status or systemd qualify/qualify-recover OPERATOR")
	}
	if err != nil {
		fmt.Fprintln(diag, "error:", err)
		return 1
	}
	return 0
}

func qualCanaryState(ctx context.Context) (bool, error) {
	s, err := output(ctx, "systemctl", "show", "-p", "ActiveState", "-p", "LoadState", "--value", canaryTimer)
	if err != nil {
		return false, err
	}
	f := strings.Fields(s)
	if len(f) == 2 && ((f[0] == "loaded" && f[1] == "active") || (f[1] == "loaded" && f[0] == "active")) {
		return true, nil
	}
	if len(f) == 2 && ((f[0] == "loaded" && f[1] == "inactive") || (f[1] == "loaded" && f[0] == "inactive")) {
		return false, nil
	}
	return false, fmt.Errorf("unexpected canary timer state: %q", s)
}

func qualPreflight(ctx context.Context, home string) error {
	if err := qualificationSettled(); err != nil {
		return err
	}
	var prev benchReport
	if err := readJSON(filepath.Join(benchmarkState, "latest.json"), &prev); err == nil {
		if prev.Restoration != "verified" {
			return errors.New("benchmark recovery pending")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := qualCanaryState(ctx); err != nil {
		return err
	}
	return benchPreflight(ctx, home)
}

func qualCandidatesClear(ctx context.Context) error {
	if err := confirmedInactive(ctx, piUnit); err != nil {
		return err
	}
	if state, err := inklingState(ctx); err != nil || state != "absent" {
		return fmt.Errorf("Inkling exit not confirmed: %s: %v", state, err)
	}
	return nil
}

func qualStopStock(ctx context.Context, out io.Writer) error {
	for i := len(stockUnits) - 1; i >= 0; i-- {
		if err := command(ctx, out, "", "systemctl", "stop", stockUnits[i]); err != nil {
			return err
		}
		if err := confirmedInactive(ctx, stockUnits[i]); err != nil {
			return err
		}
	}
	s, err := output(ctx, "docker", "ps", "--filter", "name=^/qwen38-sglang$", "--format", "{{.Names}}")
	if err != nil || s != "" {
		return errors.New("stock Docker exit not confirmed")
	}
	return nil
}

func executeQualification(ctx context.Context, u *user.User, out io.Writer) (err error) {
	if err = qualPreflight(ctx, u.HomeDir); err != nil {
		return err
	}
	var prior qualification
	if err = qualRead(&prior); err != nil {
		return fmt.Errorf("prior qualification required: %w", err)
	}
	if prior.Restoration != "verified" || prior.OriginalImage != stockImage || prior.Output != qualOutput {
		return errors.New("prior qualification is not compatible")
	}
	for _, name := range []string{"stock-qwen", "pi-q8", "inkling-2-mini"} {
		if tier, tokens := qualPriorTier(prior, name); tier == 0 || tokens < 160 {
			return fmt.Errorf("prior direct evidence missing for %s", name)
		}
	}
	image, err := benchRunnerImage(ctx)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(u.HomeDir, ".config", "qwen38", "api-key"))
	if err != nil {
		return err
	}
	active, err := qualCanaryState(ctx)
	if err != nil {
		return err
	}
	r := qualification{PriorStarted: prior.Started, Started: time.Now().UTC(), Operator: u.Username, OriginalImage: stockImage, CanaryActive: active, Run: "running", Restoration: "pending", PiImage: image, Output: qualOutput}
	ctx, flush, err := startBenchTelemetry(ctx, "qualification", r.Started, out, true)
	if err != nil {
		return err
	}
	defer flush()
	benchEventAt(ctx, "run.start", map[string]any{"mode": "qualification", "work_timeout_ms": qualWorkBudget.Milliseconds(), "request_timeout_ms": qualDirectBudget.Milliseconds(), "pi_session_timeout_ms": qualPiBudget.Milliseconds(), "pi_context_target": 262144, "pi_output_cap": qualOutput, "pi_image": image, "canary_was_active": active})
	defer func() {
		benchEventAt(ctx, "run.end", map[string]any{"outcome": r.Run, "shared_context": r.Shared, "error_kind": benchErrorKind(err)})
	}()
	save := func() error { return writeBenchmarkJSON(qualFile, r) }
	if err = save(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			r.Run = "failed"
			r.Error = err.Error()
			err = errors.Join(err, save())
		}
	}()
	if err = command(ctx, out, "", "systemctl", "stop", canaryTimer, canaryService); err != nil {
		return err
	}
	for _, unit := range []string{canaryTimer, canaryService} {
		if err = confirmedInactive(ctx, unit); err != nil {
			return err
		}
	}
	pi, ink := benchPaths(u.HomeDir)
	configs := []qualTarget{
		{name: "stock-qwen", model: "qwen3.8-27b", port: 30001, key: strings.TrimSpace(string(b))},
		{name: "pi-q8", model: "qwen3.8-pi", port: piPort, launch: func(c context.Context, n int) error {
			return launchPiQ8(c, u, filepath.Join(pi, "llama-"+llamaRevision, piBuildDir, "bin", "llama-server"), filepath.Join(pi, "Qwen3.8-27B-pi-Q8_0.gguf"), qualCandidateLifetime, n, out)
		}, stop: func(c context.Context) error { return stopPiQ8(c, out) }},
		{name: "inkling-2-mini", model: inklingModel, port: inklingPort, launch: func(c context.Context, n int) error { return launchInkling(c, ink, n, out) }, stop: func(c context.Context) error { return stopInkling(c, out) }},
	}
	for i, t := range configs {
		fmt.Fprintln(out, "qualifying", t.name)
		profileCtx := benchFields(ctx, map[string]any{"profile": t.name})
		if i == 1 {
			stop, cancel := context.WithTimeout(ctx, time.Minute)
			err = qualStopStock(stop, out)
			cancel()
			if err != nil {
				return err
			}
		}
		r.Profiles = append(r.Profiles, qualProfile{Name: t.name})
		p := &r.Profiles[i]
		err = benchPhase(profileCtx, "profile", func(c context.Context) error { return qualifyRetryTarget(c, t, image, p, prior, save, out) })
		if e := save(); e != nil {
			return errors.Join(err, e)
		}
		if err != nil {
			return err
		}
		if t.stop != nil {
			stop, cancel := context.WithTimeout(profileCtx, time.Minute)
			err = t.stop(stop)
			cancel()
			if err != nil {
				return err
			}
		}
	}
	r.Shared = r.Profiles[0].Verified
	for _, p := range r.Profiles {
		if p.Verified != r.Shared {
			r.Shared = 0
			break
		}
	}
	r.Run = "completed"
	fmt.Fprintln(out, "highest shared verified tier:", r.Shared)
	return save()
}

func recoverQualification(ctx context.Context, u *user.User, out io.Writer) error {
	var r qualification
	if err := qualRead(&r); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if r.Restoration == "verified" {
		return nil
	}
	if r.Operator != u.Username || r.OriginalImage != stockImage {
		return errors.New("qualification journal does not match original deployment")
	}
	if r.Run == "running" {
		r.Run = "interrupted"
	}
	ctx, flush, _ := startBenchTelemetry(ctx, "qualification", r.Started, out, false)
	defer flush()
	benchEventAt(ctx, "recovery.start", map[string]any{"prior_outcome": r.Run, "canary_was_active": r.CanaryActive})
	defer func() { benchEventAt(ctx, "recovery.end", map[string]any{"restoration": r.Restoration}) }()
	c, stop := context.WithTimeout(ctx, 20*time.Second)
	cleanup := cleanupBenchContainer(c)
	stop()
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
	err = errors.Join(cleanup, err)
	if err == nil {
		r.Restoration = "verified"
	} else {
		r.Restoration = "failed"
	}
	return errors.Join(err, writeBenchmarkJSON(qualFile, r))
}

type qualTarget struct {
	name, model, key string
	port             uint16
	launch           func(context.Context, int) error
	stop             func(context.Context) error
}

func qualVerdict(m benchSpeed, answer string, marker [3]string, near int) error {
	if m.PromptTokens == nil || m.CompletionTokens == nil {
		return errors.New("missing token usage")
	}
	if *m.PromptTokens < near-4096 || *m.PromptTokens > near {
		return fmt.Errorf("rendered input %d outside %d–%d", *m.PromptTokens, near-4096, near)
	}
	if m.FinishReason != "stop" {
		return fmt.Errorf("unexpected finish reason %q", m.FinishReason)
	}
	for _, s := range marker {
		if !strings.Contains(answer, s) {
			return errors.New("marker retrieval failed")
		}
	}
	return nil
}

func qualFixture(lines int, seed string, marker [3]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Repository inventory %s. Read all records.\n", seed)
	for i := 0; i < lines; i++ {
		for j, n := range []int{lines / 20, lines / 2, lines * 19 / 20} {
			if i == n && marker[j] != "" {
				fmt.Fprintf(&b, "Verified seal %d: %s\n", j+1, marker[j])
			}
		}
		h := sha256.Sum256([]byte(seed + strconv.Itoa(i)))
		fmt.Fprintf(&b, "module %s-%04d validates staged ledger rows and records journal checksum %x before updating the repository index.\n", []string{"river", "oak", "coast", "field", "maple", "delta", "ridge"}[i%7], i, h[:5])
	}
	if marker[0] != "" {
		b.WriteString("Return the three verified seal values exactly as written, one per line. No explanation.")
	} else {
		b.WriteString("Reply with OK.")
	}
	return b.String()
}

func qualSwapUsed(total, free *uint64) uint64 {
	if total == nil || free == nil || *free > *total {
		return 0
	}
	return *total - *free
}

func qualMeasure(ctx context.Context, p *qualProbe, run func(context.Context) error) (error, error) {
	m, err := localMemory()
	if err != nil {
		return err, err
	}
	p.MinAvailable = m.Available
	p.MaxSwapUsed = qualSwapUsed(m.SwapTotal, m.SwapFree)
	initialSwap := p.MaxSwapUsed
	if m.Available < 10*trialGiB {
		err = errors.New("host below 10 GiB available")
		return err, err
	}
	phase, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-phase.Done():
				done <- nil
				return
			case <-tick.C:
				s, e := localMemory()
				if e == nil {
					p.MinAvailable = min(p.MinAvailable, s.Available)
					p.MaxSwapUsed = max(p.MaxSwapUsed, qualSwapUsed(s.SwapTotal, s.SwapFree))
					if s.Available < 10*trialGiB || (s.Available < 15*trialGiB && p.MaxSwapUsed > initialSwap+256*(1<<20)) {
						e = errors.New("host memory pressure guard tripped")
					}
				}
				if e != nil {
					cancel()
					done <- e
					return
				}
			}
		}
	}()
	err = benchPhase(benchFields(phase, map[string]any{"tier": p.Tier, "server_context": p.Server}), p.Stage, run)
	cancel()
	guard := <-done
	return errors.Join(err, guard), guard
}

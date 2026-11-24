package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/walterra/spark-roster/internal/host"
	"github.com/walterra/spark-roster/internal/trial"
)

const (
	inklingImage    = "lmsysorg/sglang@sha256:bc811ac7ac68c6fca116a49750baca817b8613a30abface8a347682b55d132e3"
	inklingRevision = "b85c3cfa615fc413cc9af3379f491b963f3b586a"
	inklingRepo     = "thinkingmachines/Inkling-2-Mini"
	inklingName     = "spark-roster-inkling-mini"
	inklingModel    = "inkling-2-mini"
	inklingPort     = 9932
	stockImage      = "sha256:25c2f6220187a810411d93ad8a3f04ee3ed954c1bee9c62317a585794b177a3d"
	inklingLabel    = "io.spark-roster.trial=inkling-2-mini"
)

// runInkling runs a supervised, single-device trial. All Docker commands run
// as the existing operator; only stock systemd lifecycle calls require sudo.
func runInkling(args []string, out, diag io.Writer) int {
	if len(args) != 1 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(out, "Usage: spark-roster inkling stage|switch|status|restore")
		if len(args) == 1 {
			return 0
		}
		return 2
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" || os.Geteuid() == 0 {
		fmt.Fprintln(diag, "error: run on Linux ARM64 as the unprivileged operator")
		return 1
	}
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintln(diag, err)
		return 1
	}
	// Share the existing Pi Q8 operation lock to avoid two simultaneous switches.
	shared := filepath.Join(home, ".local", "share", "spark-roster", "pi-q8")
	if err = os.MkdirAll(shared, 0700); err != nil {
		fmt.Fprintln(diag, err)
		return 1
	}
	f, err := os.OpenFile(filepath.Join(shared, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		fmt.Fprintln(diag, err)
		return 1
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		fmt.Fprintln(diag, "error: another workload trial operation is running")
		return 1
	}
	base := filepath.Join(home, ".local", "share", "spark-roster", "inkling-2-mini", inklingRevision)
	limit := 20 * time.Minute
	if args[0] == "stage" {
		limit = 55 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	switch args[0] {
	case "stage":
		err = stageInkling(ctx, base, out)
	case "switch":
		err = switchInkling(ctx, home, base, out)
	case "restore":
		if err = command(ctx, out, "", "sudo", "-v"); err == nil {
			err = restoreInkling(ctx, home, out)
		}
	case "status":
		var state string
		state, err = inklingState(ctx)
		if err == nil {
			fmt.Fprintln(out, "Inkling container:", state)
			if state == "running" {
				p, c := context.WithTimeout(ctx, 20*time.Second)
				defer c()
				err = probeInkling(p)
				if err == nil {
					fmt.Fprintln(out, "Inkling generation: verified")
				}
			}
		}
	default:
		fmt.Fprintln(diag, "error: unknown Inkling operation")
		return 2
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	return 0
}

func stageInkling(ctx context.Context, base string, out io.Writer) error {
	if err := os.MkdirAll(base, 0700); err != nil {
		return err
	}
	s, err := host.ReadStorage(base)
	if err != nil {
		return err
	}
	// Conservative local preflight for compressed image, extracted layers and weights.
	if s.Available < 100*trialGiB {
		return fmt.Errorf("Inkling staging requires at least 100 GiB available on the model filesystem; check Docker storage separately if mounted elsewhere")
	}
	if err := command(ctx, out, "", "docker", "pull", "--platform=linux/arm64", inklingImage); err != nil {
		return err
	}
	if err := checkInklingImage(ctx); err != nil {
		return err
	}
	if err := command(ctx, out, "", "docker", "run", "--rm", "--pull=never", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-e", "HF_HOME=/tmp/inkling-hf", "--mount", "type=bind,src="+base+",dst=/model", "--entrypoint", "python3", inklingImage, "-c", "from huggingface_hub import snapshot_download; snapshot_download(repo_id='"+inklingRepo+"', revision='"+inklingRevision+"', local_dir='/model', max_workers=4)"); err != nil {
		return err
	}
	if err := checkInklingFiles(base); err != nil {
		return err
	}
	if err := verifiedFile(filepath.Join(base, "model-00001-of-00001.safetensors"), "65e60f51aa1284d4fb022584908c315270334d991828bb774af60584523539d1", 19526019168); err != nil {
		return err
	}
	if err := verifiedFile(filepath.Join(base, "mtp.safetensors"), "7c3b651317f01f73bcf4e1dceba66ef8b7e1a1eac6bc171e882e690a4b10a1d2", 635801416); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(base, ".staged-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = fmt.Fprintln(tmp, inklingRevision)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), filepath.Join(base, "staged")); err != nil {
		return err
	}
	fmt.Fprintln(out, "Inkling stage verified: pinned ARM64 image present, pinned snapshot downloaded and weight SHA-256 values verified. Stock unchanged.")
	return nil
}
func checkInklingImage(ctx context.Context) error {
	arch, err := output(ctx, "docker", "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", inklingImage)
	if err != nil || arch != "linux/arm64" {
		return fmt.Errorf("pinned Inkling ARM64 image unavailable; run inkling stage")
	}
	return nil
}
func checkInklingFiles(base string) error {
	for name, size := range map[string]int64{"model-00001-of-00001.safetensors": 19526019168, "mtp.safetensors": 635801416, "config.json": 2722, "tokenizer.json": 26178678, "chat_template.jinja": 6868, "model.safetensors.index.json": 109300, "hf_quant_config.json": 67924, "tokenizer_config.json": 13235, "processor_config.json": 461} {
		s, err := os.Lstat(filepath.Join(base, name))
		if err != nil || !s.Mode().IsRegular() || s.Size() != size {
			return fmt.Errorf("missing or wrong-sized pinned Inkling file %s", name)
		}
	}
	return nil
}
func inklingState(ctx context.Context) (string, error) {
	// Verify daemon access with list first; an inspect failure is not proof of absence.
	ids, err := output(ctx, "docker", "ps", "-a", "--filter", "name=^/"+inklingName+"$", "--format", "{{.ID}}")
	if err != nil {
		return "", err
	}
	if ids == "" {
		return "absent", nil
	}
	s, err := output(ctx, "docker", "inspect", "--type=container", "--format", `{{index .Config.Labels "io.spark-roster.trial"}} {{.State.Status}}`, inklingName)
	if err != nil {
		return "", err
	}
	a := strings.Fields(s)
	if len(a) != 2 || a[0] != inklingModel {
		return "", fmt.Errorf("candidate container name is occupied by an unmanaged workload")
	}
	return a[1], nil
}
func stopInkling(ctx context.Context, out io.Writer) error {
	s, err := inklingState(ctx)
	if err != nil {
		return err
	}
	if s == "absent" {
		return nil
	}
	if s != "exited" && s != "created" {
		if err := command(ctx, out, "", "docker", "stop", "--time", "20", inklingName); err != nil {
			return err
		}
	}
	if err := command(ctx, out, "", "docker", "rm", inklingName); err != nil {
		return err
	}
	if s, err := inklingState(ctx); err != nil || s != "absent" {
		return fmt.Errorf("Inkling container exit not confirmed")
	}
	return nil
}
func probeInkling(ctx context.Context) error {
	return trial.CheckGenerationTokens(ctx, inklingPort, inklingModel, "", 1024)
}

func switchInkling(ctx context.Context, home, base string, out io.Writer) (err error) {
	if err := confirmedInactive(ctx, piUnit); err != nil {
		return err
	}
	if s, err := inklingState(ctx); err != nil || s != "absent" {
		return fmt.Errorf("Inkling container is not confirmed absent: %s: %v", s, err)
	}
	if err := checkInklingImage(ctx); err != nil {
		return err
	}
	marker, err := os.ReadFile(filepath.Join(base, "staged"))
	if err != nil || strings.TrimSpace(string(marker)) != inklingRevision {
		return fmt.Errorf("run inkling stage before switching")
	}
	if err = checkInklingFiles(base); err != nil {
		return err
	}
	l, err := net.Listen("tcp4", "127.0.0.1:9932")
	if err != nil {
		return fmt.Errorf("candidate port not free: %w", err)
	}
	l.Close()
	p, c := context.WithTimeout(ctx, 15*time.Second)
	err = stockProbe(p, home)
	c()
	if err != nil {
		return fmt.Errorf("stock generation preflight: %w", err)
	}
	image, err := output(ctx, "docker", "container", "inspect", "--format", "{{.Image}} {{.State.Running}}", "qwen38-sglang")
	if err != nil || image != stockImage+" true" {
		return fmt.Errorf("stock restore image does not match verified baseline")
	}
	if err = authorizeTrial(ctx, out); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			r, c := context.WithTimeout(context.Background(), 15*time.Minute)
			defer c()
			if e := restoreInkling(r, home, out); e != nil {
				err = errors.Join(err, fmt.Errorf("stock restore failed; run inkling restore: %w", e))
			}
		}
	}()
	for i := len(stockUnits) - 1; i >= 0; i-- {
		if err = command(ctx, out, "", "sudo", "-n", "systemctl", "stop", stockUnits[i]); err != nil {
			return err
		}
		if err = confirmedInactive(ctx, stockUnits[i]); err != nil {
			return err
		}
	}
	return launchInkling(ctx, base, 32768, out)
}

// launchInkling also supports benchmark handoffs, where stock is already stopped.
func launchInkling(ctx context.Context, base string, window int, out io.Writer) error {
	if err := confirmedInactive(ctx, piUnit); err != nil {
		return err
	}
	for _, unit := range stockUnits {
		if err := confirmedInactive(ctx, unit); err != nil {
			return err
		}
	}
	if s, err := inklingState(ctx); err != nil || s != "absent" {
		return fmt.Errorf("Inkling container is not confirmed absent: %s: %v", s, err)
	}
	running, err := output(ctx, "docker", "ps", "--filter", "name=^/qwen38-sglang$", "--format", "{{.Names}}")
	if err != nil || running != "" {
		return fmt.Errorf("stock Docker exit not confirmed")
	}
	if err = requireRAM(75 * trialGiB); err != nil {
		return err
	}
	// Track the lowest observed MemAvailable from just before launch. This is
	// host-wide sampled pressure, not exact model attribution or an OOM limit.
	m, err := localMemory()
	if err != nil {
		return err
	}
	var minimum atomic.Uint64
	minimum.Store(m.Available)
	startup, stop := context.WithTimeout(ctx, inklingStartupBudget)
	defer stop()
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-startup.Done():
				return
			case <-ticker.C:
				sample, e := localMemory()
				if e != nil {
					stop()
					return
				}
				if sample.Available < minimum.Load() {
					minimum.Store(sample.Available)
				}
				if sample.Available < 10*trialGiB {
					stop()
					return
				}
			}
		}
	}()
	defer func() { stop(); <-sampled }()
	start := time.Now()
	if err = command(startup, out, "", "docker", "run", "--detach", "--pull=never", "--name", inklingName, "--label", inklingLabel, "--gpus", "all", "--network", "host", "--shm-size", "8g", "--memory", "96g", "--memory-swap", "96g", "--security-opt", "no-new-privileges", "--env", "MAX_JOBS=4", "--env", "HF_HUB_OFFLINE=1", "--mount", "type=bind,src="+base+",dst=/model,readonly", "--entrypoint", "sglang", inklingImage, "serve", "--model-path", "/model", "--served-model-name", inklingModel, "--tp", "1", "--ep", "1", "--attention-backend", "fa4", "--dtype", "bfloat16", "--page-size", "128", "--mem-fraction-static", "0.45", "--grammar-backend", "xgrammar", "--reasoning-parser", "inkling", "--tool-call-parser", "inkling2", "--context-length", fmt.Sprint(window), "--max-running-requests", "1", "--enable-metrics", "--host", "127.0.0.1", "--port", "9932"); err != nil {
		return err
	}
	if err = waitGeneration(startup, probeInkling, 10*trialGiB); err != nil {
		return err
	}
	stop()
	<-sampled
	fmt.Fprintf(out, "Inkling generation verified in %s at http://127.0.0.1:%d/v1 (model %s). Minimum sampled host MemAvailable %.2f GiB; decline from prelaunch %.2f GiB. Stock stopped; run inkling restore after testing.\n", time.Since(start).Round(time.Millisecond), inklingPort, inklingModel, gib(int64(minimum.Load())), gib(int64(m.Available-minimum.Load())))
	return nil
}
func restoreInkling(ctx context.Context, home string, out io.Writer) error {
	if err := stopInkling(ctx, out); err != nil {
		return err
	}
	if err := confirmedInactive(ctx, piUnit); err != nil {
		return err
	}
	return restoreAuthorized(ctx, home, out)
}

package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/walterra/spark-roster/internal/host"
	"github.com/walterra/spark-roster/internal/trial"
)

const (
	piBuildDir    = "build-spark-roster-headless"
	llamaRevision = "b1b770d90d94cad9e9ea5b993bdbc383a522fb28"
	piModelURL    = "https://huggingface.co/harness-hub/Qwen3.8-27B-pi-adapted-GGUF/resolve/19980ce556b41c7b73818083c9adf4b3fa0c72e5/Qwen3.8-27B-pi-Q8_0.gguf"
	piModelSHA    = "bcb2b967901e772e76842bee0c67bc92f27c819c0ce7f14642e17d64aadd4d21"
	piModelBytes  = 28595763840
	piUnit        = "spark-roster-pi-q8.service"
	piPort        = 9931
	trialGiB      = uint64(1 << 30)
)

var stockUnits = []string{"qwen38-sglang.service", "qwen38-keepalive.service"}

func command(ctx context.Context, dst io.Writer, dir, name string, args ...string) error {
	var action string
	switch {
	case name == "systemctl" && len(args) >= 2:
		action = args[0] + " " + args[1]
	case name == "sudo" && len(args) >= 4 && args[1] == "systemctl":
		action = args[2] + " " + args[3]
	case name == "sudo" && len(args) >= 2 && args[1] == "systemd-run":
		action = "launch pi-q8"
	case name == "docker" && len(args) > 0 && (args[0] == "run" || args[0] == "stop" || args[0] == "rm"):
		action = "docker " + args[0]
	}
	if action != "" {
		return benchPhase(benchFields(ctx, map[string]any{"command": name, "action": action}), "service.command", func(c context.Context) error {
			return runCommand(c, dst, dir, name, args...)
		})
	}
	return runCommand(ctx, dst, dir, name, args...)
}

func runCommand(ctx context.Context, dst io.Writer, dir, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdin = os.Stdin
	cmd.Stdout = dst
	cmd.Stderr = dst
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func operatorForHome(home string) (*user.User, error) {
	if os.Geteuid() == 0 {
		u, err := user.Lookup(filepath.Base(home))
		if err != nil || u.HomeDir != home || u.Uid == "0" {
			return nil, fmt.Errorf("invalid benchmark operator home")
		}
		return u, nil
	}
	return user.Current()
}

func authorizeTrial(ctx context.Context, out io.Writer) error {
	if os.Geteuid() == 0 {
		return nil
	}
	return command(ctx, out, "", "sudo", "-v")
}

func output(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "", fmt.Errorf("%s: %w", name, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func verifiedFile(path, sha string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() != size {
		return fmt.Errorf("unexpected file type or size: %s", path)
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return err
	}
	if fmt.Sprintf("%x", h.Sum(nil)) != sha {
		return fmt.Errorf("SHA-256 mismatch: %s", path)
	}
	return nil
}

func runPiQ8(args []string, out, diag io.Writer) int {
	fs := flag.NewFlagSet("pi-q8", flag.ContinueOnError)
	fs.SetOutput(diag)
	if len(args) != 1 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(out, "Usage: spark-roster pi-q8 stage|stage-status|switch|restore|status")
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
	base := filepath.Join(home, ".local", "share", "spark-roster", "pi-q8")
	if args[0] == "stage-status" {
		exe, err := os.Executable()
		if err == nil {
			var f *os.File
			f, err = os.Open(filepath.Join(filepath.Dir(exe), "stage.log"))
			if err == nil {
				defer f.Close()
				var st os.FileInfo
				st, err = f.Stat()
				if err == nil {
					_, err = f.Seek(max(0, st.Size()-4096), io.SeekStart)
				}
				if err == nil {
					_, err = io.Copy(out, io.LimitReader(f, 4096))
				}
			}
		}
		if err != nil {
			fmt.Fprintln(diag, err)
			return 1
		}
		return 0
	}
	if err = os.MkdirAll(base, 0700); err != nil {
		fmt.Fprintln(diag, err)
		return 1
	}
	lock, err := os.OpenFile(filepath.Join(base, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		fmt.Fprintln(diag, err)
		return 1
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		fmt.Fprintln(diag, "error: another Pi Q8 operation is running")
		return 1
	}
	timeout := 20 * time.Minute
	if args[0] == "stage" {
		timeout = 45 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	switch args[0] {
	case "stage":
		err = stagePiQ8(ctx, base, out)
	case "switch":
		err = switchPiQ8(ctx, home, base, 2*time.Hour, out)
	case "restore":
		err = restorePiQ8(ctx, home, out)
	case "status":
		state, e := piUnitState(ctx)
		err = e
		if err == nil {
			fmt.Fprintf(out, "Pi Q8 unit state: %s\n", state)
			if state == "active" {
				p, c := context.WithTimeout(ctx, 15*time.Second)
				defer c()
				err = trial.CheckGeneration(p, piPort, "qwen3.8-pi", "")
				if err == nil {
					fmt.Fprintln(out, "generation: verified")
				}
			}
		}
	default:
		fmt.Fprintln(diag, "error: unknown pi-q8 operation")
		return 2
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	return 0
}

func stagePiQ8(ctx context.Context, base string, out io.Writer) error {
	src := filepath.Join(base, "llama-"+llamaRevision)
	build := filepath.Join(src, piBuildDir)
	weights := filepath.Join(base, "Qwen3.8-27B-pi-Q8_0.gguf")
	cuda := os.Getenv("CUDACXX")
	if cuda == "" {
		cuda = "/usr/local/cuda/bin/nvcc"
	}
	if _, err := os.Stat(cuda); err != nil {
		return fmt.Errorf("CUDA compiler unavailable at %s", cuda)
	}
	if err := os.MkdirAll(src, 0700); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(src, ".git")); os.IsNotExist(err) {
		if err := command(ctx, out, src, "git", "init", "--quiet"); err != nil {
			return err
		}
	}
	head, _ := output(ctx, "git", "-C", src, "rev-parse", "HEAD")
	if head != llamaRevision {
		if err := command(ctx, out, src, "git", "fetch", "--depth=1", "https://github.com/ggml-org/llama.cpp.git", llamaRevision); err != nil {
			return err
		}
		if err := command(ctx, out, src, "git", "checkout", "--detach", llamaRevision); err != nil {
			return err
		}
	}
	if err := command(ctx, out, src, "git", "diff", "--quiet", llamaRevision, "--"); err != nil {
		return fmt.Errorf("pinned llama.cpp tree has tracked modifications: %w", err)
	}
	if err := command(ctx, out, src, "cmake", "-S", src, "-B", build, "-DGGML_CUDA=ON", "-DCMAKE_CUDA_ARCHITECTURES=121a-real", "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_CUDA_COMPILER="+cuda, "-DLLAMA_BUILD_TESTS=OFF", "-DLLAMA_BUILD_EXAMPLES=OFF", "-DLLAMA_BUILD_UI=OFF", "-DLLAMA_USE_PREBUILT_UI=OFF"); err != nil {
		return err
	}
	if err := command(ctx, out, src, "cmake", "--build", build, "--target", "llama-server", "--parallel", "4"); err != nil {
		return err
	}
	if _, err := os.Stat(weights); os.IsNotExist(err) {
		temp := weights + ".partial"
		var offset int64
		if s, e := os.Stat(temp); e == nil {
			offset = s.Size()
		}
		if offset < piModelBytes {
			space, err := host.ReadStorage(base)
			if err != nil {
				return err
			}
			if space.Available < uint64(piModelBytes-offset)+5*trialGiB {
				return fmt.Errorf("insufficient local storage for Pi Q8 plus reserve")
			}
			if err := command(ctx, out, base, "curl", "--fail", "--location", "--silent", "--show-error", "--retry", "3", "--continue-at", "-", "--output", temp, piModelURL); err != nil {
				return err
			}
		}
		if err := verifiedFile(temp, piModelSHA, piModelBytes); err != nil {
			return err
		}
		if err := os.Rename(temp, weights); err != nil {
			return err
		}
	}
	if err := verifiedFile(weights, piModelSHA, piModelBytes); err != nil {
		return err
	}
	if err := command(ctx, out, base, filepath.Join(build, "bin", "llama-server"), "--version"); err != nil {
		return err
	}
	fmt.Fprintln(out, "Pi Q8 stage verified: pinned CUDA build and GGUF checksum. Stock Qwen unchanged.")
	return nil
}

func stockProbe(ctx context.Context, home string) error {
	key, err := os.ReadFile(filepath.Join(home, ".config", "qwen38", "api-key"))
	if err != nil {
		return fmt.Errorf("read stock canary key: %w", err)
	}
	return trial.CheckGeneration(ctx, 30001, "qwen3.8-27b", strings.TrimSpace(string(key)))
}
func waitGeneration(ctx context.Context, probe func(context.Context) error, floor uint64) error {
	for {
		if floor > 0 {
			if err := requireRAM(floor); err != nil {
				return err
			}
		}
		p, c := context.WithTimeout(ctx, 8*time.Second)
		err := probe(p)
		c()
		if err == nil {
			if floor > 0 {
				return requireRAM(floor)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("generation readiness: %w", ctx.Err())
		case <-time.After(4 * time.Second):
		}
	}
}
func requireRAM(minimum uint64) error {
	m, err := localMemory()
	if err != nil {
		return err
	}
	if m.Available < minimum {
		return fmt.Errorf("available RAM %.2f GiB below %.2f GiB trial floor", gib(int64(m.Available)), gib(int64(minimum)))
	}
	return nil
}
func piUnitState(ctx context.Context) (string, error) {
	s, err := output(ctx, "systemctl", "list-units", "--all", "--plain", "--no-legend", "--no-pager", piUnit)
	if err != nil {
		return "", err
	}
	if s == "" {
		return "inactive", nil
	}
	f := strings.Fields(s)
	if len(f) < 4 || f[0] != piUnit || strings.Contains(s, "\n") {
		return "", fmt.Errorf("unexpected candidate unit state")
	}
	return f[2], nil
}

func confirmedInactive(ctx context.Context, unit string) error {
	var state string
	var err error
	if unit == piUnit {
		state, err = piUnitState(ctx)
	} else {
		state, err = output(ctx, "systemctl", "show", "-p", "ActiveState", "--value", unit)
	}
	if err != nil {
		return err
	}
	if state != "inactive" && state != "failed" {
		return fmt.Errorf("%s is %s, not stopped", unit, state)
	}
	return nil
}
func restorePiQ8(ctx context.Context, home string, out io.Writer) error {
	if err := command(ctx, out, "", "sudo", "-v"); err != nil {
		return err
	}
	return restoreAuthorized(ctx, home, out)
}
func stopPiQ8(ctx context.Context, out io.Writer) error {
	state, err := piUnitState(ctx)
	if err != nil {
		return err
	}
	if state != "inactive" && state != "failed" {
		if err := command(ctx, out, "", "sudo", "-n", "systemctl", "stop", piUnit); err != nil {
			return err
		}
	}
	return confirmedInactive(ctx, piUnit)
}

func restoreAuthorized(ctx context.Context, home string, out io.Writer) error {
	if err := stopPiQ8(ctx, out); err != nil {
		return err
	}
	p, c := context.WithTimeout(ctx, 12*time.Second)
	err := stockProbe(p, home)
	c()
	if err == nil {
		fmt.Fprintln(out, "Stock Qwen generation verified; Pi Q8 stopped.")
		return nil
	}
	// Never credit memory reclaim merely from stopping a unit. If the engine is
	// already active, starting its proxy does not require loading a second copy.
	state, err := output(ctx, "systemctl", "show", "-p", "ActiveState", "--value", stockUnits[0])
	if err != nil {
		return err
	}
	if state != "active" && state != "activating" {
		if err := requireRAM(90 * trialGiB); err != nil {
			return err
		}
	}
	for _, unit := range stockUnits {
		if err := command(ctx, out, "", "sudo", "-n", "systemctl", "start", unit); err != nil {
			return err
		}
	}
	if err := waitGeneration(ctx, func(c context.Context) error { return stockProbe(c, home) }, 0); err != nil {
		return err
	}
	fmt.Fprintln(out, "Stock Qwen restored; authenticated generation verified.")
	return nil
}
func switchPiQ8(ctx context.Context, home, base string, lifetime time.Duration, out io.Writer) (err error) {
	u, e := operatorForHome(home)
	if e != nil {
		return e
	}
	exe := filepath.Join(base, "llama-"+llamaRevision, piBuildDir, "bin", "llama-server")
	weights := filepath.Join(base, "Qwen3.8-27B-pi-Q8_0.gguf")
	if info, e := os.Stat(exe); e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0100 == 0 {
		return fmt.Errorf("candidate not built; run pi-q8 stage")
	}
	if e := verifiedFile(weights, piModelSHA, piModelBytes); e != nil {
		return fmt.Errorf("candidate model: %w", e)
	}
	if e := confirmedInactive(ctx, piUnit); e != nil {
		return e
	}
	listener, e := net.Listen("tcp4", "127.0.0.1:9931")
	if e != nil {
		return fmt.Errorf("candidate port not free: %w", e)
	}
	listener.Close()
	p, c := context.WithTimeout(ctx, 15*time.Second)
	e = stockProbe(p, home)
	c()
	if e != nil {
		return fmt.Errorf("stock restore preflight: %w", e)
	}
	if e = authorizeTrial(ctx, out); e != nil {
		return e
	}
	defer func() {
		if err != nil {
			r, c := context.WithTimeout(context.Background(), 12*time.Minute)
			defer c()
			if e := restoreAuthorized(r, home, out); e != nil {
				err = fmt.Errorf("%w; stock restore failed: %v; run pi-q8 restore from a fresh session", err, e)
			}
		}
	}()
	for i := len(stockUnits) - 1; i >= 0; i-- {
		if e := command(ctx, out, "", "sudo", "-n", "systemctl", "stop", stockUnits[i]); e != nil {
			return e
		}
		if e := confirmedInactive(ctx, stockUnits[i]); e != nil {
			return e
		}
	}
	running, e := output(ctx, "docker", "ps", "--filter", "name=^/qwen38-sglang$", "--format", "{{.Names}}")
	if e != nil || running != "" {
		return fmt.Errorf("stock Docker exit not confirmed")
	}
	return launchPiQ8(ctx, u, exe, weights, lifetime, 32768, out)
}

func launchPiQ8(ctx context.Context, u *user.User, exe, weights string, lifetime time.Duration, window int, out io.Writer) error {
	if err := qualCandidatesClear(ctx); err != nil {
		return err
	}
	if e := requireRAM(65 * trialGiB); e != nil {
		return e
	}
	if e := command(ctx, out, "", "sudo", "-n", "systemd-run", "--unit="+piUnit, "--uid="+u.Uid, "--gid="+u.Gid, "--property=MemoryMax=48G", "--property=MemorySwapMax=0", fmt.Sprintf("--property=RuntimeMaxSec=%ds", int64(lifetime/time.Second)), "--property=KillMode=control-group", "--property=Type=exec", "--", exe, "--model", weights, "--alias", "qwen3.8-pi", "--host", "127.0.0.1", "--port", "9931", "--ctx-size", fmt.Sprint(window), "--parallel", "1", "--gpu-layers", "all", "--jinja", "--reasoning", "off"); e != nil {
		return e
	}
	startup, stop := context.WithTimeout(ctx, 4*time.Minute)
	defer stop()
	if e := waitGeneration(startup, func(p context.Context) error { return trial.CheckGeneration(p, piPort, "qwen3.8-pi", "") }, 10*trialGiB); e != nil {
		return e
	}
	fmt.Fprintf(out, "Pi Q8 generation verified at http://127.0.0.1:9931/v1 (model qwen3.8-pi). Stock Qwen stopped. Candidate lifetime: %s. Expiry alone does not restore stock.\n", lifetime)
	return nil
}

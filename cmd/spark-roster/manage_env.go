//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/walterra/spark-roster/internal/host"
	"github.com/walterra/spark-roster/internal/profile"
)

func realEnv() (env, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return env{}, err
	}
	u, err := user.Current()
	if err != nil {
		return env{}, err
	}
	g, err := user.LookupGroupId(u.Gid)
	if err != nil {
		return env{}, err
	}
	st, _ := os.Stdin.Stat()
	return env{
		home: home, getenv: os.Getenv, stdin: os.Stdin,
		interactive: st != nil && st.Mode()&os.ModeCharDevice != 0,
		user:        u.Username, group: g.Name, goos: runtime.GOOS, goarch: runtime.GOARCH,
		backend: func(p paths) (hostOps, stager) {
			h := profile.NewHost(p.HFCache)
			s := &profile.Stager{
				HFCache: p.HFCache, Run: runIn, Output: h.Output, Stat: os.Stat,
				Available: freeBytes, Download: profile.HTTPDownload, ReserveBytes: 20 << 30,
				Log: func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...) },
			}
			return h, s
		},
		portFree: portFree, lock: lockDir, unitInstalled: unitInstalled,
		euid: os.Geteuid(), now: time.Now,
		exe: func() (string, error) {
			p, err := os.Executable()
			if err != nil {
				return "", err
			}
			return filepath.EvalSymlinks(p)
		},
		sha256: fileSHA256,
		chown:  os.Lchown,
		lookupUser: func(name string) (OperatorInfo, error) {
			u, err := user.Lookup(name)
			if err != nil {
				return OperatorInfo{}, err
			}
			uid, e1 := strconv.Atoi(u.Uid)
			gid, e2 := strconv.Atoi(u.Gid)
			if e1 != nil || e2 != nil {
				return OperatorInfo{}, fmt.Errorf("unexpected uid or gid for %s", name)
			}
			return OperatorInfo{Home: u.HomeDir, UID: uid, GID: gid}, nil
		},
		output: func(ctx context.Context, name string, args ...string) (string, error) {
			out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
			return strings.TrimSpace(string(out)), err
		},
		available: freeBytes,
		memory: func(context.Context) (uint64, error) {
			m, err := localMemory()
			return m.Available, err
		},
	}, nil
}

func runIn(ctx context.Context, dir string, argv ...string) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", argv[0], err)
	}
	return nil
}

func freeBytes(path string) (uint64, error) {
	s, err := host.ReadStorage(path)
	return s.Available, err
}

// portFree reports an error when something already listens on the port.
func portFree(port uint16) error {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	return l.Close()
}

// lockDir takes the exclusive operation lock. When the older pi-q8 and inkling
// commands have created their lock under the same data directory it is taken as
// well, so the two interfaces can never switch at the same time.
func lockDir(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	var held []*os.File
	release := func() {
		for _, f := range held {
			f.Close()
		}
	}
	for _, p := range []string{filepath.Join(dir, "lock"), filepath.Join(dir, "pi-q8", "lock")} {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
		if err != nil {
			if os.IsNotExist(err) && strings.HasSuffix(filepath.Dir(p), "pi-q8") {
				continue
			}
			release()
			return nil, err
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
			release()
			return nil, fmt.Errorf("another SparkRoster operation is running (lock %s)", p)
		}
		held = append(held, f)
	}
	return release, nil
}

// unitInstalled reports whether /etc/systemd/system holds exactly this unit.
func unitInstalled(name, rendered string) (bool, error) {
	b, err := os.ReadFile("/etc/systemd/system/" + name)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return bytes.Equal(b, []byte(rendered)), nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

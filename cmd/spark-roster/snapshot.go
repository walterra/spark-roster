package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/walterra/spark-roster/internal/host"
)

func localMemory() (host.Memory, error) {
	if runtime.GOOS != "linux" {
		return host.Memory{}, errors.New("live snapshot requires Linux")
	}
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return host.Memory{}, err
	}
	defer f.Close()
	return host.ReadMemory(f, time.Now())
}

func runSnapshot(args []string, out, diag io.Writer, inspect func(context.Context, string) (host.Unit, error), memory func() (host.Memory, error)) int {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	fs.SetOutput(diag)
	path := fs.String("inventory", "", "v1 systemd inventory JSON file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *path == "" || fs.NArg() != 0 {
		fmt.Fprintln(diag, "error: snapshot requires --inventory FILE; output is host_snapshot JSON")
		return 2
	}
	var inventory host.Inventory
	if err := readJSON(*path, &inventory); err != nil {
		fmt.Fprintf(diag, "error: %s: %v\n", *path, err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), host.SnapshotTimeout)
	defer cancel()
	snapshot, err := host.CaptureSnapshot(ctx, inventory, inspect, memory)
	if err == nil {
		e := json.NewEncoder(out)
		e.SetIndent("", "  ")
		err = e.Encode(snapshot)
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	return 0
}

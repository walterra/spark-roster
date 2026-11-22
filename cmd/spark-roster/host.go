package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/walterra/spark-roster/internal/host"
)

func runMemory(args []string, out, diag io.Writer, now time.Time) int {
	fs := flag.NewFlagSet("memory", flag.ContinueOnError)
	fs.SetOutput(diag)
	path := fs.String("meminfo", "/proc/meminfo", "Linux meminfo file (or an offline fixture)")
	jsonMode := fs.Bool("json", false, "write a versioned JSON observation")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 || *path == "" {
		fmt.Fprintln(diag, "error: memory expects no positional arguments and a non-empty meminfo path")
		return 2
	}
	f, err := os.Open(*path)
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	defer f.Close()
	m, err := host.ReadMemory(f, now)
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	return writeObservation(out, diag, *jsonMode, m, func() error {
		_, err := fmt.Fprintf(out, "Memory available: %.2f GiB / %.2f GiB\nNo service inventory collected. Swap is not included in available memory.\n", gib(int64(m.Available)), gib(int64(m.Total)))
		return err
	})
}

func runUnit(args []string, out, diag io.Writer, draft bool) int {
	name := "inspect"
	if draft {
		name = "profile draft"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(diag)
	unit := fs.String("unit", "", "literal .service unit name")
	show := fs.String("from-show", "", "read offline systemctl show data instead of querying the local host")
	jsonMode := fs.Bool("json", false, "write a versioned JSON result")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 || !host.ValidService(*unit) {
		fmt.Fprintln(diag, "error: supply a literal .service name with --unit")
		return 2
	}
	var u host.Unit
	var err error
	if *show != "" {
		f, openErr := os.Open(*show)
		if openErr != nil {
			fmt.Fprintf(diag, "error: %v\n", openErr)
			return 1
		}
		defer f.Close()
		u, err = host.ReadUnit(*unit, f)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		u, err = host.InspectUnit(ctx, *unit)
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	var value any = u
	var required []string
	if draft {
		if u.LoadState != "loaded" {
			fmt.Fprintln(diag, "error: profile drafting requires a loaded service")
			return 1
		}
		d := u.Draft()
		value, required = d, d.RequiredInputs
	}
	return writeObservation(out, diag, *jsonMode, value, func() error {
		var b strings.Builder
		fmt.Fprintf(&b, "%s: %s / %s\n", u.Unit, u.LoadState, u.ActiveState)
		if u.UnitFileState != "" {
			fmt.Fprintf(&b, "Unit file: %s\n", u.UnitFileState)
		}
		if u.MemoryCurrent != nil {
			fmt.Fprintf(&b, "Cgroup memory current: %.2f GiB\n", gib(int64(*u.MemoryCurrent)))
		}
		if u.MemoryMax != nil {
			fmt.Fprintf(&b, "Unit memory max: %.2f GiB\n", gib(int64(*u.MemoryMax)))
		} else if u.MemoryMaxUnlimited {
			b.WriteString("Unit memory max: unlimited (parent limits not inspected)\n")
		}
		if draft {
			fmt.Fprintf(&b, "Draft only; still required: %s\n", strings.Join(required, ", "))
		}
		_, err := io.WriteString(out, b.String())
		return err
	})
}

func writeObservation(out, diag io.Writer, jsonMode bool, value any, human func() error) int {
	var err error
	if jsonMode {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		err = enc.Encode(value)
	} else {
		err = human()
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	return 0
}

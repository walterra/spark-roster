package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/walterra/spark-roster/internal/planner"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, time.Now())) }

func run(args []string, out, diag io.Writer, now time.Time) int {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(out, "Usage: spark-roster plan --catalog FILE --deployment-set FILE --snapshot FILE [--as-of RFC3339] [--json]")
		return 0
	}
	if args[0] != "plan" {
		fmt.Fprintf(diag, "error: unknown command %q\n", args[0])
		return 2
	}
	flags := flag.NewFlagSet("plan", flag.ContinueOnError)
	flags.SetOutput(diag)
	catalogFile := flags.String("catalog", "", "v1 model catalog JSON file")
	setFile := flags.String("deployment-set", "", "v1 desired deployment set JSON file")
	snapshotFile := flags.String("snapshot", "", "v1 host snapshot JSON file")
	asOf := flags.String("as-of", "", "evaluation time in RFC3339, for offline replay (default: now)")
	asJSON := flags.Bool("json", false, "write a versioned JSON result instead of a human report")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *catalogFile == "" || *setFile == "" || *snapshotFile == "" || flags.NArg() != 0 {
		fmt.Fprintln(diag, "error: supply --catalog, --deployment-set, and --snapshot; see plan --help")
		return 2
	}
	if *asOf != "" {
		var err error
		now, err = time.Parse(time.RFC3339, *asOf)
		if err != nil {
			fmt.Fprintln(diag, "error: --as-of must be an RFC3339 timestamp")
			return 2
		}
	}
	var catalog planner.Catalog
	var set planner.DeploymentSet
	var snapshot planner.Snapshot
	for _, input := range []struct {
		path   string
		target any
	}{{*catalogFile, &catalog}, {*setFile, &set}, {*snapshotFile, &snapshot}} {
		if err := readJSON(input.path, input.target); err != nil {
			fmt.Fprintf(diag, "error: %s: %v\n", input.path, err)
			return 1
		}
	}
	result, err := planner.Build(catalog, set, snapshot, now)
	if err == nil {
		if *asJSON {
			encoder := json.NewEncoder(out)
			encoder.SetIndent("", "  ")
			err = encoder.Encode(result)
		} else {
			_, err = io.WriteString(out, human(result))
		}
	}
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	return 0
}

func readJSON(path string, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	const limit = 4 << 20
	reader := &io.LimitedReader{R: f, N: limit + 1}
	d := json.NewDecoder(reader)
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected exactly one JSON document")
	}
	if reader.N == 0 {
		return fmt.Errorf("input exceeds 4 MiB")
	}
	return nil
}

func gib(n int64) float64 { return float64(n) / (1 << 30) }

func human(r planner.Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s (%s)\n", strings.ToUpper(r.Verdict), r.DeploymentSet, r.ReasonCode)
	fmt.Fprintf(&b, "Available %.2f GiB; reserve %.2f GiB; snapshot %s\n", gib(r.AvailableBytes), gib(r.ReserveBytes), r.SnapshotAt.Format(time.RFC3339))
	if len(r.Protected) > 0 {
		fmt.Fprintf(&b, "Keep online: %s\n", strings.Join(r.Protected, ", "))
	}
	for _, p := range r.Paths {
		fmt.Fprintf(&b, "%s:\n", p.Name)
		if len(p.StopBeforeLaunch) > 0 {
			fmt.Fprintf(&b, "  stop before launch: %s\n", strings.Join(p.StopBeforeLaunch, ", "))
		}
		for _, row := range p.Phases {
			if row.HeadroomBytes == nil {
				fmt.Fprintf(&b, "  %s: unknown\n", row.Name)
				continue
			}
			fmt.Fprintf(&b, "  %s: %.2f GiB added demand, %+.2f GiB after reserve\n", row.Name, gib(*row.AdditionalDemandBytes), gib(*row.HeadroomBytes))
		}
		for _, reason := range p.Reasons {
			fmt.Fprintf(&b, "  %s", strings.ReplaceAll(reason.Code, "_", " "))
			if reason.Subject != "" {
				fmt.Fprintf(&b, ": %s", reason.Subject)
			}
			b.WriteByte('\n')
		}
	}
	if r.Verdict == "stop-first" {
		b.WriteString("Recheck available memory after stopping, before launching the replacement.\n")
	}
	return b.String()
}

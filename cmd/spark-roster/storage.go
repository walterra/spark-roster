package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/walterra/spark-roster/internal/host"
)

func runStorage(args []string, out, diag io.Writer, read func(string) (host.Storage, error)) int {
	fs := flag.NewFlagSet("storage", flag.ContinueOnError)
	fs.SetOutput(diag)
	path := fs.String("path", "", "existing directory on the filesystem to inspect")
	jsonMode := fs.Bool("json", false, "write a versioned JSON observation")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 || *path == "" {
		fmt.Fprintln(diag, "error: supply an existing directory with --path")
		return 2
	}
	s, err := read(*path)
	if err != nil {
		fmt.Fprintf(diag, "error: %v\n", err)
		return 1
	}
	return writeObservation(out, diag, *jsonMode, s, func() error {
		_, err := fmt.Fprintf(out, "Filesystem for %q: %.2f GiB available / %.2f GiB total\n", s.Path, gib(int64(s.Available)), gib(int64(s.Total)))
		return err
	})
}

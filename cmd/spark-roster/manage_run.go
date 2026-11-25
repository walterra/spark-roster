package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// runManage dispatches the profile commands with the real machine behind them.
// It reports false when args[0] is not one of them.
func runManage(args []string, out, diag io.Writer) (int, bool) {
	switch {
	case benchmarkManaged(args):
	case args[0] == "list", args[0] == "status", args[0] == "doctor", args[0] == "stage", args[0] == "switch", args[0] == "stop", args[0] == "recover", args[0] == "version", args[0] == "--version":
	default:
		return 0, false
	}
	if args[0] == "version" || args[0] == "--version" {
		fmt.Fprintln(out, versionString())
		return exitOK, true
	}
	e, err := realEnv()
	if err != nil {
		// list needs no machine access, so it still works off-device.
		if args[0] == "list" {
			e = offlineEnv()
		} else {
			fmt.Fprintf(diag, "error: %v\n", err)
			return exitPrecondition, true
		}
	}
	// A switch has to finish or restore even if the terminal closes: ignore HUP, and turn
	// INT and TERM into a cancelled context, which Switch answers by restoring.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	signal.Ignore(syscall.SIGHUP)
	return manage(ctx, args, out, diag, e)
}

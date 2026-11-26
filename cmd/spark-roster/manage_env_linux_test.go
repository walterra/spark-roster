//go:build linux

package main

import (
	"os"
	"testing"
)

// realEnv must work in the environment systemd gives a transient root service: no $HOME.
func TestRealEnvWorksWithoutHOME(t *testing.T) {
	old, had := os.LookupEnv("HOME")
	os.Unsetenv("HOME")
	t.Cleanup(func() {
		if had {
			os.Setenv("HOME", old)
		}
	})
	e, err := realEnv()
	if err != nil {
		t.Fatalf("realEnv failed without $HOME: %v", err)
	}
	if e.home != "" && e.home[0] != '/' {
		t.Fatalf("home %q is not absolute", e.home)
	}
	if e.user == "" || e.getenv == nil || e.lookupUser == nil {
		t.Fatalf("%+v", e)
	}
}

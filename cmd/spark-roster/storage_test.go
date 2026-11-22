package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/walterra/spark-roster/internal/host"
)

func TestStorageOutput(t *testing.T) {
	const dir = "/synthetic/archive"
	want := host.Storage{SchemaVersion: 1, Kind: "linux_storage_observation", Path: dir, Total: 4 << 30, Available: 2 << 30}
	read := func(path string) (host.Storage, error) {
		if path != dir {
			t.Fatalf("unexpected path %q", path)
		}
		return want, nil
	}
	for _, mode := range []bool{false, true} {
		var out, diag bytes.Buffer
		args := []string{"--path", dir}
		if mode {
			args = append(args, "--json")
		}
		if code := runStorage(args, &out, &diag, read); code != 0 || diag.Len() != 0 {
			t.Fatalf("exit %d: %s", code, &diag)
		}
		if mode {
			var got host.Storage
			if err := json.Unmarshal(out.Bytes(), &got); err != nil || got != want {
				t.Fatalf("invalid observation: %+v, %v", got, err)
			}
		} else if !strings.Contains(out.String(), "2.00 GiB available / 4.00 GiB total") {
			t.Fatalf("unexpected report: %s", &out)
		}
	}
}

func TestStorageFailures(t *testing.T) {
	var out, diag bytes.Buffer
	if code := runStorage([]string{"--json"}, &out, &diag, nil); code != 2 || out.Len() != 0 || diag.Len() == 0 {
		t.Fatalf("usage: %d, %q, %q", code, &out, &diag)
	}
	diag.Reset()
	read := func(string) (host.Storage, error) { return host.Storage{}, errors.New("storage unavailable") }
	if code := runStorage([]string{"--path", "/missing", "--json"}, &out, &diag, read); code != 1 || out.Len() != 0 || !strings.Contains(diag.String(), "storage unavailable") {
		t.Fatalf("read error: %d, %q, %q", code, &out, &diag)
	}
}

package host

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadStorageDirectory(t *testing.T) {
	dir := t.TempDir()
	obs, err := ReadStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if obs.Path != dir || obs.Total == 0 || obs.Available > obs.Total || obs.CapturedAt.IsZero() {
		t.Fatalf("invalid observation: %+v", obs)
	}
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadStorage(file); err == nil {
		t.Fatal("accepted regular file")
	}
	if _, err := ReadStorage(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("accepted missing directory")
	}
}

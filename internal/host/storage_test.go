package host

import (
	"testing"
	"time"
)

func TestStorageBlockConversion(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	s, err := storageFromBlocks("/synthetic", 4096, 10000, 6000, at)
	if err != nil || s.Total != 40960000 || s.Available != 24576000 || !s.CapturedAt.Equal(at) || s.SchemaVersion != 1 || s.Kind != "linux_storage_observation" {
		t.Fatalf("unexpected storage: %+v, %v", s, err)
	}
}

func TestStorageBlockValidation(t *testing.T) {
	for _, c := range [][3]uint64{{0, 1, 1}, {1, 0, 0}, {4096, 10, 11}, {4096, maxBytes/4096 + 1, 0}} {
		if _, err := storageFromBlocks("/synthetic", c[0], c[1], c[2], time.Time{}); err == nil {
			t.Errorf("accepted invalid counters %v", c)
		}
	}
	if _, err := storageFromBlocks("/synthetic", 4096, 10, 0, time.Time{}); err != nil {
		t.Errorf("zero available: %v", err)
	}
}

package host

import (
	"fmt"
	"time"
)

// Storage describes the filesystem containing an existing directory. Available
// excludes reserved blocks; it does not account for quotas or future writes.
type Storage struct {
	SchemaVersion int       `json:"schema_version"`
	Kind          string    `json:"kind"`
	CapturedAt    time.Time `json:"captured_at"`
	Path          string    `json:"path"`
	Total         uint64    `json:"total_bytes"`
	Available     uint64    `json:"available_bytes"`
}

func storageFromBlocks(path string, blockSize, total, available uint64, now time.Time) (Storage, error) {
	if blockSize == 0 || total == 0 || available > total || total > maxBytes/blockSize {
		return Storage{}, fmt.Errorf("invalid or unsupported filesystem counters")
	}
	return Storage{
		SchemaVersion: 1, Kind: "linux_storage_observation", CapturedAt: now.UTC(),
		Path: path, Total: total * blockSize, Available: available * blockSize,
	}, nil
}

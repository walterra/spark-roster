package host

import (
	"strings"
	"testing"
	"time"
)

func TestReadMemory(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	m, err := ReadMemory(strings.NewReader("MemTotal: 4096 kB\nMemFree: 128 kB\nMemAvailable: 0 kB\nSwapTotal: 64 kB\nSwapFree: 32 kB\nHugePages_Total: 0"), at)
	if err != nil {
		t.Fatal(err)
	}
	if m.Total != 4194304 || m.Available != 0 || *m.SwapFree != 32768 || !m.CapturedAt.Equal(at) {
		t.Fatalf("unexpected observation: %+v", m)
	}
}
func TestBadMeminfo(t *testing.T) {
	for name, input := range map[string]string{
		"missing available": "MemTotal: 10 kB\nMemFree: 9 kB\n",
		"wrong unit":        "MemTotal: 10 kB\nMemAvailable: 5 MB\n",
		"overflow":          "MemTotal: 18446744073709551615 kB\nMemAvailable: 0 kB\n",
		"duplicate":         "MemTotal: 10 kB\nMemAvailable: 1 kB\nMemAvailable: 0 kB\n",
		"out of bounds":     "MemTotal: 10 kB\nMemAvailable: 11 kB\n",
		"swap bounds":       "MemTotal: 10 kB\nMemAvailable: 10 kB\nSwapTotal: 1 kB\nSwapFree: 2 kB\n",
		"oversized":         strings.Repeat("ignored\n", 10000),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ReadMemory(strings.NewReader(input), time.Now()); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
}

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/walterra/spark-roster/internal/profile"
)

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestExampleProfileParsesAndRendersNothingItShouldNot(t *testing.T) {
	p, err := profile.Parse([]byte(readRepoFile(t, "examples/adopted-service.json")))
	if err != nil {
		t.Fatal(err)
	}
	if p.Adopt == nil || p.Launch != nil || len(p.PauseDuringBenchmark) == 0 {
		t.Fatalf("%+v", p)
	}
	for _, private := range []string{"walterra", "qwen38", "gx10", "/Users/"} {
		if strings.Contains(strings.ToLower(readRepoFile(t, "examples/adopted-service.json")), private) {
			t.Errorf("example mentions %q", private)
		}
	}
}

// Every relative link in the Markdown must point at a file that exists.
func TestMarkdownRelativeLinksResolve(t *testing.T) {
	link := regexp.MustCompile(`\]\(([^)#\s]+)(#[^)]*)?\)`)
	for _, doc := range []string{"README.md", "docs/planning.md", "docs/profiles.md"} {
		dir := filepath.Dir(doc)
		for _, m := range link.FindAllStringSubmatch(readRepoFile(t, doc), -1) {
			target := m[1]
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			if _, err := os.Stat(filepath.Join("..", "..", dir, target)); err != nil {
				t.Errorf("%s links to %s, which does not exist", doc, target)
			}
		}
	}
}

// The README names commands and flags. Each must be real, so the guide cannot drift from the binary.
func TestReadmeCommandsAndFlagsExist(t *testing.T) {
	readme := readRepoFile(t, "README.md")
	var out, diag bytes.Buffer
	if code := run([]string{"--help"}, &out, &diag, nowForTest()); code != 0 {
		t.Fatal("help failed")
	}
	help := out.String()
	for _, c := range []string{"list", "status", "doctor", "stage", "switch", "stop", "recover", "version", "benchmark", "plan"} {
		if !strings.Contains(help, c) {
			t.Errorf("--help does not list %s, which the README documents", c)
		}
	}
	usage := manageUsage
	for _, c := range []string{"--json", "--dry-run", "--yes"} {
		if !strings.Contains(readme, c) || !strings.Contains(usage+" --json", c) {
			t.Errorf("flag %s missing from README or usage", c)
		}
	}
	// Every `spark-roster <command>` in a code block or table must be a command that exists.
	known := map[string]bool{"list": true, "status": true, "doctor": true, "stage": true, "switch": true, "stop": true, "recover": true, "version": true, "benchmark": true, "plan": true, "snapshot": true, "memory": true, "storage": true, "inspect": true, "profile": true, "pi-q8": true, "inkling": true}
	for _, m := range regexp.MustCompile("spark-roster ([a-z][a-z0-9-]*)").FindAllStringSubmatch(readme, -1) {
		if !known[m[1]] && m[1] != "roster" && !strings.HasPrefix(m[1], "v0") {
			t.Errorf("README runs `spark-roster %s`, which is not a command", m[1])
		}
	}
	// The profile ids the README uses are the built-in ones.
	set, _ := profile.Builtin()
	for _, id := range regexp.MustCompile(`(qwen3-8-27b-[a-z0-9-]+|inkling-2-mini)`).FindAllString(readme, -1) {
		if _, ok := set[id]; !ok {
			t.Errorf("README names profile %q, which is not built in", id)
		}
	}
	// The floors the README quotes are the profiles' own, in the exact sentence that quotes them.
	floors := []int64{set["qwen3-8-27b-pi-q8"].Memory.StartMinAvailableBytes >> 30, set["inkling-2-mini"].Memory.StartMinAvailableBytes >> 30, set["qwen3-8-27b-dflash2"].Memory.StartMinAvailableBytes >> 30}
	sentence := fmt.Sprintf("The floors in the built-in profiles (%d, %d and %d GiB)", floors[0], floors[1], floors[2])
	if !strings.Contains(readme, sentence) {
		t.Errorf("README's memory sentence does not match the profiles: want %q", sentence)
	}
	for _, row := range regexp.MustCompile(`(?m)^(inkling-2-mini|qwen3-8-27b-[a-z0-9-]+) .*?(\d+) GiB \(estimated\)`).FindAllStringSubmatch(readme, -1) {
		if got := fmt.Sprint(set[row[1]].Memory.StartMinAvailableBytes >> 30); got != row[2] {
			t.Errorf("README's list output shows %s GiB for %s, the profile says %s", row[2], row[1], got)
		}
	}
}

func TestReadmeStatesOnlyExitCodesTheBinaryUses(t *testing.T) {
	readme := readRepoFile(t, "README.md")
	for code, meaning := range map[int]string{exitOK: "Done", exitError: "failed", exitUsage: "Bad arguments", exitPrecondition: "nothing was changed", exitRestored: "restored and verified", exitUnsettled: "Host state is unknown"} {
		// Tolerate column padding: a formatter may align the table.
		row := regexp.MustCompile(`(?m)^\|\s*` + string(rune('0'+code)) + `\s*\|([^\n]*)`).FindStringSubmatch(readme)
		if row == nil || !strings.Contains(row[1], meaning) {
			t.Errorf("exit code %d row missing or wrong in README (want %q): %v", code, meaning, row)
		}
	}
}

func TestPiBenchSettingsInReadmeMatchTheCode(t *testing.T) {
	readme := readRepoFile(t, "README.md")
	settings := fmt.Sprintf("Settings for every run: Pi 1.5.0 with a %s-token window and a %s-token output cap, at most 20 inference requests and %d minutes per attempt",
		commas(scoredWindow), commas(scoredOutput), int(taskLimit.Minutes()))
	if !strings.Contains(readme, settings) {
		t.Errorf("README's benchmark settings sentence does not match the code: want %q", settings)
	}
	if scoredWindow != 262144 || scoredOutput != 16384 || taskLimit.Minutes() != 10 || benchRepeats != 3 {
		t.Fatalf("code settings changed (window %d cap %d limit %v repeats %d); update the README", scoredWindow, scoredOutput, taskLimit, benchRepeats)
	}
	_, tasks, err := benchInputs()
	if err != nil || len(tasks) != 8 {
		t.Fatalf("README says eight tasks; the suite has %d (%v)", len(tasks), err)
	}
	if !strings.Contains(readRepoFile(t, "cmd/spark-roster/pi-bench/package.json"), `"1.5.0"`) || !strings.Contains(readme, "Pi 1.5.0") {
		t.Error("Pi version in README and package.json differ")
	}
}

func nowForTest() time.Time { return time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC) }

func commas(n int) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

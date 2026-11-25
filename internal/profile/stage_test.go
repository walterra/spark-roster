package profile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type stageFx struct {
	st        *Stager
	runs      []string
	outputs   map[string]string
	downloads []string
	content   map[string][]byte // url -> body
	free      uint64
}

func newStager(t *testing.T) *stageFx {
	x := &stageFx{outputs: map[string]string{}, content: map[string][]byte{}, free: 1 << 40}
	x.st = &Stager{
		HFCache: filepath.Join(t.TempDir(), "hf"),
		Stat:    os.Stat,
		Run: func(_ context.Context, dir string, argv ...string) error {
			x.runs = append(x.runs, strings.Join(argv, " "))
			return nil
		},
		Output: func(_ context.Context, name string, args ...string) (string, error) {
			k := name + " " + strings.Join(args, " ")
			v, ok := x.outputs[k]
			if !ok {
				return "", errors.New("not found")
			}
			return v, nil
		},
		Available: func(string) (uint64, error) { return x.free, nil },
		Download: func(_ context.Context, url, dst string, offset int64) error {
			x.downloads = append(x.downloads, fmt.Sprintf("%s@%d", url, offset))
			body, ok := x.content[url]
			if !ok {
				return errors.New("404")
			}
			f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = f.WriteAt(body[offset:], offset)
			return err
		},
		ReserveBytes: 1000,
	}
	return x
}

func hfProfile(rev string, body []byte) Profile {
	p := prof("m", 1)
	p.Artifacts = []Artifact{{Kind: "huggingface", Name: "org/m", Revision: rev, Files: []File{{Path: "w/x.bin", Bytes: int64(len(body)), SHA256: sum(body)}}}}
	return p
}

func TestStageDownloadsAndVerifiesHuggingFaceFiles(t *testing.T) {
	x := newStager(t)
	rev := strings.Repeat("a", 40)
	body := []byte("weights")
	x.content["https://huggingface.co/org/m/resolve/"+rev+"/w/x.bin"] = body
	p := hfProfile(rev, body)
	if err := x.st.Stage(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(HFDir(x.st.HFCache, p.Artifacts[0]), "w/x.bin"))
	if err != nil || string(got) != "weights" {
		t.Fatalf("%v %q", err, got)
	}
	// Idempotent: a second stage downloads nothing.
	x.downloads = nil
	if err := x.st.Stage(context.Background(), p); err != nil || len(x.downloads) != 0 {
		t.Fatalf("err=%v downloads=%v", err, x.downloads)
	}
}

func TestStageRemovesCorruptDownloadAndNeverInstallsIt(t *testing.T) {
	x := newStager(t)
	rev := strings.Repeat("a", 40)
	p := hfProfile(rev, []byte("weights"))
	x.content["https://huggingface.co/org/m/resolve/"+rev+"/w/x.bin"] = []byte("WRONGXX") // same size, wrong bytes
	err := x.st.Stage(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatalf("err=%v", err)
	}
	dst := filepath.Join(HFDir(x.st.HFCache, p.Artifacts[0]), "w/x.bin")
	if _, e := os.Stat(dst); e == nil {
		t.Fatal("corrupt file was moved into place")
	}
	if _, e := os.Stat(dst + ".partial"); e == nil {
		t.Fatal("corrupt partial file was kept")
	}
}

func TestStageResumesPartialDownload(t *testing.T) {
	x := newStager(t)
	rev := strings.Repeat("a", 40)
	body := []byte("0123456789")
	p := hfProfile(rev, body)
	url := "https://huggingface.co/org/m/resolve/" + rev + "/w/x.bin"
	x.content[url] = body
	dst := filepath.Join(HFDir(x.st.HFCache, p.Artifacts[0]), "w/x.bin")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst+".partial", body[:4], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := x.st.Stage(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(x.downloads, []string{url + "@4"}) {
		t.Fatalf("downloads %v", x.downloads)
	}
}

func TestStageRefusesWhenDiskTooSmallBeforeDownloading(t *testing.T) {
	x := newStager(t)
	rev := strings.Repeat("a", 40)
	body := make([]byte, 5000)
	p := hfProfile(rev, body)
	x.free = 5500 // need 5000 + 1000 reserve
	err := x.st.Stage(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "reserve") || len(x.downloads) != 0 {
		t.Fatalf("err=%v downloads=%v", err, x.downloads)
	}
}

func TestStageNeedCountsOnlyMissingBytes(t *testing.T) {
	x := newStager(t)
	rev := strings.Repeat("a", 40)
	a := Artifact{Kind: "huggingface", Name: "org/m", Revision: rev, Files: []File{{Path: "have", Bytes: 7}, {Path: "miss", Bytes: 100}}}
	dir := HFDir(x.st.HFCache, a)
	_ = os.MkdirAll(dir, 0o755)
	_ = os.WriteFile(filepath.Join(dir, "have"), []byte("1234567"), 0o644)
	if n, _ := x.st.Need(a); n != 100 {
		t.Fatalf("need %d", n)
	}
	u := Artifact{Kind: "url", Name: filepath.Join(dir, "big.gguf"), Bytes: 1000}
	_ = os.WriteFile(u.Name+".partial", make([]byte, 400), 0o644)
	if n, _ := x.st.Need(u); n != 600 {
		t.Fatalf("need %d", n)
	}
}

func TestStageURLChecksSHAEvenWhenSizeMatches(t *testing.T) {
	x := newStager(t)
	dst := filepath.Join(t.TempDir(), "m.gguf")
	_ = os.WriteFile(dst, []byte("WRONG"), 0o644)
	p := prof("m", 1)
	p.Artifacts = []Artifact{{Kind: "url", Name: dst, URL: "https://x/m.gguf", Bytes: 5, SHA256: sum([]byte("right"))}}
	if err := x.st.Stage(context.Background(), p); err == nil || !strings.Contains(err.Error(), "wrong SHA-256") || len(x.downloads) != 0 {
		t.Fatalf("err=%v downloads=%v", err, x.downloads)
	}
}

func TestStageImagePullsOnlyWhenMissing(t *testing.T) {
	x := newStager(t)
	p := prof("m", 1)
	d := "sha256:" + strings.Repeat("a", 64)
	p.Artifacts = []Artifact{{Kind: "image", Name: "lmsysorg/sglang", Revision: d}}
	ref := "lmsysorg/sglang@" + d
	if err := x.st.Stage(context.Background(), p); err != nil || !slices.Equal(x.runs, []string{"docker pull --platform=linux/arm64 " + ref}) {
		t.Fatalf("err=%v runs=%v", err, x.runs)
	}
	x.runs = nil
	x.outputs["docker image inspect --format {{.Id}} "+ref] = "sha256:abc"
	if err := x.st.Stage(context.Background(), p); err != nil || len(x.runs) != 0 {
		t.Fatalf("present image pulled again: %v %v", err, x.runs)
	}
}

func TestStageGitFetchesPinnedCommitRefusesModifiedTreeAndBuildsOnce(t *testing.T) {
	x := newStager(t)
	dir := filepath.Join(t.TempDir(), "llama")
	rev := strings.Repeat("d", 40)
	out := filepath.Join(dir, "build", "server")
	p := prof("m", 1)
	p.Artifacts = []Artifact{{Kind: "git", Name: dir, Revision: rev, URL: "https://x/llama.git", Creates: out, Build: [][]string{{"cmake", "-S", dir}, {"cmake", "--build", dir}}}}
	x.st.Run = func(_ context.Context, d string, argv ...string) error {
		x.runs = append(x.runs, strings.Join(argv, " "))
		if argv[0] == "cmake" && argv[1] == "--build" {
			_ = os.MkdirAll(filepath.Dir(out), 0o755)
			_ = os.WriteFile(out, []byte("bin"), 0o755)
		}
		return nil
	}
	if err := x.st.Stage(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	want := []string{"git init --quiet", "git fetch --depth=1 https://x/llama.git " + rev, "git checkout --detach " + rev, "git diff --quiet " + rev + " --", "cmake -S " + dir, "cmake --build " + dir}
	if !slices.Equal(x.runs, want) {
		t.Fatalf("runs %v", x.runs)
	}
	// Second run: head matches and the build output exists, so nothing is rebuilt.
	x.runs = nil
	x.outputs["git -C "+dir+" rev-parse HEAD"] = rev
	if err := x.st.Stage(context.Background(), p); err != nil || slices.Contains(x.runs, "cmake --build "+dir) {
		t.Fatalf("err=%v runs=%v", err, x.runs)
	}
	// A modified tree is refused.
	x.st.Run = func(_ context.Context, d string, argv ...string) error {
		if argv[0] == "git" && argv[1] == "diff" {
			return errors.New("exit 1")
		}
		return nil
	}
	_ = os.Remove(out)
	if err := x.st.Stage(context.Background(), p); err == nil || !strings.Contains(err.Error(), "tracked modifications") {
		t.Fatalf("err=%v", err)
	}
}

func TestStageFailsWhenBuildLeavesNoOutput(t *testing.T) {
	x := newStager(t)
	dir := filepath.Join(t.TempDir(), "l")
	p := prof("m", 1)
	p.Artifacts = []Artifact{{Kind: "git", Name: dir, Revision: strings.Repeat("e", 40), URL: "https://x/y", Creates: filepath.Join(dir, "nope"), Build: [][]string{{"true"}}}}
	if err := x.st.Stage(context.Background(), p); err == nil || !strings.Contains(err.Error(), "is missing") {
		t.Fatalf("err=%v", err)
	}
}

func TestInstallUnitPrintsExactCommandsAndRunsNoSudo(t *testing.T) {
	dir := t.TempDir()
	path, cmds, err := InstallUnit(launched("process"), "operator", "operator", dir)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "ExecStart=") || path != filepath.Join(dir, "spark-roster-pi-q8.service") {
		t.Fatalf("%s %s", path, b)
	}
	if len(cmds) != 2 || !strings.HasPrefix(cmds[0], "sudo install -o root -g root -m 0644 '") || !strings.HasSuffix(cmds[0], "/etc/systemd/system/spark-roster-pi-q8.service") || cmds[1] != "sudo systemctl daemon-reload" {
		t.Fatalf("%v", cmds)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", st.Mode())
	}
	if _, _, err := InstallUnit(prof("adopted", 1), "operator", "operator", dir); err == nil {
		t.Fatal("adopted profile produced a unit")
	}
}

func TestStageDiscardsOversizedPartialAndRestarts(t *testing.T) {
	x := newStager(t)
	rev := strings.Repeat("a", 40)
	body := []byte("0123456789")
	p := hfProfile(rev, body)
	url := "https://huggingface.co/org/m/resolve/" + rev + "/w/x.bin"
	x.content[url] = body
	dst := filepath.Join(HFDir(x.st.HFCache, p.Artifacts[0]), "w/x.bin")
	_ = os.MkdirAll(filepath.Dir(dst), 0o755)
	_ = os.WriteFile(dst+".partial", []byte("0123456789-and-more-garbage"), 0o644)
	if err := x.st.Stage(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(x.downloads, []string{url + "@0"}) {
		t.Fatalf("downloads %v", x.downloads)
	}
}

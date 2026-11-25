package profile

import (
	"slices"
	"strings"
	"testing"
)

func TestBuiltinProfilesLoadAndValidate(t *testing.T) {
	m, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"inkling-2-mini", "qwen3-8-27b-dflash2", "qwen3-8-27b-pi-q8"}; !slices.Equal(IDs(m), want) {
		t.Fatalf("ids %v", IDs(m))
	}
	ports := map[uint16]string{}
	for id, p := range m {
		if p.Launch == nil {
			t.Errorf("%s: built-in profiles must be launchable", id)
		}
		if other, dup := ports[p.Serving.Port]; dup {
			t.Errorf("%s and %s share port %d", id, other, p.Serving.Port)
		}
		ports[p.Serving.Port] = id
		if _, err := RenderUnit(p, "operator", "operator"); err != nil {
			t.Errorf("%s: %v", id, err)
		}
		if _, err := Expand(p, Vars{Home: "/h", Data: "/d", HFCache: "/hf"}); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
}

func TestBuiltinPinsMatchVerifiedSources(t *testing.T) {
	m, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	art := func(id, kind, name string) Artifact {
		for _, a := range m[id].Artifacts {
			if a.Kind == kind && strings.HasSuffix(a.Name, name) {
				return a
			}
		}
		t.Fatalf("%s has no %s %s", id, kind, name)
		return Artifact{}
	}
	// Pins read from the Hugging Face tree API and the Docker registry on 2026-11-25.
	if a := art("qwen3-8-27b-dflash2", "huggingface", "Qwen3.8-27B-NVFP4"); a.Revision != "52d1adc5f38aa5ebf099c29ed7025ba34cfbb854" || len(a.Files) != 22 {
		t.Errorf("target pin %+v", a.Revision)
	}
	if a := art("qwen3-8-27b-dflash2", "huggingface", "DFlash2-NVFP4-RTNcal"); a.Revision != "bd7a934213c47a9e7ef69eef36bb3325f47fd1f1" || len(a.Files) != 6 {
		t.Errorf("drafter pin %+v", a.Revision)
	}
	if a := art("qwen3-8-27b-dflash2", "image", "sglang"); a.Revision != "sha256:e3a4f06001a29542fc4a8638536d9ffc5eb9c9942b08603c09b19a56eae4a1ae" {
		t.Errorf("qwen image %s", a.Revision)
	}
	if a := art("inkling-2-mini", "image", "sglang"); a.Revision != "sha256:bc811ac7ac68c6fca116a49750baca817b8613a30abface8a347682b55d132e3" {
		t.Errorf("inkling image %s", a.Revision)
	}
	ink := art("inkling-2-mini", "huggingface", "Inkling-2-Mini")
	if ink.Revision != "b85c3cfa615fc413cc9af3379f491b963f3b586a" || len(ink.Files) != 14 {
		t.Errorf("inkling pin %+v", ink.Revision)
	}
	var big File
	for _, f := range ink.Files {
		if f.Path == "model-00001-of-00001.safetensors" {
			big = f
		}
	}
	if big.Bytes != 19526019168 || big.SHA256 != "65e60f51aa1284d4fb022584908c315270334d991828bb774af60584523539d1" {
		t.Errorf("inkling weights %+v", big)
	}
	gguf := art("qwen3-8-27b-pi-q8", "url", "Qwen3.8-27B-pi-Q8_0.gguf")
	if gguf.Bytes != 28595763840 || gguf.SHA256 != "bcb2b967901e772e76842bee0c67bc92f27c819c0ce7f14642e17d64aadd4d21" || !strings.Contains(gguf.URL, "19980ce556b41c7b73818083c9adf4b3fa0c72e5") {
		t.Errorf("gguf %+v", gguf)
	}
	if g := art("qwen3-8-27b-pi-q8", "git", "b1b770d90d94cad9e9ea5b993bdbc383a522fb28"); g.Revision != "b1b770d90d94cad9e9ea5b993bdbc383a522fb28" {
		t.Errorf("llama.cpp %s", g.Revision)
	}
}

func TestBuiltinLaunchesUseTheScoredWindowAndPinnedImage(t *testing.T) {
	m, _ := Builtin()
	for id, p := range m {
		if p.ContextTokens != 262144 {
			t.Errorf("%s context_tokens %d", id, p.ContextTokens)
		}
		joined := strings.Join(p.Launch.Args, " ")
		if !strings.Contains(joined, "262144") && id != "qwen3-8-27b-dflash2" { // Qwen's window is the model's native 262,144
			t.Errorf("%s does not set a 262144-token window: %s", id, joined)
		}
		if p.Launch.Runtime == "docker" && !strings.Contains(p.Launch.Image, "@sha256:") {
			t.Errorf("%s image is not pinned", id)
		}
		if strings.Contains(joined, "--entrypoint") {
			t.Errorf("%s hides an entrypoint override in its arguments", id)
		}
	}
	q := m["qwen3-8-27b-dflash2"].Launch
	if q.Args[0] != "python3" || slices.Contains(q.DockerArgs, "--entrypoint") {
		t.Errorf("qwen must keep the image entrypoint and run python3 as the command: %v %v", q.Args[:2], q.DockerArgs)
	}
}

// The hub cache is mounted where huggingface_hub looks for it. {hf_cache} is the
// hub directory itself, the one holding models--org--name, so it must land on
// .../huggingface/hub and not one level above.
func TestBuiltinQwenMountsTheHubCacheAtTheHubPath(t *testing.T) {
	m, _ := Builtin()
	mounts := m["qwen3-8-27b-dflash2"].Launch.Mounts
	if !slices.Contains(mounts, "{hf_cache}:/root/.cache/huggingface/hub") || slices.Contains(mounts, "{hf_cache}:/root/.cache/huggingface") {
		t.Fatalf("mounts %v", mounts)
	}
	for _, a := range m["qwen3-8-27b-dflash2"].Artifacts {
		if a.Kind == "huggingface" && a.Dest != "" {
			t.Fatalf("%s uses a flat dest, so the engine would not find it in the hub cache", a.Name)
		}
	}
}

func TestBuiltinRejectsProfileWhoseIDDiffersFromItsFileName(t *testing.T) {
	if _, err := parseNamed("site-qwen.json", []byte(goodAdopt)); err != nil {
		t.Fatalf("matching name rejected: %v", err)
	}
	if _, err := parseNamed("other-name.json", []byte(goodAdopt)); err == nil || !strings.Contains(err.Error(), "does not match the file name") {
		t.Fatalf("mismatch accepted: %v", err)
	}
}

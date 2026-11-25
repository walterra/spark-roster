package profile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/walterra/spark-roster/internal/planner"
)

// ToPlanner converts a profile into the planner's catalog entry, so memory
// forecasts use the same pins the switch engine launches. Planner revisions
// come from the artifacts marked model, drafter and runtime.
func ToPlanner(p Profile) (planner.Profile, error) {
	var model, runtime, drafter *Artifact
	for i := range p.Artifacts {
		switch p.Artifacts[i].Role {
		case "model":
			model = &p.Artifacts[i]
		case "runtime":
			runtime = &p.Artifacts[i]
		case "drafter":
			drafter = &p.Artifacts[i]
		}
	}
	if model == nil || runtime == nil {
		return planner.Profile{}, fmt.Errorf("profile %s needs artifacts with role model and role runtime to be planned", p.ID)
	}
	rev := func(a Artifact) planner.Revision {
		switch a.Kind {
		case "url":
			return planner.Revision{Name: a.URL, Revision: a.SHA256}
		case "git":
			return planner.Revision{Name: a.URL, Revision: a.Revision}
		}
		return planner.Revision{Name: a.Name, Revision: a.Revision}
	}
	startup, steady := p.Memory.StartMinAvailableBytes, p.Memory.SteadyBytes
	if steady == 0 {
		steady = startup
	}
	out := planner.Profile{
		Model: rev(*model), Runtime: rev(*runtime), Concurrency: max(p.Concurrency, 1),
		Memory: &planner.Memory{Startup: &startup, Steady: &steady, Evidence: p.Memory.Evidence, RecordedAt: p.Memory.RecordedAt, Note: p.Memory.Note},
	}
	if drafter != nil {
		d := rev(*drafter)
		out.Drafter = &d
	}
	if p.ContextTokens > 0 {
		c := p.ContextTokens
		out.ContextTokens = &c
	}
	return out, nil
}

// Catalog builds a planner catalog from a profile set. Profiles that cannot be
// planned are reported, not silently dropped.
func Catalog(set map[string]Profile) (planner.Catalog, error) {
	c := planner.Catalog{Header: planner.Header{SchemaVersion: 1, Kind: "catalog"}, Profiles: map[string]planner.Profile{}}
	var problems []string
	for _, id := range IDs(set) {
		pp, err := ToPlanner(set[id])
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		c.Profiles[id] = pp
	}
	if len(problems) > 0 {
		return c, errors.New(strings.Join(problems, "; "))
	}
	return c, nil
}

// Load returns the built-in profiles plus every *.json file in dir. A missing
// directory is fine. A site profile may not reuse a built-in id, so a typo
// cannot silently replace a shipped pin.
func Load(dir string) (map[string]Profile, error) {
	set, err := Builtin()
	if err != nil {
		return nil, err
	}
	if dir == "" {
		return set, nil
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return set, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			return nil, err
		}
		p, err := parseNamed(n, b)
		if err != nil {
			return nil, err
		}
		if _, dup := set[p.ID]; dup {
			return nil, fmt.Errorf("%s: id %q is already defined; site profiles cannot replace another profile", filepath.Join(dir, n), p.ID)
		}
		set[p.ID] = p
	}
	return set, nil
}

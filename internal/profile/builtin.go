package profile

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed builtin/*.json
var builtinFS embed.FS

// Builtin returns the profiles shipped with SparkRoster, keyed by id. They are
// validated when loaded, so a bad pin fails the tests rather than a switch.
func Builtin() (map[string]Profile, error) {
	entries, err := builtinFS.ReadDir("builtin")
	if err != nil {
		return nil, err
	}
	out := map[string]Profile{}
	for _, e := range entries {
		b, err := builtinFS.ReadFile("builtin/" + e.Name())
		if err != nil {
			return nil, err
		}
		p, err := parseNamed(e.Name(), b)
		if err != nil {
			return nil, err
		}
		if _, dup := out[p.ID]; dup {
			return nil, fmt.Errorf("profile %s: duplicate id", e.Name())
		}
		out[p.ID] = p
	}
	return out, nil
}

// IDs returns the sorted ids of a profile set.
func IDs(m map[string]Profile) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// parseNamed parses a profile file and checks that its id is the file name.
func parseNamed(file string, b []byte) (Profile, error) {
	p, err := Parse(b)
	if err != nil {
		return Profile{}, fmt.Errorf("profile %s: %w", file, err)
	}
	if want := strings.TrimSuffix(file, ".json"); p.ID != want {
		return Profile{}, fmt.Errorf("profile %s: id %q does not match the file name", file, p.ID)
	}
	return p, nil
}

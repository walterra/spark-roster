package profile

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Vars supplies the host-specific paths a profile refers to as {name}.
type Vars struct {
	Home    string // operator home directory
	Data    string // SparkRoster data directory, for weights and builds
	HFCache string // Hugging Face hub cache
}

var token = regexp.MustCompile(`\{(home|data|hf_cache|dir)\}`)

func (v Vars) check() error {
	for name, p := range map[string]string{"home": v.Home, "data": v.Data, "hf_cache": v.HFCache} {
		if !strings.HasPrefix(p, "/") || strings.ContainsAny(p, "\r\n\x00") {
			return fmt.Errorf("%s must be an absolute path without control characters", name)
		}
	}
	return nil
}

func (v Vars) replace(s, dir string) string {
	return token.ReplaceAllStringFunc(s, func(m string) string {
		switch m {
		case "{home}":
			return v.Home
		case "{data}":
			return v.Data
		case "{hf_cache}":
			return v.HFCache
		default:
			return dir
		}
	})
}

// Expand returns a copy of p with {home}, {data} and {hf_cache} replaced, and
// {dir} inside a git artifact's build commands replaced by that artifact's
// checkout directory. Other braces, such as JSON in an argument, are left alone.
func Expand(p Profile, v Vars) (Profile, error) {
	if err := v.check(); err != nil {
		return Profile{}, err
	}
	q := p
	q.Artifacts = make([]Artifact, len(p.Artifacts))
	for i, a := range p.Artifacts {
		a.Name = v.replace(a.Name, "")
		a.Dest = v.replace(a.Dest, "")
		a.Creates = v.replace(a.Creates, "")
		if a.Build != nil {
			build := make([][]string, len(a.Build))
			for j, c := range a.Build {
				build[j] = make([]string, len(c))
				for k, w := range c {
					build[j][k] = v.replace(w, a.Name)
				}
			}
			a.Build = build
		}
		q.Artifacts[i] = a
	}
	if p.Launch != nil {
		l := *p.Launch
		each := func(in []string) []string {
			out := make([]string, len(in))
			for i, w := range in {
				out[i] = v.replace(w, "")
			}
			return out
		}
		l.Exe = v.replace(l.Exe, "")
		l.Args, l.Mounts, l.Dirs, l.DockerArgs = each(l.Args), each(l.Mounts), each(l.Dirs), each(l.DockerArgs)
		if l.Env != nil {
			l.Env = map[string]string{}
			for k, w := range p.Launch.Env {
				l.Env[k] = v.replace(w, "")
			}
		}
		q.Launch = &l
	}
	q.Serving.KeyFile = v.replace(p.Serving.KeyFile, "")
	if strings.Contains(q.Serving.KeyFile, "{") {
		return Profile{}, errors.New("key_file has an unknown variable")
	}
	return q, nil
}

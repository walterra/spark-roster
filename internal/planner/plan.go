// Package planner forecasts memory without reading the host or changing services.
package planner

import (
	"fmt"
	"regexp"
	"slices"
	"time"
)

const (
	MaxBytes       = int64(1<<53 - 1)
	SnapshotMaxAge = 5 * time.Minute
	EvidenceMaxAge = 30 * 24 * time.Hour
)

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

type Header struct {
	SchemaVersion int    `json:"schema_version"`
	Kind          string `json:"kind"`
}
type Revision struct {
	Name     string `json:"name"`
	Revision string `json:"revision"`
}
type Memory struct {
	Startup    *int64    `json:"startup_peak_bytes"`
	Steady     *int64    `json:"steady_peak_bytes"`
	Evidence   string    `json:"evidence"`
	RecordedAt time.Time `json:"recorded_at"`
	Note       string    `json:"note"`
}
type Profile struct {
	Model         Revision  `json:"model"`
	Runtime       Revision  `json:"runtime"`
	Drafter       *Revision `json:"drafter,omitempty"`
	Concurrency   int       `json:"concurrency"`
	ContextTokens *int      `json:"context_tokens,omitempty"`
	Memory        *Memory   `json:"memory,omitempty"`
}
type Catalog struct {
	Header
	Profiles map[string]Profile `json:"profiles"`
}
type DeploymentSet struct {
	Header
	ID        string            `json:"id"`
	Services  map[string]string `json:"services"`
	Protected []string          `json:"protected_services"`
	Reserve   *int64            `json:"reserve_bytes"`
}
type RunningService struct {
	ProfileID string `json:"profile_id"`
	Growth    *int64 `json:"additional_growth_upper_bound_bytes,omitempty"`
	Reclaim   *int64 `json:"stop_reclaim_lower_bound_bytes,omitempty"`
}
type Snapshot struct {
	Header
	CapturedAt time.Time                 `json:"captured_at"`
	Total      *int64                    `json:"total_bytes"`
	Available  *int64                    `json:"available_bytes"`
	Running    map[string]RunningService `json:"running_services"`
}

type Reason struct {
	Code    string `json:"code"`
	Subject string `json:"subject,omitempty"`
}

type LaunchEvidence struct {
	Service   string  `json:"service"`
	ProfileID string  `json:"profile_id"`
	Memory    *Memory `json:"memory,omitempty"`
}

type Phase struct {
	Name                  string `json:"name"`
	AdditionalDemandBytes *int64 `json:"additional_demand_bytes"`
	HeadroomBytes         *int64 `json:"headroom_bytes"`
}

type Path struct {
	Name             string   `json:"name"`
	StopBeforeLaunch []string `json:"stop_before_launch"`
	ReclaimBytes     *int64   `json:"reclaim_bytes"`
	Phases           []Phase  `json:"phases"`
	Reasons          []Reason `json:"reasons"`
}

type Result struct {
	SchemaVersion  int              `json:"schema_version"`
	DeploymentSet  string           `json:"deployment_set"`
	EvaluatedAt    time.Time        `json:"evaluated_at"`
	SnapshotAt     time.Time        `json:"snapshot_at"`
	AvailableBytes int64            `json:"available_bytes"`
	ReserveBytes   int64            `json:"reserve_bytes"`
	Verdict        string           `json:"verdict"`
	ReasonCode     string           `json:"reason_code"`
	Protected      []string         `json:"protected_services"`
	Replace        []string         `json:"replace_services"`
	Launch         []LaunchEvidence `json:"launch"`
	Paths          []Path           `json:"paths"`
}

func validBytes(v *int64) bool      { return v != nil && *v >= 0 && *v <= MaxBytes }
func validRevision(r Revision) bool { return r.Name != "" && r.Revision != "" }

func keys[V any](m map[string]V) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	slices.Sort(result)
	return result
}

func validate(c Catalog, d DeploymentSet, s Snapshot) error {
	if c.Header != (Header{1, "catalog"}) || d.Header != (Header{1, "deployment_set"}) || s.Header != (Header{1, "host_snapshot"}) {
		return fmt.Errorf("expected v1 catalog, deployment_set, and host_snapshot")
	}
	if !idPattern.MatchString(d.ID) || len(c.Profiles) == 0 || len(d.Services) == 0 || d.Protected == nil || s.Running == nil ||
		!validBytes(d.Reserve) || !validBytes(s.Total) || !validBytes(s.Available) || *s.Total == 0 || *s.Available > *s.Total || s.CapturedAt.IsZero() {
		return fmt.Errorf("invalid or missing deployment or host snapshot fields")
	}
	for _, id := range keys(c.Profiles) {
		p := c.Profiles[id]
		if !idPattern.MatchString(id) || !validRevision(p.Model) || !validRevision(p.Runtime) ||
			(p.Drafter != nil && !validRevision(*p.Drafter)) || p.Concurrency < 1 || (p.ContextTokens != nil && *p.ContextTokens < 1) {
			return fmt.Errorf("invalid profile %q", id)
		}
		if m := p.Memory; m != nil && (!validBytes(m.Startup) || !validBytes(m.Steady) ||
			(m.Evidence != "measured" && m.Evidence != "estimated") || m.RecordedAt.IsZero() || m.Note == "") {
			return fmt.Errorf("invalid memory evidence for profile %q", id)
		}
	}
	for _, name := range keys(d.Services) {
		if _, ok := c.Profiles[d.Services[name]]; !idPattern.MatchString(name) || !ok {
			return fmt.Errorf("invalid desired service or unknown profile for %q", name)
		}
	}
	for _, name := range keys(s.Running) {
		r := s.Running[name]
		if !idPattern.MatchString(name) || !idPattern.MatchString(r.ProfileID) ||
			(r.Growth != nil && (!validBytes(r.Growth) || *r.Growth > *s.Total)) ||
			(r.Reclaim != nil && (!validBytes(r.Reclaim) || *r.Reclaim > *s.Total)) {
			return fmt.Errorf("invalid running service %q", name)
		}
	}
	seen := make(map[string]bool)
	for _, name := range d.Protected {
		if _, ok := d.Services[name]; !ok || seen[name] {
			return fmt.Errorf("invalid or duplicate protected service %q", name)
		}
		seen[name] = true
	}
	return nil
}

func fresh(at, now time.Time, limit time.Duration) bool {
	return !at.After(now) && now.Sub(at) <= limit
}

// add caps aggregate calculations at the exact-integer limit for JSON clients.
func add(sum *int64, n int64) error {
	if n > MaxBytes-*sum {
		return fmt.Errorf("aggregate memory exceeds supported byte range")
	}
	*sum += n
	return nil
}

func Build(c Catalog, d DeploymentSet, s Snapshot, now time.Time) (Result, error) {
	if err := validate(c, d, s); err != nil {
		return Result{}, err
	}
	r := Result{
		SchemaVersion: 1, DeploymentSet: d.ID, EvaluatedAt: now.UTC(), SnapshotAt: s.CapturedAt.UTC(),
		AvailableBytes: *s.Available, ReserveBytes: *d.Reserve, Protected: append([]string{}, d.Protected...),
		Replace: []string{}, Launch: []LaunchEvidence{}, Paths: []Path{},
	}
	slices.Sort(r.Protected)
	common := []Reason{}
	if !fresh(s.CapturedAt, now, SnapshotMaxAge) {
		common = append(common, Reason{"snapshot_stale_or_future", ""})
	}
	for _, name := range r.Protected {
		if s.Running[name].ProfileID != d.Services[name] {
			r.Verdict, r.ReasonCode = "unknown", "protected_service_not_running_as_requested"
			r.Paths = append(r.Paths, Path{Name: "overlap", StopBeforeLaunch: []string{}, Phases: []Phase{}, Reasons: []Reason{{r.ReasonCode, name}}})
			return r, nil
		}
	}
	var startup, steady int64
	for _, name := range keys(d.Services) {
		id := d.Services[name]
		running, exists := s.Running[name]
		if exists && running.ProfileID == id {
			continue
		}
		if exists {
			r.Replace = append(r.Replace, name)
		}
		m := c.Profiles[id].Memory
		r.Launch = append(r.Launch, LaunchEvidence{name, id, m})
		switch {
		case m == nil:
			common = append(common, Reason{"missing_launch_evidence", name})
		case m.Evidence != "measured":
			common = append(common, Reason{"unverified_launch_estimate", name})
		case !fresh(m.RecordedAt, now, EvidenceMaxAge):
			common = append(common, Reason{"launch_evidence_stale_or_future", name})
		default:
			if err := add(&startup, *m.Startup); err != nil {
				return Result{}, err
			}
			if err := add(&steady, *m.Steady); err != nil {
				return Result{}, err
			}
		}
	}
	overlap, err := budget("overlap", []string{}, s, r.ReserveBytes, startup, steady, common)
	if err != nil {
		return Result{}, err
	}
	r.Paths = append(r.Paths, overlap)
	if len(r.Replace) > 0 {
		stop, err := budget("stop-first", r.Replace, s, r.ReserveBytes, startup, steady, common)
		if err != nil {
			return Result{}, err
		}
		r.Paths = append(r.Paths, stop)
	}
	switch {
	case fits(overlap):
		r.Verdict, r.ReasonCode = "fit", "overlap_within_budget"
	case len(r.Paths) > 1 && fits(r.Paths[1]):
		r.Verdict, r.ReasonCode = "stop-first", "stop_first_within_budget"
	case slices.ContainsFunc(r.Paths, func(p Path) bool { return len(p.Reasons) > 0 }):
		r.Verdict, r.ReasonCode = "unknown", "incomplete_memory_evidence"
	default:
		r.Verdict, r.ReasonCode = "insufficient", "no_path_within_budget"
	}
	return r, nil
}

func budget(name string, stops []string, s Snapshot, reserve, startup, steady int64, common []Reason) (Path, error) {
	p := Path{Name: name, StopBeforeLaunch: stops, Reasons: append([]Reason{}, common...)}
	var growth, reclaim int64
	for _, service := range keys(s.Running) {
		rs := s.Running[service]
		if slices.Contains(stops, service) {
			if rs.Reclaim == nil {
				p.Reasons = append(p.Reasons, Reason{"missing_stop_reclaim", service})
				continue
			}
			if err := add(&reclaim, *rs.Reclaim); err != nil {
				return Path{}, err
			}
		} else {
			if rs.Growth == nil {
				p.Reasons = append(p.Reasons, Reason{"missing_running_growth", service})
				continue
			}
			if err := add(&growth, *rs.Growth); err != nil {
				return Path{}, err
			}
		}
	}
	if reclaim > *s.Total-*s.Available {
		p.Reasons = append(p.Reasons, Reason{"reclaim_exceeds_unavailable_memory", ""})
	}
	for _, phase := range []struct {
		name   string
		demand int64
	}{{"startup", startup}, {"steady", steady}} {
		row := Phase{Name: phase.name}
		if len(p.Reasons) == 0 {
			demand := phase.demand
			if err := add(&demand, growth); err != nil {
				return Path{}, err
			}
			headroom := *s.Available + reclaim - demand - reserve
			p.ReclaimBytes, row.AdditionalDemandBytes, row.HeadroomBytes = &reclaim, &demand, &headroom
		}
		p.Phases = append(p.Phases, row)
	}
	return p, nil
}

func fits(p Path) bool {
	return len(p.Phases) > 0 && !slices.ContainsFunc(p.Phases, func(row Phase) bool {
		return row.HeadroomBytes == nil || *row.HeadroomBytes < 0
	})
}

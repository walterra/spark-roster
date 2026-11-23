package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const qualDirectBudget = 30 * time.Minute
const qualPiRequests = 4
const qualPiBudget = qualPiRequests*qualDirectBudget + time.Minute

// Two direct probes, three Pi sessions and 27 minutes for preflight, switches and tokenization.
const qualWorkBudget = 2*qualDirectBudget + 3*qualPiBudget + 27*time.Minute
const qualCandidateLifetime = qualDirectBudget + qualPiBudget + 15*time.Minute

// qualRequest is the relay's record of one chat request: what was sent, what the
// upstream answered, and why the exchange ended.
type qualRequest struct {
	Cap       int    `json:"output_cap"`
	Bytes     int    `json:"request_bytes"`
	Status    int    `json:"http_status"`
	Finish    string `json:"finish_reason,omitempty"`
	SawDone   bool   `json:"saw_done,omitempty"`
	BadEvents int    `json:"unparseable_events,omitempty"`
	End       string `json:"end"`
	Error     string `json:"error,omitempty"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

type qualSSE struct {
	line     []byte
	overflow bool
	finish   string
	done     bool
	bad      int
}

// terminal reports whether the stream carried a finish_reason or [DONE].
func (s *qualSSE) terminal() bool { return s.finish != "" || s.done }

func (s *qualSSE) add(b []byte) {
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		n := len(b)
		if i >= 0 {
			n = i
		}
		if !s.overflow && len(s.line)+n <= benchContentLimit {
			s.line = append(s.line, b[:n]...)
		} else {
			s.overflow = true
		}
		if i < 0 {
			return
		}
		if !s.overflow {
			if data, ok := bytes.CutPrefix(s.line, []byte("data:")); ok {
				data = bytes.TrimSpace(data)
				var event struct {
					Choices []struct {
						Finish string `json:"finish_reason"`
					}
				}
				switch {
				case string(data) == "[DONE]":
					s.done = true
				case json.Unmarshal(data, &event) != nil:
					s.bad++
				case len(event.Choices) > 0 && event.Choices[0].Finish != "":
					s.finish = event.Choices[0].Finish
				}
			}
		}
		s.line = s.line[:0]
		s.overflow = false
		b = b[n+1:]
	}
}

func qualAPI(ctx context.Context, t qualTarget, path string, body any, result any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://127.0.0.1:%d%s", t.port, path), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.key != "" {
		req.Header.Set("Authorization", "Bearer "+t.key)
	}
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	c := http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := c.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("%s HTTP %d", path, res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(result)
}

func qualCount(ctx context.Context, t qualTarget, prompt string) (int, error) {
	messages := []map[string]string{{"role": "user", "content": prompt}}
	if t.name == "pi-q8" {
		var rendered struct{ Prompt string }
		if err := qualAPI(ctx, t, "/apply-template", map[string]any{"messages": messages}, &rendered); err != nil {
			return 0, err
		}
		if rendered.Prompt == "" {
			return 0, errors.New("template returned no prompt")
		}
		var toks struct{ Tokens []int }
		err := qualAPI(ctx, t, "/tokenize", map[string]any{"content": rendered.Prompt, "add_special": true}, &toks)
		return len(toks.Tokens), err
	}
	// SGLang's configured proxy forwards the chat tokenization endpoint. Do not bypass auth or count raw text.
	var res struct {
		Count  int
		Tokens []int
	}
	var errs error
	for _, path := range []string{"/v1/tokenize", "/tokenize"} {
		res = struct {
			Count  int
			Tokens []int
		}{}
		if err := qualAPI(ctx, t, path, map[string]any{"model": t.model, "messages": messages}, &res); err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		if n := max(res.Count, len(res.Tokens)); n > 0 {
			return n, nil
		}
	}
	return 0, errors.Join(errors.New("chat tokenization unavailable"), errs)
}

func qualTopFixture(ctx context.Context, t qualTarget) (string, [3]string, int, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", [3]string{}, 0, err
	}
	seed := fmt.Sprintf("%x", nonce[:8])
	var markers [3]string
	for i := range markers {
		markers[i] = fmt.Sprintf("seal-%x", nonce[8+i*8:16+i*8])
	}
	target := 262144 - qualOutput - 2048
	phase, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	n, err := qualCount(phase, t, qualFixture(160, seed, markers))
	if err != nil {
		return "", markers, 0, err
	}
	if n < 160 {
		return "", markers, 0, errors.New("chat tokenizer returned implausible size")
	}
	lines := target * 160 / n
	for range 3 {
		prompt := qualFixture(lines, seed, markers)
		n, err = qualCount(phase, t, prompt)
		if err != nil {
			return "", markers, 0, err
		}
		if n >= target-1536 && n <= target+1536 {
			return prompt, markers, n, nil
		}
		if n < 160 {
			return "", markers, 0, errors.New("chat tokenizer returned implausible size")
		}
		lines = lines * target / n
	}
	return "", markers, n, errors.New("could not size near-256K fixture without inference")
}

func qualPriorTier(r qualification, name string) (int, int) {
	tier, calibration := 0, 0
	for _, p := range r.Profiles {
		if p.Name != name || p.Server != 262144 {
			continue
		}
		for _, s := range p.Probes {
			if s.Server != 262144 || s.Error != "" || s.Metrics == nil || s.Metrics.PromptTokens == nil {
				continue
			}
			if s.Stage == "calibrate" {
				calibration = *s.Metrics.PromptTokens
			}
			if s.Stage == "cold_capacity" || s.Stage == "prior_cold_capacity" {
				tier = max(tier, s.Tier)
			}
		}
	}
	return tier, calibration
}

func qualifyRetryTarget(ctx context.Context, t qualTarget, image string, p *qualProfile, prior qualification, save func() error, out io.Writer) error {
	const largest = 262144
	if t.launch != nil {
		s := qualProbe{Server: largest, Tier: largest, Stage: "launch"}
		err, _ := qualMeasure(ctx, &s, func(c context.Context) error { return t.launch(c, largest) })
		if err != nil {
			s.Error = err.Error()
		}
		p.Probes = append(p.Probes, s)
		if e := save(); e != nil {
			return errors.Join(e, err)
		}
		if err != nil {
			return err
		}
	}
	p.Server = largest
	tier, tokens := qualPriorTier(prior, t.name)
	for _, profile := range prior.Profiles {
		if profile.Name == t.name {
			for _, s := range profile.Probes {
				if s.Server != largest || s.Error != "" {
					continue
				}
				if s.Stage == "cold_capacity" || s.Stage == "prior_cold_capacity" {
					s.Stage = "prior_cold_capacity"
					p.Probes = append(p.Probes, s)
				}
				if s.Stage == "calibrate" {
					p.Probes = append(p.Probes, s)
				}
			}
		}
	}
	if tier < largest {
		s := qualProbe{Server: largest, Tier: largest, Stage: "cold_capacity"}
		err, guard := qualMeasure(ctx, &s, func(c context.Context) error {
			prompt, markers, count, e := qualTopFixture(c, t)
			if e != nil {
				return e
			}
			s.SizedInput = count
			phase, cancel := context.WithTimeout(c, qualDirectBudget)
			defer cancel()
			m, answer, e := timedCompletion(phase, t.port, t.model, t.key, prompt, 512)
			s.Metrics = &m
			if e != nil {
				return e
			}
			return qualVerdict(m, answer, markers, largest-qualOutput)
		})
		if err != nil {
			s.Error = err.Error()
			fmt.Fprintf(out, "%s 256K direct unverified: %v\n", t.name, err)
		} else {
			tier = largest
		}
		p.Probes = append(p.Probes, s)
		if e := save(); e != nil {
			return e
		}
		if guard != nil {
			return guard
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	p.Direct = tier
	if tier == 0 || tokens < 160 {
		return errors.New("prior report lacks a passing direct probe or calibration")
	}
	s := qualProbe{Server: largest, Tier: tier, Stage: "pi_tool_followup"}
	err, guard := qualMeasure(ctx, &s, func(c context.Context) error {
		// Keep room for Pi's 4K safety reserve, tools/history and the separate 16K output cap.
		lines := (tier - qualOutput - 4096) * 8 * 160 / (10 * tokens)
		var e error
		p.Pi, e = qualifyPi(c, image, t, tier, lines)
		if e == nil && p.Pi.PeakInput < (tier-qualOutput)*3/4 {
			return errors.New("Pi did not report enough rendered input to verify the long tool path")
		}
		return e
	})
	if err != nil {
		s.Error = err.Error()
		fmt.Fprintf(out, "%s Pi %d unverified: %v\n", t.name, tier, err)
	} else {
		p.Verified = tier
	}
	p.Probes = append(p.Probes, s)
	if guard != nil {
		return guard
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return save()
}

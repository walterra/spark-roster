package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const benchContentLimit = 256 << 10

type benchSpeed struct {
	Pair             int      `json:"pair"`
	Cache            string   `json:"cache"`
	Error            string   `json:"error,omitempty"`
	FirstDeltaMS     int64    `json:"first_nonempty_text_delta_ms"`
	FirstAnswerMS    *int64   `json:"first_visible_answer_ms,omitempty"`
	WallMS           int64    `json:"wall_ms"`
	PromptTokens     *int     `json:"prompt_tokens,omitempty"`
	CompletionTokens *int     `json:"completion_tokens,omitempty"`
	DecodeEstimate   *float64 `json:"estimated_decode_tokens_per_second,omitempty"`
	FinishReason     string   `json:"finish_reason"`
}

func benchFreePort(port uint16) error {
	l, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	return l.Close()
}

func measureBenchProfile(ctx context.Context, res *benchResult, port uint16, model, key string, start func(context.Context) error, quality func(context.Context) error, seed string) error {
	m, err := localMemory()
	if err != nil {
		return err
	}
	var minimum atomic.Uint64
	minimum.Store(m.Available)
	phase, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-phase.Done():
				done <- nil
				return
			case <-ticker.C:
				sample, err := localMemory()
				if err != nil {
					done <- err
					cancel()
					return
				}
				if sample.Available < minimum.Load() {
					minimum.Store(sample.Available)
				}
				if sample.Available < 10*trialGiB {
					done <- errors.New("sampled host memory fell below 10 GiB")
					cancel()
					return
				}
			}
		}
	}()
	runErr := func() error {
		if start != nil {
			t := time.Now()
			if err := start(phase); err != nil {
				return err
			}
			res.SwitchReadyMS = time.Since(t).Milliseconds()
		}
		res.Speed = runSpeedPairs(phase, port, model, key, seed)
		if err := phase.Err(); err != nil {
			return err
		}
		return quality(phase)
	}()
	cancel()
	sampleErr := <-done
	res.MinAvailable = minimum.Load()
	return errors.Join(runErr, sampleErr)
}

func benchCompletion(ctx context.Context, port uint16, model, key, prompt string, tokens int) (benchSpeed, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	payload, err := json.Marshal(map[string]any{"model": model, "messages": []map[string]string{{"role": "user", "content": prompt}}, "max_tokens": tokens, "temperature": 0, "stream": true, "stream_options": map[string]bool{"include_usage": true}})
	if err != nil {
		return benchSpeed{}, "", err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://127.0.0.1:%d/v1/chat/completions", port), bytes.NewReader(payload))
	if err != nil {
		return benchSpeed{}, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return benchSpeed{}, "", fmt.Errorf("stream request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return benchSpeed{}, "", fmt.Errorf("stream HTTP %d", resp.StatusCode)
	}
	return readBenchStream(resp.Body, start)
}

func readBenchStream(r io.Reader, start time.Time) (s benchSpeed, answer string, err error) {
	scanner := bufio.NewScanner(io.LimitReader(r, 4<<20))
	scanner.Buffer(make([]byte, 4096), benchContentLimit)
	var first, last time.Time
	var visible strings.Builder
	done := false
	for scanner.Scan() {
		data, ok := strings.CutPrefix(scanner.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			done = true
			break
		}
		var event struct {
			Error   json.RawMessage `json:"error"`
			Choices []struct {
				Finish string `json:"finish_reason"`
				Delta  struct {
					Content   string `json:"content"`
					Reasoning string `json:"reasoning_content"`
					Alternate string `json:"reasoning"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *struct {
				Prompt     *int `json:"prompt_tokens"`
				Completion *int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &event) != nil || (len(event.Error) > 0 && string(event.Error) != "null") {
			return s, "", errors.New("invalid or error benchmark stream event")
		}
		if event.Usage != nil {
			s.PromptTokens = event.Usage.Prompt
			s.CompletionTokens = event.Usage.Completion
		}
		if len(event.Choices) == 0 {
			continue
		}
		if len(event.Choices) != 1 {
			return s, "", errors.New("expected one choice")
		}
		choice := event.Choices[0]
		if choice.Finish != "" {
			s.FinishReason = choice.Finish
		}
		d := choice.Delta
		if d.Content == "" && d.Reasoning == "" && d.Alternate == "" {
			continue
		}
		now := time.Now()
		if first.IsZero() {
			first = now
			s.FirstDeltaMS = now.Sub(start).Milliseconds()
		}
		last = now
		if d.Content != "" {
			if s.FirstAnswerMS == nil {
				ms := now.Sub(start).Milliseconds()
				s.FirstAnswerMS = &ms
			}
			if visible.Len()+len(d.Content) > benchContentLimit {
				return s, "", errors.New("benchmark content oversized")
			}
			visible.WriteString(d.Content)
		}
	}
	if scanner.Err() != nil {
		return s, "", scanner.Err()
	}
	if !done || first.IsZero() || (s.FinishReason != "stop" && s.FinishReason != "length") {
		return s, "", errors.New("incomplete benchmark stream")
	}
	s.WallMS = time.Since(start).Milliseconds()
	// This is an endpoint estimate: one SSE event can contain multiple tokens.
	if s.CompletionTokens != nil && *s.CompletionTokens > 1 && last.After(first) {
		rate := float64(*s.CompletionTokens-1) / last.Sub(first).Seconds()
		s.DecodeEstimate = &rate
	}
	return s, visible.String(), nil
}

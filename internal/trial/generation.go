package trial

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// CheckGeneration verifies an actual answer from a local OpenAI-compatible
// endpoint. It never follows redirects or returns response content or secrets.
func CheckGeneration(ctx context.Context, port uint16, model, key string) error {
	return CheckGenerationTokens(ctx, port, model, key, 16)
}

// CheckGenerationTokens permits a larger budget for models that emit reasoning.
func CheckGenerationTokens(ctx context.Context, port uint16, model, key string, maxTokens int) error {
	if port == 0 || model == "" || maxTokens <= 0 || strings.ContainsAny(key, "\r\n") {
		return errors.New("invalid generation probe configuration")
	}
	payload, err := json.Marshal(map[string]any{
		"model": model, "messages": []map[string]string{{"role": "user", "content": "Reply with exactly: OK"}},
		"max_tokens": maxTokens, "temperature": 0, "stream": false,
		"chat_template_kwargs": map[string]bool{"enable_thinking": false},
	})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/chat/completions", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	transport := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return errors.New("generation probe request failed")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("generation probe HTTP %d", res.StatusCode)
	}
	const limit = 64 << 10
	body, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil || len(body) > limit {
		return errors.New("generation probe response unreadable or oversized")
	}
	var answer struct {
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &answer); err != nil || (len(answer.Error) > 0 && string(answer.Error) != "null") ||
		len(answer.Choices) != 1 || answer.Choices[0].FinishReason != "stop" || strings.TrimSpace(answer.Choices[0].Message.Content) != "OK" {
		return errors.New("generation probe did not return the expected final answer")
	}
	return nil
}

package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/arekbartnik/nats-workflows/engine"
)

const askModel = "qwen/qwen3.6-35b-a3b"

type AskState struct {
	Question string `json:"question"`
	Answer   string `json:"answer,omitempty"`
}

func Ask() engine.Workflow {
	return engine.Workflow{
		Name: "ask",
		Steps: []engine.Step{
			{Name: "ask-ai", Run: engine.Typed(askAI)},
		},
		Result: func(run *engine.Run) string {
			s, _ := engine.StateOf[AskState](run)
			return s.Answer
		},
	}
}

func askAI(ctx context.Context, run *engine.Run, s *AskState) error {
	key := os.Getenv("OPENROUTER_API_KEY")
	if key == "" {
		return errors.New("OPENROUTER_API_KEY not set (put it in .env)")
	}
	body, err := json.Marshal(map[string]any{
		"model":      askModel,
		"max_tokens": 1000, // without this OpenRouter reserves the model max, which can exceed small credit balances
		"messages": []map[string]string{
			{"role": "system", "content": "Respond to the user's question only in one sentence."},
			{"role": "user", "content": s.Question},
		},
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://openrouter.ai/api/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("openrouter: %s: %s", resp.Status, detail)
	}

	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if len(out.Choices) == 0 {
		return errors.New("openrouter returned no choices")
	}
	s.Answer = out.Choices[0].Message.Content
	return nil
}

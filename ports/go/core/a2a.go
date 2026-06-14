package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// A2AClient calls a peer agent's HTTP gateway (/agent + /.well-known/agent-card.json)
// with bounded retries + backoff.
type A2AClient struct {
	baseURL string
	retries int
	backoff time.Duration
	http    *http.Client
}

// NewA2AClient builds a client.
func NewA2AClient(baseURL string, retries int) *A2AClient {
	if retries < 0 {
		retries = 0
	}
	return &A2AClient{baseURL: strings.TrimRight(baseURL, "/"), retries: retries,
		backoff: 200 * time.Millisecond, http: &http.Client{Timeout: 30 * time.Second}}
}

// Card fetches the peer's Agent Card.
func (c *A2AClient) Card() (map[string]any, error) {
	resp, err := c.http.Get(c.baseURL + "/.well-known/agent-card.json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// Send delegates a turn to the peer and returns its JSON reply.
func (c *A2AClient) Send(conversationID, text, userID string) (map[string]any, error) {
	body, _ := json.Marshal(map[string]string{"conversation_id": conversationID, "text": text, "user_id": userID})
	var last error
	for attempt := 0; attempt <= c.retries; attempt++ {
		resp, err := c.http.Post(c.baseURL+"/agent", "application/json", bytes.NewReader(body))
		if err == nil {
			defer resp.Body.Close()
			var out map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&out); err == nil {
				return out, nil
			}
		}
		last = err
		if attempt < c.retries {
			time.Sleep(c.backoff * time.Duration(1<<attempt))
		}
	}
	return nil, fmt.Errorf("A2A call to %s/agent failed: %v", c.baseURL, last)
}

// PeerTool wraps a peer agent as a ToolRegistry tool (delegate a turn).
func PeerTool(baseURL string, retries int) ToolFunc {
	client := NewA2AClient(baseURL, retries)
	return func(params map[string]any) any {
		out, err := client.Send(
			fmt.Sprint(orDefault(params["conversation_id"], "a2a")),
			fmt.Sprint(orDefault(params["text"], "")),
			fmt.Sprint(orDefault(params["user_id"], "anonymous")))
		if err != nil {
			return map[string]any{"error": err.Error()}
		}
		return out
	}
}

func orDefault(v any, def string) any {
	if v == nil {
		return def
	}
	return v
}

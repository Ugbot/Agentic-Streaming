package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// ChatResult is one model step: a final Text answer, or a Tool call with Args — the
// portable analogue of the Flink ChatConnection result.
type ChatResult struct {
	Text string
	Tool string
	Args map[string]any
}

// IsToolCall reports whether this step calls a tool.
func (r ChatResult) IsToolCall() bool { return r.Tool != "" }

// TextResult / ToolCall are convenience constructors.
func TextResult(text string) ChatResult { return ChatResult{Text: text} }
func ToolCall(tool string, args map[string]any) ChatResult {
	if args == nil {
		args = map[string]any{}
	}
	return ChatResult{Tool: tool, Args: args}
}

// ChatClient turns (system prompt + transcript + tools) into a ChatResult. messages and
// tools are []map[string]string ({role,content} / {name,description}).
type ChatClient interface {
	Chat(messages []map[string]string, tools []map[string]string) ChatResult
}

const reactSystem = "You are a tool-using agent. On each step reply with ONE JSON object and nothing " +
	`else. To call a tool: {"tool": "<name>", "args": {<json args>}}. To give the final ` +
	`answer: {"text": "<answer>"}. Available tools: `

// LlmBrain is a Brain that drives a bounded ReAct loop over a ChatClient (thought →
// tool → observation → final). RuleBrain stays the default; LlmBrain is opt-in.
type LlmBrain struct {
	client        ChatClient
	name          string
	systemPrompt  string
	allowed       map[string]bool // nil => all tools
	maxIterations int
}

// NewLlmBrain builds an LLM brain. allowed may be nil (all tools); maxIterations<=0 => 6.
func NewLlmBrain(client ChatClient, name, systemPrompt string, allowed []string, maxIterations int) *LlmBrain {
	var set map[string]bool
	if allowed != nil {
		set = map[string]bool{}
		for _, t := range allowed {
			set[t] = true
		}
	}
	if maxIterations <= 0 {
		maxIterations = 6
	}
	return &LlmBrain{client: client, name: name, systemPrompt: systemPrompt, allowed: set, maxIterations: maxIterations}
}

// Turn implements Brain.
func (b *LlmBrain) Turn(userText string, ctx *AgentContext) string {
	var specs []map[string]string
	var toolList []string
	for _, s := range ctx.Tools.Specs() {
		if b.allowed == nil || b.allowed[s["name"]] {
			specs = append(specs, s)
			toolList = append(toolList, s["name"]+": "+s["description"])
		}
	}
	sys := strings.TrimSpace(b.systemPrompt + "\n" + reactSystem + strings.Join(toolList, ", "))
	msgs := []map[string]string{{"role": "system", "content": sys}}
	for _, m := range ctx.Store.History(ctx.ConversationID) {
		msgs = append(msgs, map[string]string{"role": m.Role, "content": m.Content})
	}
	if len(msgs) == 0 || msgs[len(msgs)-1]["content"] != userText {
		msgs = append(msgs, map[string]string{"role": "user", "content": userText})
	}

	for i := 0; i < b.maxIterations; i++ {
		r := b.client.Chat(msgs, specs)
		if r.IsToolCall() {
			obs := ctx.CallTool(r.Tool, r.Args)
			msgs = append(msgs, map[string]string{"role": "assistant", "content": fmt.Sprintf(`{"tool":%q}`, r.Tool)})
			msgs = append(msgs, map[string]string{"role": "tool", "content": fmt.Sprint(obs)})
			continue
		}
		if r.Text != "" {
			return "[" + b.name + "] " + r.Text
		}
		return "[" + b.name + "] (no answer)"
	}
	return fmt.Sprintf("[%s] (stopped after %d steps)", b.name, b.maxIterations)
}

// StubChatClient is a deterministic, scripted ChatClient for offline tests.
type StubChatClient struct {
	script []ChatResult
	i      int
}

// NewStubChatClient builds a scripted client (panics if empty).
func NewStubChatClient(script ...ChatResult) *StubChatClient {
	if len(script) == 0 {
		panic("NewStubChatClient needs at least one ChatResult")
	}
	return &StubChatClient{script: script}
}

// Chat returns the next scripted result, repeating the last once exhausted.
func (s *StubChatClient) Chat(messages []map[string]string, tools []map[string]string) ChatResult {
	idx := s.i
	if idx >= len(s.script) {
		idx = len(s.script) - 1
	}
	s.i++
	return s.script[idx]
}

// parseChatJSON parses a model reply that should be one JSON object; tolerant of prose.
func parseChatJSON(content string) ChatResult {
	s := strings.TrimSpace(content)
	start, end := strings.Index(s, "{"), strings.LastIndex(s, "}")
	if start != -1 && end > start {
		var obj map[string]any
		if json.Unmarshal([]byte(s[start:end+1]), &obj) == nil {
			if tool, ok := obj["tool"].(string); ok && tool != "" {
				args, _ := obj["args"].(map[string]any)
				return ToolCall(tool, args)
			}
			if text, ok := obj["text"].(string); ok {
				return TextResult(text)
			}
		}
	}
	return TextResult(s)
}

type httpChatClient struct {
	model     string
	url       string
	headers   map[string]string
	extract   func(map[string]any) string
	buildBody func(model string, messages []map[string]string) map[string]any
	http      *http.Client
}

func (c *httpChatClient) Chat(messages []map[string]string, tools []map[string]string) ChatResult {
	body, _ := json.Marshal(c.buildBody(c.model, messages))
	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		panic("chat call failed: " + err.Error())
	}
	defer resp.Body.Close()
	var parsed map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		panic("chat decode failed: " + err.Error())
	}
	return parseChatJSON(c.extract(parsed))
}

// NewOllamaChatClient talks to a local Ollama /api/chat endpoint (JSON mode).
func NewOllamaChatClient(model, baseURL string) ChatClient {
	if baseURL == "" {
		if env := os.Getenv("AGENTIC_OLLAMA_URL"); env != "" {
			baseURL = env
		} else {
			baseURL = "http://localhost:11434"
		}
	}
	return &httpChatClient{
		model: model, url: strings.TrimRight(baseURL, "/") + "/api/chat",
		http: &http.Client{Timeout: 60 * time.Second},
		buildBody: func(model string, messages []map[string]string) map[string]any {
			return map[string]any{"model": model, "messages": messages, "stream": false, "format": "json"}
		},
		extract: func(resp map[string]any) string {
			if msg, ok := resp["message"].(map[string]any); ok {
				if c, ok := msg["content"].(string); ok {
					return c
				}
			}
			return ""
		},
	}
}

// NewOpenAIChatClient talks to the OpenAI (or compatible) /chat/completions endpoint.
func NewOpenAIChatClient(model, apiKey, baseURL string) ChatClient {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if apiKey == "" {
		panic("OPENAI_API_KEY not set")
	}
	if baseURL == "" {
		if env := os.Getenv("OPENAI_BASE_URL"); env != "" {
			baseURL = env
		} else {
			baseURL = "https://api.openai.com/v1"
		}
	}
	return &httpChatClient{
		model: model, url: strings.TrimRight(baseURL, "/") + "/chat/completions",
		headers: map[string]string{"Authorization": "Bearer " + apiKey},
		http:    &http.Client{Timeout: 60 * time.Second},
		buildBody: func(model string, messages []map[string]string) map[string]any {
			return map[string]any{"model": model, "messages": messages, "response_format": map[string]any{"type": "json_object"}}
		},
		extract: func(resp map[string]any) string {
			if choices, ok := resp["choices"].([]any); ok && len(choices) > 0 {
				if choice, ok := choices[0].(map[string]any); ok {
					if msg, ok := choice["message"].(map[string]any); ok {
						if c, ok := msg["content"].(string); ok {
							return c
						}
					}
				}
			}
			return ""
		},
	}
}

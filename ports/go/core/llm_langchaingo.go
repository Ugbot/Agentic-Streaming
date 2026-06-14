package core

import (
	"context"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/ollama"
	"github.com/tmc/langchaingo/llms/openai"
)

// langChainGoChatClient is a real ChatClient via langchaingo (Ollama / OpenAI / …),
// using the same JSON-mode ReAct protocol as the rest of the framework so LlmBrain works
// unchanged.
type langChainGoChatClient struct {
	model    llms.Model
	jsonMode bool
}

// NewLangChainGoChatClient builds a real chat client. provider is "ollama" | "openai".
func NewLangChainGoChatClient(provider, model, baseURL string) (ChatClient, error) {
	var m llms.Model
	var err error
	switch provider {
	case "openai":
		opts := []openai.Option{}
		if model != "" {
			opts = append(opts, openai.WithModel(model))
		}
		if baseURL != "" {
			opts = append(opts, openai.WithBaseURL(baseURL))
		}
		m, err = openai.New(opts...)
	default: // ollama
		if model == "" {
			model = "llama3.2"
		}
		opts := []ollama.Option{ollama.WithModel(model), ollama.WithFormat("json")}
		if baseURL != "" {
			opts = append(opts, ollama.WithServerURL(baseURL))
		}
		m, err = ollama.New(opts...)
	}
	if err != nil {
		return nil, err
	}
	return &langChainGoChatClient{model: m, jsonMode: true}, nil
}

func (c *langChainGoChatClient) Chat(messages []map[string]string, tools []map[string]string) ChatResult {
	msgs := make([]llms.MessageContent, 0, len(messages))
	for _, m := range messages {
		msgs = append(msgs, llms.TextParts(roleOf(m["role"]), m["content"]))
	}
	opts := []llms.CallOption{}
	if c.jsonMode {
		opts = append(opts, llms.WithJSONMode())
	}
	resp, err := c.model.GenerateContent(context.Background(), msgs, opts...)
	if err != nil || len(resp.Choices) == 0 {
		if err != nil {
			return TextResult("[error] " + err.Error())
		}
		return TextResult("")
	}
	return parseChatJSON(resp.Choices[0].Content)
}

func roleOf(role string) llms.ChatMessageType {
	switch role {
	case "system":
		return llms.ChatMessageTypeSystem
	case "assistant":
		return llms.ChatMessageTypeAI
	case "tool":
		return llms.ChatMessageTypeTool
	default:
		return llms.ChatMessageTypeHuman
	}
}

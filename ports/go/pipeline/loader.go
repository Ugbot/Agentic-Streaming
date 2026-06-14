package pipeline

import (
	"context"
	"fmt"
	"os"

	"github.com/jagentic/goagentic/core"
	"github.com/jagentic/goagentic/engines/natsjs"
	"gopkg.in/yaml.v3"
)

// Runtime is the backend seam: process one turn.
type Runtime interface {
	Submit(core.Event) core.TurnResult
}

// System is a built, deployed agentic system on the chosen backend.
type System struct {
	BackendName string
	Built       Built
	rt          Runtime
}

// Submit runs a turn on the backend.
func (s *System) Submit(e core.Event) core.TurnResult { return s.rt.Submit(e) }

// Load reads a pipeline.yaml and builds the system (backend from YAML unless overridden).
func Load(path, backend string) (*System, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var spec map[string]any
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return BuildSystem(spec, backend)
}

// BuildSystem compiles a spec and wires it onto a backend.
func BuildSystem(spec map[string]any, backend string) (*System, error) {
	built, err := Build(spec, defaultChatClient)
	if err != nil {
		return nil, err
	}
	name := backend
	if name == "" {
		name = asString(spec["backend"], "local")
	}
	rt, err := makeBackend(name, built)
	if err != nil {
		return nil, err
	}
	return &System{BackendName: name, Built: built, rt: rt}, nil
}

type localBackend struct{ rt *core.LocalRuntime }

func (b localBackend) Submit(e core.Event) core.TurnResult { return b.rt.Submit(e) }

type natsBackend struct {
	rt  *natsjs.Runtime
	ctx context.Context
}

func (b natsBackend) Submit(e core.Event) core.TurnResult {
	res, err := b.rt.Submit(b.ctx, e)
	if err != nil {
		return core.TurnResult{ConversationID: e.ConversationID, Reply: "[error] " + err.Error(), OK: false}
	}
	return res
}

func makeBackend(name string, built Built) (Runtime, error) {
	switch name {
	case "", "local":
		return localBackend{core.NewLocalRuntime(built.Graph, nil, nil, built.Tools, built.Retriever)}, nil
	case "nats":
		rt := natsjs.New(built.Graph, built.Tools, built.Retriever)
		ctx := context.Background()
		if err := rt.Connect(ctx, os.Getenv("AGENTIC_NATS_URL")); err != nil {
			return nil, fmt.Errorf("nats backend: %w", err)
		}
		return natsBackend{rt: rt, ctx: ctx}, nil
	default:
		return nil, fmt.Errorf("backend %q not wired in Go (have: local, nats; temporal runs via a worker)", name)
	}
}

func defaultChatClient(llm map[string]any) core.ChatClient {
	switch asString(llm["provider"], "ollama") {
	case "ollama":
		return core.NewOllamaChatClient(asString(llm["model"], "qwen2.5:3b"), asString(llm["base_url"], ""))
	case "openai":
		return core.NewOpenAIChatClient(asString(llm["model"], "gpt-5.4-mini"), "", asString(llm["base_url"], ""))
	case "stub":
		var script []core.ChatResult
		for _, raw := range asList(llm["script"]) {
			step := asMap(raw)
			if tool := asString(step["tool"], ""); tool != "" {
				args := map[string]any{}
				for k, v := range asMap(step["args"]) {
					args[k] = v
				}
				script = append(script, core.ToolCall(tool, args))
			} else {
				script = append(script, core.TextResult(asString(step["text"], "ok")))
			}
		}
		if len(script) == 0 {
			script = append(script, core.TextResult("ok"))
		}
		return core.NewStubChatClient(script...)
	default:
		panic("unknown llm provider " + asString(llm["provider"], ""))
	}
}

// ---- spec casting helpers (yaml.v3 gives map[string]any / []any / int / float64) ----

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func asList(v any) []any {
	if l, ok := v.([]any); ok {
		return l
	}
	return nil
}

func asString(v any, def string) string {
	if s, ok := v.(string); ok {
		return s
	}
	return def
}

func asInt(v any, def int) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return def
}

func asFloat(v any, def float64) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	}
	return def
}

func asBool(v any) bool {
	b, _ := v.(bool)
	return b
}

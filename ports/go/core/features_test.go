package core

import (
	"strings"
	"testing"
)

// ---- skills ----

func TestSkillRegistryExpands(t *testing.T) {
	reg := NewSkillRegistry().Register(Skill{
		Name: "billing", Tools: []string{"get_balance", "refund"},
		PromptFragment: "Be precise about amounts.", RequiredFacts: []string{"account"}})
	tools, fragment, facts := reg.Expand([]string{"billing"})
	if len(tools) != 2 || tools[0] != "get_balance" {
		t.Fatalf("tools = %v", tools)
	}
	if !strings.Contains(fragment, "precise") || len(facts) != 1 {
		t.Fatalf("fragment/facts wrong: %q %v", fragment, facts)
	}
}

// ---- structured output ----

func TestValidateSchema(t *testing.T) {
	schema := map[string]any{"type": "object", "required": []any{"category", "amount"},
		"properties": map[string]any{"category": map[string]any{"type": "string"},
			"amount": map[string]any{"type": "number"}}}
	if errs := ValidateSchema(map[string]any{"category": "refund", "amount": 42.0}, schema); len(errs) != 0 {
		t.Fatalf("valid object rejected: %v", errs)
	}
	if errs := ValidateSchema(map[string]any{"category": "refund"}, schema); len(errs) == 0 {
		t.Fatal("missing field not caught")
	}
	if errs := ValidateSchema(map[string]any{"category": 1.0, "amount": "x"}, schema); len(errs) != 2 {
		t.Fatalf("type errors = %v", errs)
	}
}

func TestParseStructuredToleratesProse(t *testing.T) {
	schema := map[string]any{"type": "object", "required": []any{"ok"},
		"properties": map[string]any{"ok": map[string]any{"type": "boolean"}}}
	obj, errs := ParseStructured(`here you go: {"ok": true} done`, schema)
	if len(errs) != 0 || obj["ok"] != true {
		t.Fatalf("parse failed: %v %v", obj, errs)
	}
}

func TestLlmBrainOutputSchemaReturnsValidatedJSON(t *testing.T) {
	schema := map[string]any{"type": "object", "required": []any{"category"},
		"properties": map[string]any{"category": map[string]any{"type": "string"}}}
	brain := NewLlmBrain(NewStubChatClient(TextResult(`{"category":"refund"}`)), "triage", "", nil, 6).
		WithOutputSchema(schema)
	res := NewAgent("triage", "p", brain).Turn(NewEvent("c1", "u", "refund please"), llmCtx(NewToolRegistry()))
	if res.Reply != `[triage] {"category":"refund"}` {
		t.Fatalf("reply = %q", res.Reply)
	}
}

// ---- richer listeners ----

func TestToolCallAndCompositeListenerHooksFire(t *testing.T) {
	m1, m2 := NewMetricsListener(), NewMetricsListener()
	composite := NewCompositeListener(m1, m2)
	tools := NewToolRegistry().Register("get_balance", "balance", func(p map[string]any) any { return 1234.56 })
	pay := NewAgent("payments", "p", NewLlmBrain(NewStubChatClient(
		ToolCall("get_balance", nil), TextResult("done")), "payments", "", nil, 6))
	graph := NewRoutedGraph(func(ev Event, ctx *AgentContext) string { return "payments" },
		map[string]*Agent{"payments": pay}, nil).WithListeners(composite)
	graph.Handle(NewEvent("c1", "u", "balance?"), llmCtx(tools))
	for _, m := range []*MetricsListener{m1, m2} {
		if m.Turns != 1 || m.ToolCalls != 1 || m.PathCount("payments") != 1 {
			t.Fatalf("metrics = turns:%d tools:%d paths:%v", m.Turns, m.ToolCalls, m.Paths)
		}
	}
}

func TestListenerErrorHookOnToolPanic(t *testing.T) {
	m := NewMetricsListener()
	tools := NewToolRegistry().Register("boom", "fails", func(p map[string]any) any { panic("tool exploded") })
	pay := NewAgent("payments", "p", NewLlmBrain(NewStubChatClient(
		ToolCall("boom", nil), TextResult("x")), "payments", "", nil, 6))
	graph := NewRoutedGraph(func(ev Event, ctx *AgentContext) string { return "payments" },
		map[string]*Agent{"payments": pay}, nil).WithListeners(m)
	func() {
		defer func() { recover() }()
		graph.Handle(NewEvent("c1", "u", "go"), llmCtx(tools))
	}()
	if m.Errors != 1 {
		t.Fatalf("errors = %d", m.Errors)
	}
}

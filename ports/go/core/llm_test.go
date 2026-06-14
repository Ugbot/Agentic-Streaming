package core

import (
	"strings"
	"testing"
)

func llmCtx(tools *ToolRegistry) *AgentContext {
	return &AgentContext{
		ConversationID: "c1", UserID: "alice",
		Store: NewInMemoryConversationStore(0), State: NewInMemoryKeyedStateStore(),
		Tools: tools,
	}
}

func TestLlmBrainRunsReactToolThenFinal(t *testing.T) {
	tools := NewToolRegistry().Register("get_balance", "Look up balance", func(p map[string]any) any { return 1234.56 })
	stub := NewStubChatClient(
		ToolCall("get_balance", map[string]any{"user": "alice"}),
		TextResult("Your balance is 1234.56."),
	)
	brain := NewLlmBrain(stub, "payments", "", []string{"get_balance"}, 6)
	agent := NewAgent("payments", "You answer payment questions.", brain)
	res := agent.Turn(NewEvent("c1", "alice", "what is my balance?"), llmCtx(tools))
	if !contains(res.ToolCalls, "get_balance") {
		t.Fatalf("get_balance not called: %v", res.ToolCalls)
	}
	if res.Reply != "[payments] Your balance is 1234.56." {
		t.Fatalf("reply = %q", res.Reply)
	}
}

func TestLlmBrainDirectFinalNoTool(t *testing.T) {
	brain := NewLlmBrain(NewStubChatClient(TextResult("Hello!")), "general", "", nil, 6)
	res := NewAgent("general", "p", brain).Turn(NewEvent("c1", "u", "hi"), llmCtx(NewToolRegistry()))
	if res.Reply != "[general] Hello!" || len(res.ToolCalls) != 0 {
		t.Fatalf("unexpected: %q tools=%v", res.Reply, res.ToolCalls)
	}
}

func TestLlmBrainInRoutedGraph(t *testing.T) {
	tools := NewToolRegistry().Register("get_balance", "balance", func(p map[string]any) any { return 1234.56 })
	pay := NewAgent("payments", "p", NewLlmBrain(NewStubChatClient(
		ToolCall("get_balance", nil), TextResult("It is 1234.56.")), "payments", "", nil, 6))
	general := NewAgent("general", "p", NewLlmBrain(NewStubChatClient(TextResult("hi")), "general", "", nil, 6))
	graph := NewRoutedGraph(
		func(ev Event, ctx *AgentContext) string {
			if strings.Contains(strings.ToLower(ev.Text), "balance") {
				return "payments"
			}
			return "general"
		},
		map[string]*Agent{"payments": pay, "general": general},
		func(reply string, ctx *AgentContext) (bool, string) { return strings.HasPrefix(reply, "["), reply },
	)
	res := graph.Handle(NewEvent("c1", "alice", "what is my balance?"), llmCtx(tools))
	if res.Path != "payments" || !res.OK || !contains(res.ToolCalls, "get_balance") || !strings.Contains(res.Reply, "1234.56") {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestParseChatJSON(t *testing.T) {
	if !parseChatJSON(`{"tool":"t","args":{"x":1}}`).IsToolCall() {
		t.Fatal("should be tool call")
	}
	if got := parseChatJSON(`prefix {"text":"hi"} suffix`).Text; got != "hi" {
		t.Fatalf("text = %q", got)
	}
	if got := parseChatJSON("plain").Text; got != "plain" {
		t.Fatalf("freeform text = %q", got)
	}
}

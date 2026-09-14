package temporal

import (
	"strings"
	"testing"
	"time"

	"github.com/jagentic/goagentic/core"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// signalAt schedules a "turn" signal at the given delay.
func turnAt(env *testsuite.TestWorkflowEnvironment, d time.Duration, text, user string) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(TurnSignal, TurnRequest{Text: text, UserID: user})
	}, d)
}

func TestBankingWorkflowRoutesPersistsAndTools(t *testing.T) {
	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(BankingConversationWorkflow)

	// Three turns to the SAME conversation, then close — proving ordered, durable,
	// event-sourced state across turns (C1+C2+C3) on an in-memory Temporal service.
	turnAt(env, 1*time.Millisecond, "what card types do you offer?", "demo")
	turnAt(env, 2*time.Millisecond, "tell me about crypto cash-back", "demo")
	turnAt(env, 3*time.Millisecond, "what is my balance?", "demo")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(CloseSignal, "") }, 4*time.Millisecond)

	env.ExecuteWorkflow(BankingConversationWorkflow, "c1")

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	var replies []TurnReply
	rv, err := env.QueryWorkflow(RepliesQuery)
	if err != nil {
		t.Fatalf("query replies: %v", err)
	}
	if err := rv.Get(&replies); err != nil {
		t.Fatalf("decode replies: %v", err)
	}
	if len(replies) != 3 {
		t.Fatalf("want 3 replies, got %d", len(replies))
	}
	if replies[0].Path != "cards" || replies[1].Path != "cards" {
		t.Fatalf("first two turns should route to cards: %+v", replies)
	}
	if replies[2].Path != "payments" || !strings.Contains(replies[2].Reply, "1234.56") {
		t.Fatalf("balance turn should be payments+tool: %+v", replies[2])
	}
	if !containsStr(replies[2].ToolCalls, "get_balance") {
		t.Fatalf("get_balance not called: %v", replies[2].ToolCalls)
	}

	var count int
	cv, err := env.QueryWorkflow(MessageCountQuery)
	if err != nil {
		t.Fatalf("query count: %v", err)
	}
	cv.Get(&count)
	if count != 6 { // three turns: user+assistant x3, durable in event-sourced state
		t.Fatalf("want 6 durable messages, got %d", count)
	}
}

func TestExtendedCoreGraphFlowsThroughTheWorkflow(t *testing.T) {
	tools := core.DefaultBankingTools().Register("freeze_card", "Freeze the user's card",
		func(p map[string]any) any { return "FRZ-" + strings_(p["user"]) })
	fraud := core.BrainFunc(func(userText string, ctx *core.AgentContext) string {
		ref := ctx.CallTool("freeze_card", map[string]any{"user": ctx.UserID})
		return "[fraud] Your card is frozen (ref " + strings_(ref) + ")."
	})
	paths := map[string]*core.Agent{
		"cards":    core.NewAgent("cards", "cards", core.RuleBrain{Name: "cards"}),
		"payments": core.NewAgent("payments", "payments", core.RuleBrain{Name: "payments"}),
		"general":  core.NewAgent("general", "general", core.RuleBrain{Name: "general"}),
		"fraud":    core.NewAgent("fraud", "fraud", fraud),
	}
	router := func(ev core.Event, ctx *core.AgentContext) string {
		low := strings.ToLower(ev.Text)
		if strings.Contains(low, "stolen") || strings.Contains(low, "freeze") {
			return "fraud"
		}
		return core.BankingRouter(ev, ctx)
	}
	graph := core.NewRoutedGraph(router, paths, func(reply string, ctx *core.AgentContext) (bool, string) {
		return strings.HasPrefix(reply, "["), reply
	})

	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	ext := MakeConversationWorkflow(graph, tools, core.BankingRetriever())
	env.RegisterWorkflowWithOptions(ext, workflow.RegisterOptions{Name: "ConversationWorkflowExtended"})

	turnAt(env, 1*time.Millisecond, "my card was stolen, please freeze it", "alice")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(CloseSignal, "") }, 2*time.Millisecond)

	env.ExecuteWorkflow("ConversationWorkflowExtended", "c-fraud")

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow not completed cleanly: %v", env.GetWorkflowError())
	}
	var replies []TurnReply
	rv, err := env.QueryWorkflow(RepliesQuery)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	rv.Get(&replies)
	if len(replies) != 1 || replies[0].Path != "fraud" || !strings.Contains(replies[0].Reply, "FRZ-alice") {
		t.Fatalf("extended graph did not flow through the workflow: %+v", replies)
	}
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func strings_(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

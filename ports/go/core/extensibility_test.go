package core

import (
	"fmt"
	"strings"
	"testing"
)

// ExtendedBankingTools returns the default tools PLUS a new freeze_card tool — added
// purely through the public API, no framework edits.
func ExtendedBankingTools() *ToolRegistry {
	return DefaultBankingTools().Register("freeze_card", "Freeze the user's card",
		func(p map[string]any) any { return "FRZ-" + fmt.Sprint(p["user"]) })
}

// ExtendedBankingGraph is the banking graph PLUS a brand-new 'fraud' path, with a router
// that prefers it. Reuses the framework's RoutedGraph/Agent verbatim.
func ExtendedBankingGraph() *RoutedGraph {
	fraud := BrainFunc(func(userText string, ctx *AgentContext) string {
		ref := ctx.CallTool("freeze_card", map[string]any{"user": ctx.UserID})
		return fmt.Sprintf("[fraud] Your card is frozen (ref %v). A specialist will call you.", ref)
	})
	paths := map[string]*Agent{
		"cards":    NewAgent("cards", "cards", RuleBrain{Name: "cards"}),
		"payments": NewAgent("payments", "payments", RuleBrain{Name: "payments"}),
		"general":  NewAgent("general", "general", RuleBrain{Name: "general"}),
		"fraud":    NewAgent("fraud", "fraud", fraud),
	}
	router := func(event Event, ctx *AgentContext) string {
		low := strings.ToLower(event.Text)
		if strings.Contains(low, "stolen") || strings.Contains(low, "fraud") || strings.Contains(low, "freeze") {
			return "fraud"
		}
		return BankingRouter(event, ctx)
	}
	verifier := func(reply string, ctx *AgentContext) (bool, string) {
		return strings.HasPrefix(reply, "["), reply
	}
	return NewRoutedGraph(router, paths, verifier)
}

func extendedRuntime() *LocalRuntime {
	return NewLocalRuntime(ExtendedBankingGraph(), nil, nil, ExtendedBankingTools(), BankingRetriever())
}

func TestNewPathAndToolReachableViaPublicAPI(t *testing.T) {
	rt := extendedRuntime()
	res := rt.Submit(NewEvent("c-fraud", "alice", "my card was stolen, please freeze it"))
	if res.Path != "fraud" || !res.OK {
		t.Fatalf("want fraud/ok, got %s/%v", res.Path, res.OK)
	}
	if !strings.Contains(strings.ToLower(res.Reply), "frozen") {
		t.Fatalf("unexpected fraud reply: %s", res.Reply)
	}
	if !contains(res.ToolCalls, "freeze_card") {
		t.Fatalf("freeze_card not called: %v", res.ToolCalls)
	}
	if !strings.Contains(res.Reply, "FRZ-alice") {
		t.Fatalf("new tool did not execute: %s", res.Reply)
	}
	if v, _ := rt.Store().GetAttribute("c-fraud", PathAttr); v != "fraud" {
		t.Fatalf("path attr should be fraud, got %s", v)
	}
}

func TestExistingPathsStillWorkAfterExtension(t *testing.T) {
	rt := extendedRuntime()
	if p := rt.Submit(NewEvent("c1", "bob", "what card types do you offer?")).Path; p != "cards" {
		t.Fatalf("cards regressed: %s", p)
	}
	pay := rt.Submit(NewEvent("c2", "bob", "what is my balance?"))
	if pay.Path != "payments" || !contains(pay.ToolCalls, "get_balance") {
		t.Fatalf("payments/get_balance regressed: %s %v", pay.Path, pay.ToolCalls)
	}
	if p := rt.Submit(NewEvent("c3", "bob", "hello there")).Path; p != "general" {
		t.Fatalf("general regressed: %s", p)
	}
}

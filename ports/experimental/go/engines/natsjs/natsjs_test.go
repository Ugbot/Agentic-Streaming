package natsjs

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jagentic/goagentic/core"
	"github.com/nats-io/nats.go"
)

// connectOrSkip dials NATS, skipping the test if no JetStream server is reachable.
func connectOrSkip(t *testing.T, rt *Runtime) {
	t.Helper()
	url := os.Getenv("AGENTIC_NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	// Fast fail when there's no server, so the test skips instead of hanging.
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second), nats.RetryOnFailedConnect(false))
	if err != nil {
		t.Skipf("no NATS server reachable at %s: %v", url, err)
	}
	nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Connect(ctx, url); err != nil {
		t.Skipf("JetStream connect failed: %v", err)
	}
}

func uniqueID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, rand.Int63())
}

func TestSubmitRoutesAndPersistsToKV(t *testing.T) {
	rt := New(nil, nil, nil)
	connectOrSkip(t, rt)
	defer rt.Close()
	ctx := context.Background()

	cid := uniqueID("t1")
	r1, err := rt.Submit(ctx, core.NewEvent(cid, "demo", "what card types do you offer?"))
	if err != nil {
		t.Fatalf("submit1: %v", err)
	}
	if r1.Path != "cards" {
		t.Fatalf("want cards, got %s", r1.Path)
	}
	if _, err := rt.Submit(ctx, core.NewEvent(cid, "demo", "tell me about crypto cash-back")); err != nil {
		t.Fatalf("submit2: %v", err)
	}

	count, err := rt.MessageCount(ctx, cid)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 4 { // two turns: user+assistant x2, durable in JetStream KV
		t.Fatalf("want 4 durable messages, got %d", count)
	}

	bal, err := rt.Submit(ctx, core.NewEvent(uniqueID("t2"), "demo", "what is my balance?"))
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if bal.Path != "payments" || !strings.Contains(bal.Reply, "1234.56") {
		t.Fatalf("balance turn wrong: %+v", bal)
	}
}

func TestExtendedGraphFlowsThroughTheNatsSeam(t *testing.T) {
	tools := core.DefaultBankingTools().Register("freeze_card", "Freeze the user's card",
		func(p map[string]any) any { return "FRZ-" + fmt.Sprint(p["user"]) })
	fraud := core.BrainFunc(func(userText string, ctx *core.AgentContext) string {
		ref := ctx.CallTool("freeze_card", map[string]any{"user": ctx.UserID})
		return fmt.Sprintf("[fraud] Your card is frozen (ref %v).", ref)
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

	rt := New(graph, tools, core.BankingRetriever())
	connectOrSkip(t, rt)
	defer rt.Close()

	res, err := rt.Submit(context.Background(), core.NewEvent(uniqueID("tf"), "alice", "my card was stolen, please freeze it"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.Path != "fraud" || !strings.Contains(res.Reply, "FRZ-alice") {
		t.Fatalf("extended graph did not flow through NATS seam: %+v", res)
	}
}

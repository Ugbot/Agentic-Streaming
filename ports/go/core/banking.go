package core

import (
	"fmt"
	"sort"
	"strings"
)

// BankingDim is the embedding dimension used by the banking example.
const BankingDim = 256

// BankingKB is a tiny knowledge base the cards/payments paths retrieve from.
var BankingKB = map[string]string{
	"kb_cards_types":      "We offer three card types: classic, gold, and platinum, each with different fees.",
	"kb_cards_crypto":     "Crypto cash-back can be redeemed to a linked wallet or a manual address.",
	"kb_payments_limits":  "Daily transfer limits are 10,000 by default; raise them in settings.",
	"kb_payments_dispute": "To dispute a charge, open the transaction and tap Dispute within 60 days.",
}

// SeedKB populates a hot index with the banking KB (deterministic insertion order).
func SeedKB(index HotVectorIndex) {
	ids := make([]string, 0, len(BankingKB))
	for id := range BankingKB {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		index.Upsert(id, Embed(BankingKB[id], BankingDim), BankingKB[id])
	}
}

// RuleBrain is a deterministic brain: keyword rules + an optional tool call / retrieval.
// It stands in for an LLM ReAct loop so the port runs and is testable with no model.
type RuleBrain struct {
	Name string
}

// Turn implements Brain.
func (b RuleBrain) Turn(userText string, ctx *AgentContext) string {
	low := strings.ToLower(userText)
	if strings.Contains(low, "balance") {
		bal := ctx.CallTool("get_balance", map[string]any{"user": ctx.UserID})
		return fmt.Sprintf("[%s] Your balance is %v.", b.Name, bal)
	}
	if ctx.Retriever != nil {
		hits := ctx.Retriever.Retrieve(Embed(userText, BankingDim), 1)
		if len(hits) > 0 && hits[0].Score > 0.15 {
			return fmt.Sprintf("[%s] %s", b.Name, hits[0].Text)
		}
	}
	return fmt.Sprintf("[%s] I can help with %s questions. You said: %q", b.Name, b.Name, userText)
}

// BankingRouter classifies a request into cards / payments / general.
func BankingRouter(event Event, ctx *AgentContext) string {
	low := strings.ToLower(event.Text)
	for _, w := range []string{"card", "crypto", "cash-back", "cashback"} {
		if strings.Contains(low, w) {
			return "cards"
		}
	}
	for _, w := range []string{"transfer", "payment", "dispute", "charge", "limit", "balance"} {
		if strings.Contains(low, w) {
			return "payments"
		}
	}
	return "general"
}

// BuildBankingGraph assembles the router -> path -> verifier banking graph.
func BuildBankingGraph() *RoutedGraph {
	paths := map[string]*Agent{
		"cards":    NewAgent("cards", "You answer card questions.", RuleBrain{Name: "cards"}),
		"payments": NewAgent("payments", "You answer payment questions.", RuleBrain{Name: "payments"}),
		"general":  NewAgent("general", "You answer general questions.", RuleBrain{Name: "general"}),
	}
	verifier := func(reply string, ctx *AgentContext) (bool, string) {
		return strings.HasPrefix(reply, "["), reply
	}
	return NewRoutedGraph(BankingRouter, paths, verifier)
}

// DefaultBankingTools returns the tool registry with get_balance.
func DefaultBankingTools() *ToolRegistry {
	return NewToolRegistry().Register("get_balance", "Look up the user's balance",
		func(params map[string]any) any { return 1234.56 })
}

// BankingRetriever returns a hot-tier retriever seeded with the KB (cold = nil).
func BankingRetriever() *TwoTierRetriever {
	hot := NewInMemoryHotVectorIndex()
	SeedKB(hot)
	return NewTwoTierRetriever(hot, nil, 4, 4)
}

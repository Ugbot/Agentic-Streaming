package core

import (
	"fmt"
	"strings"
	"testing"
)

func bankingRuntime() *LocalRuntime { return NewBankingRuntime() }

func TestConversationStoreTranscriptAndAttrs(t *testing.T) {
	s := NewInMemoryConversationStore(5)
	cid := "c-test"
	for i := 0; i < 8; i++ {
		s.Append(cid, UserMessage(fmt.Sprintf("m%d", i)))
	}
	if got := s.MessageCount(cid); got != 5 {
		t.Fatalf("bounded transcript: want 5, got %d", got)
	}
	if got := s.History(cid)[4].Content; got != "m7" {
		t.Fatalf("want newest m7, got %s", got)
	}
	s.PutAttribute(cid, PhaseAttr, "router")
	if v, ok := s.GetAttribute(cid, PhaseAttr); !ok || v != "router" {
		t.Fatalf("attribute roundtrip failed: %v %v", v, ok)
	}
	s.AssociateUser(cid, "alice")
	if !contains(s.ConversationsForUser("alice"), cid) {
		t.Fatalf("user index missing %s", cid)
	}
}

func TestRetrievalHotKnnAndTwoTierDedup(t *testing.T) {
	hot := NewInMemoryHotVectorIndex()
	hot.Upsert("h1", Embed("the cat sat on the mat", 64), "cat on mat")
	hot.Upsert("h2", Embed("quantum physics lecture", 64), "physics")
	top := hot.Search(Embed("where is the cat and the mat", 64), 2)
	if top[0].ID != "h1" {
		t.Fatalf("want h1 top, got %s", top[0].ID)
	}
	if top[0].Score <= 0.3 {
		t.Fatalf("expected a strong match, got %f", top[0].Score)
	}

	hot.Upsert("shared", Embed("the cat sat on the mat", 64), "shared HOT")
	cold := func(q []float64, k int) []Scored {
		return []Scored{{ID: "c1", Score: 0.4, Text: "cold doc"}, {ID: "shared", Score: 0.05, Text: "shared COLD"}}
	}
	r := NewTwoTierRetriever(hot, cold, 5, 5)
	merged := r.Retrieve(Embed("cat mat", 64), 10)
	ids := map[string]Scored{}
	for _, m := range merged {
		ids[m.ID] = m
	}
	if _, ok := ids["h1"]; !ok {
		t.Fatal("merged missing h1")
	}
	if _, ok := ids["c1"]; !ok {
		t.Fatal("merged missing cold c1")
	}
	count := 0
	for _, m := range merged {
		if m.ID == "shared" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("dedup failed: shared appears %d times", count)
	}
	if ids["shared"].Text != "shared HOT" {
		t.Fatalf("hot should win text, got %q", ids["shared"].Text)
	}
}

func TestRoutedGraphRoutesPersistsPhaseAndCallsTool(t *testing.T) {
	rt := bankingRuntime()
	res := rt.Submit(NewEvent("c-cards", "demo", "what card types do you offer?"))
	if res.Path != "cards" || !res.OK {
		t.Fatalf("want cards/ok, got %s/%v", res.Path, res.OK)
	}
	if !strings.Contains(strings.ToLower(res.Reply), "card") {
		t.Fatalf("unexpected reply: %s", res.Reply)
	}
	if v, _ := rt.Store().GetAttribute("c-cards", PhaseAttr); v != "done" {
		t.Fatalf("phase should be done, got %s", v)
	}
	if v, _ := rt.Store().GetAttribute("c-cards", PathAttr); v != "cards" {
		t.Fatalf("path attr should be cards, got %s", v)
	}

	bal := rt.Submit(NewEvent("c-pay", "carol", "what is my balance?"))
	if bal.Path != "payments" {
		t.Fatalf("balance should route to payments, got %s", bal.Path)
	}
	if !strings.Contains(bal.Reply, "1234.56") {
		t.Fatalf("balance reply missing amount: %s", bal.Reply)
	}
	if !contains(bal.ToolCalls, "get_balance") {
		t.Fatalf("get_balance not called: %v", bal.ToolCalls)
	}
}

func TestPerConversationIsolationAndUserIndex(t *testing.T) {
	rt := bankingRuntime()
	rt.Submit(NewEvent("conv-a", "u1", "card help"))
	rt.Submit(NewEvent("conv-b", "u1", "transfer limit"))
	forU1 := rt.Store().ConversationsForUser("u1")
	if !contains(forU1, "conv-a") || !contains(forU1, "conv-b") {
		t.Fatalf("user index incomplete: %v", forU1)
	}
	if v, _ := rt.Store().GetAttribute("conv-a", PathAttr); v != "cards" {
		t.Fatalf("conv-a should be cards, got %s", v)
	}
	if v, _ := rt.Store().GetAttribute("conv-b", PathAttr); v != "payments" {
		t.Fatalf("conv-b should be payments, got %s", v)
	}
}

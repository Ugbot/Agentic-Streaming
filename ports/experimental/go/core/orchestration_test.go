package core

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSagaRollsBackInReverseOnFailure(t *testing.T) {
	var log []string
	s := NewSaga()
	_ = s.Step("charge", func() error { log = append(log, "charge"); return nil }, func() { log = append(log, "refund") })
	_ = s.Step("ship", func() error { log = append(log, "ship"); return nil }, func() { log = append(log, "cancel-ship") })
	err := s.Step("reserve", func() error { return errFn() }, func() { log = append(log, "unreserve") })
	if err == nil {
		t.Fatal("expected step error")
	}
	// reserve do failed (no undo recorded); ship + charge undo in reverse
	want := []string{"charge", "ship", "cancel-ship", "refund"}
	if len(log) != 4 || log[2] != "cancel-ship" || log[3] != "refund" {
		t.Fatalf("log = %v, want %v", log, want)
	}
}

func errFn() error { return &simpleErr{"inventory gone"} }

type simpleErr struct{ s string }

func (e *simpleErr) Error() string { return e.s }

func TestContextCompactionMoscow(t *testing.T) {
	items := []ContextItem{
		{Text: repeat("M", 40), Priority: PriorityMust},
		{Text: repeat("S", 40), Priority: PriorityShould},
		{Text: repeat("C", 40), Priority: PriorityCould},
		{Text: repeat("W", 40), Priority: PriorityWont},
	}
	kept := NewContextWindowManager(22).Compact(items)
	var prios []Priority
	for _, it := range kept {
		prios = append(prios, it.Priority)
	}
	hasMust, hasShould, hasCould, hasWont := false, false, false, false
	for _, p := range prios {
		switch p {
		case PriorityMust:
			hasMust = true
		case PriorityShould:
			hasShould = true
		case PriorityCould:
			hasCould = true
		case PriorityWont:
			hasWont = true
		}
	}
	if !hasMust || !hasShould || hasCould || hasWont {
		t.Fatalf("kept priorities = %v (want MUST+SHOULD only)", prios)
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func TestA2AClientAndPeerTool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/.well-known/agent-card.json" {
			json.NewEncoder(w).Encode(map[string]any{"name": "Stub Agent"})
			return
		}
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		json.NewEncoder(w).Encode(map[string]any{"reply": "echo: " + body["text"], "ok": true})
	}))
	defer srv.Close()

	client := NewA2AClient(srv.URL, 2)
	card, err := client.Card()
	if err != nil || card["name"] != "Stub Agent" {
		t.Fatalf("card = %v err = %v", card, err)
	}
	out, err := client.Send("c1", "hello", "alice")
	if err != nil || out["reply"] != "echo: hello" {
		t.Fatalf("send = %v err = %v", out, err)
	}
	tool := PeerTool(srv.URL, 2)
	res := tool(map[string]any{"conversation_id": "c1", "text": "delegate this"}).(map[string]any)
	if res["reply"] != "echo: delegate this" {
		t.Fatalf("peer tool = %v", res)
	}
}

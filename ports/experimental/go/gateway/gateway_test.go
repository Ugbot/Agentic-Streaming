package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jagentic/goagentic/core"
)

func newServer() *httptest.Server {
	g := New(core.NewBankingRuntime(), "local")
	return httptest.NewServer(g.Handler())
}

func randID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, rand.Int63())
}

func postAgent(t *testing.T, base, cid, text string) turnResponse {
	t.Helper()
	body, _ := json.Marshal(turnRequest{ConversationID: cid, Text: text, UserID: "demo"})
	resp, err := http.Post(base+"/agent", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /agent: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /agent status %d", resp.StatusCode)
	}
	var out turnResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func TestHealthAndCard(t *testing.T) {
	srv := newServer()
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", err, resp.StatusCode)
	}
	var health map[string]string
	json.NewDecoder(resp.Body).Decode(&health)
	if health["status"] != "ok" {
		t.Fatalf("health not ok: %v", health)
	}

	cresp, _ := http.Get(srv.URL + "/.well-known/agent-card.json")
	var card AgentCard
	json.NewDecoder(cresp.Body).Decode(&card)
	if card.Name == "" || len(card.Skills) == 0 || card.Skills[0].ID != "banking" {
		t.Fatalf("bad agent card: %+v", card)
	}
	if card.URL != "/agent" {
		t.Fatalf("card url should be /agent, got %s", card.URL)
	}
}

func TestAgentRoutingRandomized(t *testing.T) {
	srv := newServer()
	defer srv.Close()

	cardQs := []string{"what card types do you offer?", "tell me about crypto cash-back", "is there a gold card?"}
	if r := postAgent(t, srv.URL, randID("c"), cardQs[rand.Intn(len(cardQs))]); r.Path != "cards" {
		t.Fatalf("expected cards, got %s", r.Path)
	}

	bal := postAgent(t, srv.URL, randID("c"), "what is my balance?")
	if bal.Path != "payments" {
		t.Fatalf("expected payments, got %s", bal.Path)
	}
	if !strings.Contains(bal.Reply, "1234.56") || !contains(bal.ToolCalls, "get_balance") {
		t.Fatalf("balance turn wrong: %+v", bal)
	}

	greetings := []string{"hello there", "good morning", "who are you?"}
	if r := postAgent(t, srv.URL, randID("c"), greetings[rand.Intn(len(greetings))]); r.Path != "general" {
		t.Fatalf("expected general, got %s", r.Path)
	}
}

func TestTranscriptAccumulatesAndIsolated(t *testing.T) {
	srv := newServer()
	defer srv.Close()

	cid := randID("conv")
	postAgent(t, srv.URL, cid, "what card types do you offer?")
	postAgent(t, srv.URL, cid, "tell me about crypto cash-back")

	resp, _ := http.Get(srv.URL + "/conversations/" + cid)
	var tr transcriptResponse
	json.NewDecoder(resp.Body).Decode(&tr)
	if tr.MessageCount != 4 { // two turns: user+assistant x2
		t.Fatalf("expected 4 messages, got %d", tr.MessageCount)
	}

	other := randID("conv")
	oresp, _ := http.Get(srv.URL + "/conversations/" + other)
	var otr transcriptResponse
	json.NewDecoder(oresp.Body).Decode(&otr)
	if otr.MessageCount != 0 {
		t.Fatalf("isolation broken: other conv has %d messages", otr.MessageCount)
	}
}

func TestAgentValidation(t *testing.T) {
	srv := newServer()
	defer srv.Close()

	body, _ := json.Marshal(turnRequest{ConversationID: "c1"}) // missing text
	resp, _ := http.Post(srv.URL+"/agent", "application/json", bytes.NewReader(body))
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for missing text, got %d", resp.StatusCode)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Package gateway is a dependency-free (stdlib net/http) HTTP gateway over a Go core
// Runtime — the Go peer of the FastAPI gateway in ports/gateway-fastapi. It exposes the
// banking agent as an A2A-style HTTP service: an Agent Card, a /agent turn endpoint, a
// transcript endpoint, and a health check.
package gateway

import (
	"encoding/json"
	"net/http"

	"github.com/jagentic/goagentic/core"
)

// Runtime is anything that can process a turn — core.LocalRuntime satisfies it, as do
// the NATS/Temporal adapters.
type Runtime interface {
	Submit(core.Event) core.TurnResult
}

// Storer is an optional capability: a Runtime that exposes its ConversationStore so the
// gateway can serve transcripts.
type Storer interface {
	Store() core.ConversationStore
}

// Gateway wires a Runtime behind HTTP handlers.
type Gateway struct {
	rt      Runtime
	backend string
}

// New builds a Gateway. backend is a label reported by /healthz and the card.
func New(rt Runtime, backend string) *Gateway {
	return &Gateway{rt: rt, backend: backend}
}

// --- wire types ---

type turnRequest struct {
	ConversationID string `json:"conversation_id"`
	Text           string `json:"text"`
	UserID         string `json:"user_id"`
}

type turnResponse struct {
	ConversationID string   `json:"conversation_id"`
	Reply          string   `json:"reply"`
	Path           string   `json:"path"`
	OK             bool     `json:"ok"`
	ToolCalls      []string `json:"tool_calls"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type transcriptResponse struct {
	ConversationID string    `json:"conversation_id"`
	Messages       []message `json:"messages"`
	MessageCount   int       `json:"message_count"`
}

// AgentCard is the A2A-style descriptor. The shape matches the FastAPI gateway's card so
// the two are interchangeable to a client.
type AgentCard struct {
	Name               string       `json:"name"`
	Description        string       `json:"description"`
	Version            string       `json:"version"`
	URL                string       `json:"url"`
	Capabilities       capabilities `json:"capabilities"`
	DefaultInputModes  []string     `json:"defaultInputModes"`
	DefaultOutputModes []string     `json:"defaultOutputModes"`
	Skills             []skill      `json:"skills"`
}

type capabilities struct {
	Streaming         bool `json:"streaming"`
	PushNotifications bool `json:"pushNotifications"`
}

type skill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

func bankingCard() AgentCard {
	return AgentCard{
		Name:        "Agentic-Flink Banking Agent",
		Description: "Router -> path -> verifier banking agent (cards/payments/general) over the goagentic essence.",
		Version:     "0.1.0",
		URL:         "/agent",
		Capabilities: capabilities{Streaming: false, PushNotifications: false},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Skills: []skill{{
			ID:          "banking",
			Name:        "Banking Q&A",
			Description: "Answers card, payment, and general banking questions with tool use + retrieval.",
			Tags:        []string{"banking", "router", "rag"},
		}},
	}
}

// Handler returns the gateway's http.Handler (Go 1.22+ method+path routing).
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", g.handleHealth)
	mux.HandleFunc("GET /.well-known/agent-card.json", g.handleCard)
	mux.HandleFunc("POST /agent", g.handleAgent)
	mux.HandleFunc("GET /conversations/{cid}", g.handleTranscript)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "backend": g.backend})
}

func (g *Gateway) handleCard(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, bankingCard())
}

func (g *Gateway) handleAgent(w http.ResponseWriter, r *http.Request) {
	var req turnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.ConversationID == "" || req.Text == "" {
		writeError(w, http.StatusUnprocessableEntity, "conversation_id and text are required")
		return
	}
	if req.UserID == "" {
		req.UserID = "anonymous"
	}
	res := g.rt.Submit(core.NewEvent(req.ConversationID, req.UserID, req.Text))
	calls := res.ToolCalls
	if calls == nil {
		calls = []string{}
	}
	writeJSON(w, http.StatusOK, turnResponse{
		ConversationID: res.ConversationID,
		Reply:          res.Reply,
		Path:           res.Path,
		OK:             res.OK,
		ToolCalls:      calls,
	})
}

func (g *Gateway) handleTranscript(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	storer, ok := g.rt.(Storer)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this backend does not expose transcripts")
		return
	}
	store := storer.Store()
	hist := store.History(cid)
	msgs := make([]message, 0, len(hist))
	for _, m := range hist {
		msgs = append(msgs, message{Role: m.Role, Content: m.Content})
	}
	writeJSON(w, http.StatusOK, transcriptResponse{
		ConversationID: cid,
		Messages:       msgs,
		MessageCount:   store.MessageCount(cid),
	})
}

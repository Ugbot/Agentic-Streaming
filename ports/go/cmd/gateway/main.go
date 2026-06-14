// Command gateway serves the banking agent over HTTP using the Go core LocalRuntime —
// the complete Go experience: a runnable A2A-style gateway with an Agent Card, a turn
// endpoint, transcripts, and a health check. Stdlib only.
//
//	go run ./cmd/gateway          # listens on :8080 (override with AGENTIC_GATEWAY_ADDR)
//	curl localhost:8080/.well-known/agent-card.json
//	curl -s localhost:8080/agent -d '{"conversation_id":"c1","text":"what is my balance?"}'
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/jagentic/goagentic/core"
	"github.com/jagentic/goagentic/gateway"
)

func main() {
	addr := os.Getenv("AGENTIC_GATEWAY_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	g := gateway.New(core.NewBankingRuntime(), "local")
	srv := &http.Server{Addr: addr, Handler: g.Handler()}
	log.Printf("agentic-flink go gateway listening on %s (backend=local)", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

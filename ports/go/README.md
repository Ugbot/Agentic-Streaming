# goagentic — Agentic-Flink essence in Go

A pure-Go port of the Agentic-Flink **essence**, plus a complete Go runtime experience:
an HTTP gateway, a NATS JetStream engine, and a Temporal engine — all reusing one
dependency-free core. This is the Go peer of the Python `pyagentic` and Java
`jagentic-core` cores. See [`../../docs/portability/00-essence-and-core-abstractions.md`](../../docs/portability/00-essence-and-core-abstractions.md).

```
ports/go/
  core/                 pure essence (no deps): Event, ChatMessage, ConversationStore,
                        KeyedStateStore, ToolRegistry, AgentContext, Brain, Agent,
                        RoutedGraph, Retrieval, Banking, LocalRuntime  (+ tests)
  gateway/              stdlib net/http A2A-style gateway over a Runtime  (+ tests)
  engines/natsjs/       NATS JetStream engine: KV-backed state + stream transport (+ test)
  engines/temporal/     Temporal engine: entity workflow per conversation  (+ test)
  cmd/demo/             run the banking graph on the LocalRuntime
  cmd/gateway/          run the HTTP gateway
  cmd/natsdemo/         run the streamed NATS JetStream round-trip
```

The `core` package is the single source of truth: `gateway`, `natsjs`, and `temporal`
all consume `core.BuildBankingGraph()` / `core.DefaultBankingTools()` /
`core.BankingRetriever()` and call `RoutedGraph.Handle` — none reimplements routing,
tools, or retrieval. A new tool/path added to `core` propagates to all of them. Each
engine's seam is injectable (`natsjs.New(graph, tools, retriever)`,
`temporal.MakeConversationWorkflow(graph, tools, retriever)`) so an *extended* graph
runs unchanged — the extensibility tests prove a new `freeze_card` tool + `fraud` path
flow through both the NATS KV seam and the Temporal workflow.

## Run

```bash
cd ports/go

go test ./...                 # core + gateway + temporal always; natsjs runs if a
                              # JetStream server is reachable (else its test skips)

go run ./cmd/demo             # banking router->path->verifier on the LocalRuntime

go run ./cmd/gateway          # HTTP gateway on :8080 (AGENTIC_GATEWAY_ADDR to override)
curl localhost:8080/.well-known/agent-card.json
curl -s localhost:8080/agent -d '{"conversation_id":"c1","text":"what is my balance?"}'
curl -s localhost:8080/conversations/c1

# NATS JetStream (needs a server):
podman run -d -p 4222:4222 nats:latest -js
go run ./cmd/natsdemo         # publish turns -> consumer runs the graph against KV -> replies
```

## The two engines, in Go

- **NATS JetStream** ([`engines/natsjs`](engines/natsjs/natsjs.go)) — the JetStream **KV
  store** is native durable keyed state (C1); a persistent stream + durable consumer is
  the ordered, redelivering transport (C3). Each turn runs the graph in a load → handle →
  save bracket around the per-conversation KV envelope, with revision compare-and-set as
  the single-writer (C2) backstop. The Go peer of [`../nats`](../nats/) (Python).
- **Temporal** ([`engines/temporal`](engines/temporal/temporal.go)) — one entity workflow
  per conversation (`workflowID == conversationID`): one running execution (C2),
  event-sourced durable state (C1+C3), turns delivered as signals and processed serially.
  Runs entirely in-memory via the SDK's `testsuite.TestWorkflowEnvironment` (no server).
  The Go peer of [`../temporal`](../temporal/) (Java).

## The gateway

[`gateway`](gateway/gateway.go) is a stdlib-only HTTP front door (no third-party deps)
over any `Runtime` (the `core.LocalRuntime`, or an engine adapter). It exposes an
A2A-style **Agent Card** at `/.well-known/agent-card.json` (the same card shape as the
sibling [FastAPI gateway](../gateway-fastapi/)), a `POST /agent` turn endpoint, a
`GET /conversations/{id}` transcript endpoint, and `GET /healthz`.

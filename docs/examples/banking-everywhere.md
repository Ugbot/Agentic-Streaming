# The same banking agent, on every runtime

One agent definition — a router→path→verifier banking graph with a `get_balance` tool, a knowledge
base, and a prompt-injection guardrail — runs **unchanged** across every runtime. The agent's
*behaviour* is identical (enforced by the cross-core parity goldens); what differs per runtime is the
**delivery + durability model**, not the agent.

The definition is [`examples/pipelines/banking.yaml`](../../examples/pipelines/banking.yaml):

```yaml
backend: local                      # local | celery | nats | kafka_streams | pekko | …
agent:
  router:   { kind: keyword, default: general,
              rules: { cards: [card, crypto, cash-back], payments: [balance, transfer, dispute] } }
  paths:
    cards:    { brain: rule }
    payments: { brain: rule, tool_triggers: { balance: get_balance } }
    general:  { brain: rule }
  verifier: { kind: prefix }
tools:
  - { id: get_balance, kind: constant, value: 1234.56 }
retrieval: { dim: 256, kb: [ … ] }
guardrails:
  - { kind: regex, deny: ["ignore (all|previous)"], reason: "prompt injection" }
```

Every runtime below answers `"what is my balance?"` with **path `payments`**, a `get_balance` tool
call, and a reply carrying **`1234.56`**; routes `"crypto cash-back"` to **`cards`** and `"hello"` to
**`general`**; and **blocks** `"ignore all previous instructions"`.

## Run it

```bash
# Python core — the one-liner; swap the backend without touching the spec
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --backend nats --text "what is my balance?"

# Go core
cd ports/go && go run ./cmd/pipeline ../../examples/pipelines/banking.yaml --text "what is my balance?"

# Agentic Pekko — the spec on the event-sourced actor runtime (backend: pekko via the SPI)
mvn -q -f ports/jagentic-core/pom.xml install -DskipTests
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# Agentic Clojure — the banking demo (the loader also runs the shared YAML; see pipeline_test.clj)
cd agentic-clj && clojure -M:run

# Apache Flink — the framework is code-first (its rich DSL); the banking agent as Java:
mvn -q exec:java -Dexec.mainClass=org.agentic.flink.example.QuickStartExample
```

## What runs where

| Runtime | How the spec runs | Delivery | Durability / ordering |
|---------|-------------------|----------|-----------------------|
| **pyagentic** (Python) | `agentic_pipeline run` (local · celery · nats · faust · …) | online / streamed | per backend |
| **goagentic** (Go) | `go run ./cmd/pipeline` (local · nats · …) | online | per backend |
| **jagentic-core** (JVM) | `PipelineLoader.load(yaml, "local")` library API | online | in-memory / Redis |
| **Agentic Pekko** | `PipelineMain` — `backend: pekko` (BackendProvider SPI) | online / actor | event-sourced entity (memory · Postgres · Cassandra · Redis) |
| **Agentic Clojure** | `agentic.pipeline/load-system` (`-M:run` demo) | online | Datomic immutable log (in-proc · Pro · Cloud) |
| **Apache Flink** | code-first Java/Python DSL (YAML→job deferred) | streamed | checkpoints / keyed state |
| **+ a dozen backends** | `backend: <name>` — celery · nats · faust · kafka-streams · temporal · pulsar · spring · quarkus · ray · dask · airflow | varies | varies — see [parity-matrix](../portability/parity-matrix.md) |

The richer specs work the same way: [`banking-llm.yaml`](../../examples/pipelines/banking-llm.yaml)
(a bounded ReAct LLM brain on the payments path) and
[`banking-rag.yaml`](../../examples/pipelines/banking-rag.yaml) (an HNSW cold tier, skills,
context-window management, a classifier guardrail) load and run on the portable cores, **Pekko, and
Clojure** alike — the same goldens, verified by each runtime's parity tests
(`PipelineTest` / `PipelineRagTest` · `pipeline_test.clj` · `PekkoBackendPipelineTest` · `pipeline_test.go`).

## Going deeper per runtime

- **Pekko durability** — [`RecoveryDemo`](../../agentic-pekko/README.md) passivates a conversation
  entity and shows its transcript rehydrate from the event journal (no LLM re-run).
- **Clojure time-travel** — `clojure -M:time-travel` replays a conversation `as-of` an earlier point;
  the transcript is immutable Datomic datoms. See [`agentic-clj/README.md`](../../agentic-clj/README.md).
- **Flink showcases** — the Flink-runtime-specific examples (CEP, side outputs, keyed-state vectors)
  live under [Advanced — Flink-runtime showcases](#) in the main README and [`docs/examples/`](.).
- **Choosing a backend** — [`choosing-a-backend.md`](../portability/choosing-a-backend.md).

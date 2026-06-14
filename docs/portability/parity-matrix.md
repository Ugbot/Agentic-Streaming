# Parity matrix — what each version of Agentic Streaming can do, per backend

Agentic Streaming is **one agent essence** with many runtimes. The agent features live in
three shared, Flink-free cores (`pyagentic`, `jagentic-core`, `goagentic`); each engine
adapter is a thin, **injectable** seam over a core. So the *agent's capabilities* are the
same everywhere — what differs per backend is the **delivery model** (online turn vs
streamed vs batch) and where **durability / ordering** come from. Nothing agentic is
locked to Flink.

## 1. The three cores are at parity

The cores are behaviorally identical (enforced by cross-core parity tests:
`test_parity.py` / `ParityTest` / `parity_test.go`).

| Capability | pyagentic (Python) | jagentic-core (Java) | goagentic (Go) |
|------------|:---:|:---:|:---:|
| Event / ChatMessage / TurnResult | ✅ | ✅ | ✅ |
| ConversationStore (multi-turn memory) | ✅ | ✅ | ✅ |
| KeyedStateStore | ✅ | ✅ | ✅ |
| ToolRegistry + tool execution | ✅ | ✅ | ✅ |
| RoutedGraph (router → path → verifier) | ✅ | ✅ | ✅ |
| Hot+cold retrieval (FNV‑1a embedder, identical) | ✅ | ✅ | ✅ |
| **LLM brain SPI** (ChatClient + ReAct loop) | ✅ | ✅ | ✅ |
| Providers: Ollama / OpenAI / Stub | ✅ | ✅ | ✅ |
| **Guardrails** (input/output screen) | ✅ | ✅ | ✅ |
| **Listeners** (lifecycle + metrics) | ✅ | ✅ | ✅ |
| Call other agents (A2A‑as‑tool, http) | ✅ | ✅ | ✅ |
| **GraphBuilder** (declarative spec → graph) | ✅ | ✅ | ✅ |
| **YAML pipeline loader** | ✅ | ✅ | ✅ |
| Hot‑swap durable store (Redis/Valkey) | ✅ | SPI¹ | SPI¹ |

¹ The `ConversationStore` SPI exists in all three; the Redis implementation ships in
`pyagentic` (the canonical example). Java/Go add Redis/Postgres impls the same way (a
class implementing the SPI + a registry entry) — the interface + config wiring is already
there.

## 2. Backends: capability + limitations

Because adapters are injectable and features live in the cores, **every backend that
executes the graph runs the full agent feature set** (LLM brain, tools, retrieval,
guardrails, listeners, multi‑turn memory, agent‑calls). The columns below capture the
*real* per‑backend differences.

Legend — Delivery: **online** (synchronous turn) · **streamed** (keyed stream) · **batch**
(offline). C2 = single‑writer‑per‑conversation, C3 = durable/fault‑tolerant.

| Backend | Lang | Delivery | C2 (ordering) | C3 (durability) | YAML loader wired | Verified here |
|---------|:----:|:--------:|---------------|-----------------|:-----------------:|---------------|
| **Flink** (first‑class) | JVM/Py | streamed | native keyBy | native checkpoints | n/a² | full framework |
| **Faust** | Python | streamed | Kafka partition | Kafka changelog | core build³ | import‑guarded |
| **Kafka Streams** | JVM | streamed | partition | txn EOS | engine Runtime⁴ | TopologyTestDriver |
| **Pekko** | JVM | online/actor | actor mailbox | Persistence | engine Runtime⁴ | ActorTestKit ✅ |
| **Temporal** | JVM/Go | online | one exec/id | event‑sourced | engine Runtime⁴ | testsuite ✅ |
| **Pulsar Functions** | JVM | streamed | Key_Shared | BookKeeper state | engine Runtime⁴ | in‑mem ctx ✅ |
| **Ray** | Python | online/actor | actor | external | core build³ | import‑guarded |
| **NATS JetStream** | Py/Go | online | subject+CAS | KV + stream | **✅ local+nats** | runs live ✅ |
| **Quarkus** | JVM | streamed | partition | external store | engine Runtime⁴ | compiles |
| **Spring** | JVM | online/msg | partition | external store | engine Runtime⁴ | compiles |
| **Celery** | Python | online | queue+lock | acks+store | **✅** | runs live ✅ |
| **Dask** | Python | batch | — | retry | core build³ | runs live ✅ |
| **Airflow** | Python | batch/sched | — | retry/idempotent | core build³ | simulate ✅ |
| **Local** (in‑proc) | all 3 | online | per‑key lock | in‑mem/Redis | **✅ all 3** | runs live ✅ |

² Flink stays code‑first (its rich DSL); a YAML→Flink‑job target is deferred.
³ "core build" = the GraphBuilder builds the graph; the engine hosts it via its own
   injectable seam (`configure(...)` / constructor), not yet a one‑line loader backend.
⁴ The JVM `GraphBuilder.Built` is constructed by the engine module's entrypoint (each
   exposes a `Runtime`); the core loader ships `local`, engine modules wire the rest.

### Backend limitations (the honest caveats)

- **Batch engines (Dask, Airflow)** don't do live per‑conversation streaming — they host
  *parts* (parallel RAG ingest / offline eval / scheduled + branching DAGs). No C2.
- **LLM calls on a stream thread** (Kafka Streams, Pulsar, Faust) should be split across a
  response topic, not blocked inline — the adapters note the seam.
- **NATS** single‑writer is a convention (subject routing + KV compare‑and‑set), not a
  partition guarantee.
- **Ray** keeps C1+C2 in memory; durability (C3) is write‑through to an external store.
- **Python wheels**: Faust/Ray can't run on this box's Python 3.14 (import‑guarded); their
  agent logic is the tested core.

## 3. What stays Flink‑first (by design)

These are genuinely runtime‑native and not meaningfully portable; reimplementing them on
Airflow/Celery would be pointless, so they remain first‑class‑Flink‑only:

- the **CEP pattern engine** (event‑time pattern matching);
- **in‑JVM HNSW vector memory over Flink state**;
- **Flink checkpoint/savepoint** exactly‑once semantics.

Every other agentic capability is portable and available on all backends via the cores.

## 4. How to read this for a decision

Pick by your delivery need first (online turn? keyed stream? batch?), then by the
durability/ordering you want native vs external, then by your stack. The
**[choosing‑a‑backend guide](choosing-a-backend.md)** walks that decision; the
**[pipelines guide](pipelines.md)** shows the one YAML that runs on whichever you pick.

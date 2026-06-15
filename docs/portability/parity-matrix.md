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
| Providers (unified lib + native): litellm/Ollama/OpenAI · langchaingo · LangChain4J | ✅ | ✅ | ✅ |
| **Embedder SPI** (hashing default; real via the LLM lib) | ✅ | ✅ | ✅ |
| **Structured output** (schema‑validated answers) | ✅ | ✅ | ✅ |
| **Skills** (tools + prompt fragment + facts) | ✅ | ✅ | ✅ |
| **Guardrails** — regex + **classifier** (lexicon/embedding) | ✅ | ✅ | ✅ |
| **Listeners** (≈9 lifecycle hooks + metrics + composite) | ✅ | ✅ | ✅ |
| **Vector store SPI** — in‑memory · **in‑process HNSW** · Qdrant | ✅ | ✅ | ✅ |
| Embeddable analytical store (DuckDB) | ✅ | — | — |
| **Long‑term store SPI** — in‑memory · Postgres | ✅ | ✅ | ✅ |
| Hot‑swap conversation store — in‑memory · **Redis/Valkey** | ✅ | ✅ | ✅ |
| **MCP client** (register MCP server tools) | ✅ official SDK | ✅ pure‑Java | ✅ mark3labs |
| **A2A client** (peer‑as‑tool, card+send+retries) | ✅ | ✅ | ✅ |
| **Saga / compensation** (reverse‑order rollback) | ✅ | ✅ | ✅ |
| **Context‑window mgmt** (MoSCoW compaction) | ✅ | ✅ | ✅ |
| **Web toolkit** (robots‑aware fetch + extract) | ✅ | ✅ | ✅ |
| **DL inference SPI** (Classifier/Scorer) | ✅ | ✅ | ✅ |
| **GraphBuilder** (declarative spec → graph) | ✅ | ✅ | ✅ |
| **YAML pipeline loader** (full Phase‑F schema) | ✅ | ✅ | ✅ |

All three cores ship the same SPIs **and** one working reference implementation per heavy
integration (Qdrant vectors, Postgres long‑term, Redis/Valkey conversations, the official
MCP client, HTTP A2A). Other backends are opt‑in: implement the SPI + add a `kind` to the
loader factory. Real LLM/embedding providers, external stores and DL models are import‑/
dependency‑guarded so the model‑free defaults (Stub chat, FNV hashing embedder, in‑memory
stores, lexicon classifier) keep the offline test suites green; live paths are verified
against podman infra (Ollama, Qdrant, Postgres, Valkey) and skip cleanly when absent.

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
| **Clojure** (first‑class) | Clojure | online | per‑conv serialize | **Datomic** (immutable) | **✅ EDN+YAML** | runs live ✅ |
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
- **Clojure** is the one backend that is *not* core‑backed: it's a **pure‑Clojure
  reimplementation** of the essence (no `jagentic-core` dep), kept at parity by the same FNV +
  banking goldens. Single‑writer (C2) is a per‑conversation convention; durability (C3) is
  **Datomic** — immutable datoms give the conversation log + time‑travel natively, on the in‑process
  `com.datomic/local` **or an external Datomic Pro / Cloud** (same client API, selected by config). The
  Clojure loader runs the full shared schema — `banking.yaml`, `banking-llm.yaml` and
  `banking-rag.yaml` (skills, context‑window, classifier guardrail) — with one nuance: its cold tier
  is **exact cosine KNN** (a correctness‑superset of HNSW ANN), not an approximate index. See
  [`clojure.md`](clojure.md) and [`../../agentic-clj/`](../../agentic-clj/).
- **Pekko** runs the same specs via `backend: pekko`; the `PipelineMain` CLI drives any `pipeline.yaml`
  through the event‑sourced actor runtime (`banking`, `banking-llm`, `banking-rag` all covered by
  `PekkoBackendPipelineTest`).

## 3. What stays Flink‑first (by design)

These are genuinely runtime‑native and not meaningfully portable; reimplementing them on
Airflow/Celery would be pointless, so they remain first‑class‑Flink‑only:

- the **CEP pattern engine** (event‑time pattern matching);
- **HNSW vector memory backed by *Flink state*** (checkpointed/keyed) — note the cores now
  ship their own in‑process HNSW index (`HnswVectorStore`), so approximate‑nearest‑neighbour
  search itself is portable; what stays Flink‑only is binding that index to Flink's managed,
  checkpointed state;
- **Flink checkpoint/savepoint** exactly‑once semantics.

Every other agentic capability is portable and available on all backends via the cores.

## 4. How to read this for a decision

Pick by your delivery need first (online turn? keyed stream? batch?), then by the
durability/ordering you want native vs external, then by your stack. The
**[choosing‑a‑backend guide](choosing-a-backend.md)** walks that decision; the
**[pipelines guide](pipelines.md)** shows the one YAML that runs on whichever you pick.

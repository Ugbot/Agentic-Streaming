# Portability. Agentic Streaming off Flink (Flink is one of three first-class runtimes)

What would this project be on a *different* engine? These notes name the
engine-agnostic essence, the capabilities Flink was providing, and an honest
per-engine mapping (what fits, what's awkward, what to drop). The essence is now
implemented: two Flink-free cores (`pyagentic` · `jagentic-core`) + a declarative YAML
pipeline. Three runtimes are **first-class**, the
Flink framework, [`agentic-pekko/`](../../agentic-pekko/) (actors), and
[`agentic-clj/`](../../agentic-clj/) (pure Clojure on Datomic).
The cores are now **near-complete standalone agent frameworks**, real LLM/embedding
libraries, structured output, skills, MCP + A2A clients, saga/compensation, context-window
management, an in-process **HNSW** vector index, vector/long-term/conversation store SPIs
(Qdrant/Postgres/Redis reference impls), a web toolkit, a DL-inference SPI, and the
**stream-stateful core** (CEP · timers · windows · replay · suspend/resume · tracing, see
[`stream-stateful-core.md`](stream-stateful-core.md)), so you get the good parts without Flink.

**Build & deploy (start here if you just want to use it):**
- [`pipelines.md`](pipelines.md), define an agent in `pipeline.yaml` (prompts, tools,
  skills, retrieval + vector store, classifier/regex guardrails, MCP + A2A, context-window
  management, hot-swappable stores) and run it from Python or the JVM.
- [`using-a-real-model.md`](using-a-real-model.md), swap the model-free defaults for real
  chat + embeddings (Ollama / OpenAI / Anthropic) per language.
- [`tool-services.md`](tool-services.md), the toolkit (web scraping, **Tika**, RAG, inference,
  utilities) decomposed into standalone, framework-agnostic tools any LLM/framework can run over
  **MCP · REST · gRPC · Kafka/Redis pub-sub** (Java/Quarkus, Flink-free).
- [`choosing-a-backend.md`](choosing-a-backend.md), the decision guide.
- [`stream-stateful-core.md`](stream-stateful-core.md), the **stream-stateful core**: CEP, timers,
  windows, replay, human-in-the-loop, and tracing, portable across all three cores (CEP is no longer
  Flink-only).
- [`../capabilities.md`](../capabilities.md), the **generated** agentic/v1 capability matrix:
  per-runtime `supported` / `partial` / `unsupported` / `not_tested`, derived by
  `spec/tools/conformance_matrix.py` from the shared conformance fixtures. The only place a
  capability status is authored.
- [`../runtimes/`](../runtimes/README.md), one page per conformance tested runtime:
  [common primitives](../runtimes/common-primitives.md), [Flink](../runtimes/flink.md),
  [Pekko](../runtimes/pekko.md), [Clojure](../runtimes/clojure.md),
  [pure Python](../runtimes/python.md), [Python facade](../runtimes/python-facade.md),
  and [PyFlink](../runtimes/pyflink.md). Each carries an excerpt derived from the generated matrix.

**Design (the why):**
- [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md),
  the essence, the capability inventory, the engine-agnostic core + Engine SPI, the
  capability matrix, and the ranked fit. Every engine doc is written against it.

**Per-engine deep-dives** (same 6-section template; ranked best-fit first). Pekko and Clojure
are runtimes in this repository; the others are design notes only. The experimental adapters
were moved out; the code as it stood is at [tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/ports/experimental).

| Doc | Engine | Lang | Verdict in one line |
|-----|--------|------|---------------------|
| [`faust.md`](faust.md) | Faust (faust-streaming) | Python | Native fit: faust *agents* + *Tables* ≈ our agent + ConversationStore. |
| [`pekko.md`](pekko.md) | Apache Pekko | Java | **First-class** ([`agentic-pekko/`](../../agentic-pekko/)), event-sourced, cluster-sharded entity per conversation (C1+C2+C3 native). |
| [`clojure.md`](clojure.md) | Clojure (Datomic) | Clojure | **First-class, pure-Clojure** reimplementation; immutable datoms = the conversation log + time-travel (C3 native). |
| [`ray.md`](ray.md) | Ray | Python | Actor-per-conversation = stateful agent; durability + streaming are external. |
| [`quarkus.md`](quarkus.md) | Quarkus | Java | Reactive messaging + Mutiny; keyed state external. Already our proxy. |
| [`spring.md`](spring.md) | Spring | Java | Cloud Stream + StateMachine + Spring AI; dataflow → message channels. |
| [`dask.md`](dask.md) | Dask | Python | Great for batch/parallel RAG + eval; weak for live keyed streaming. |
| [`airflow.md`](airflow.md) | Airflow | Python | Routed graph → DAG; batch/scheduled agentic + RAG ingestion; not streaming. |

For the Python engines (Faust, Ray, Dask, Airflow) the target is **pure Python**. For the JVM
engines (Pekko, Quarkus, Spring) the existing Java core types are reused behind the Engine SPI.

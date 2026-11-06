# Portability. Agentic Streaming off Flink

What would this project be on a *different* engine? These notes name the
engine-agnostic essence, the capabilities Flink was providing, and an honest
per-engine mapping (what fits, what's awkward, what to drop). The essence is now
implemented: two Flink-free cores (`pyagentic` · `jagentic-core`) + a declarative YAML
pipeline. The supported runtimes are the Flink framework (the primary one), plain Python and
[`agentic-pekko/`](../../agentic-pekko/) (actors). All of it is experimental; see the
[README](../../README.md).
The cores hold LLM/embedding clients, structured output, skills, MCP + A2A clients, saga/compensation, context-window
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
- [`choosing-a-backend.md`](choosing-a-backend.md), the decision guide.
- [`stream-stateful-core.md`](stream-stateful-core.md), the **stream-stateful core**: CEP, timers,
  windows, replay, human-in-the-loop, and tracing, portable across the cores (CEP is no longer
  Flink-only).
- [`../runtimes/`](../runtimes/README.md), one page per runtime:
  [common primitives](../runtimes/common-primitives.md), [Flink](../runtimes/flink.md),
  [Pekko](../runtimes/pekko.md) and [pure Python](../runtimes/python.md).

**Design (the why):**
- [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md),
  the essence, the capability inventory, the engine-agnostic core + Engine SPI, the
  capability matrix, and the ranked fit. Every engine doc is written against it.

**Per-engine notes.** [`pekko.md`](pekko.md) covers Apache Pekko
([`agentic-pekko/`](../../agentic-pekko/)): an event-sourced, cluster-sharded entity per conversation.
The notes for other engines moved out with their adapters; the code as it stood is at
[tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/ports/experimental).

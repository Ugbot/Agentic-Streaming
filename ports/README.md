# `ports/`: the shared cores

`ports/` holds three things: the conformance tested cores [`jagentic-core/`](jagentic-core/)
(JVM) and [`pyagentic/`](pyagentic/) (Python), and the portable `pipeline.yaml` CLI
[`agentic-pipeline/`](agentic-pipeline/). See [conformance results](../docs/runtimes/README.md#conformance-results).

The experimental engine adapters were moved out of this repository. The code as it stood is
at [tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/ports/experimental).

```
ports/
  pyagentic/         pure-Python essence + LocalRuntime, conformance tested
  jagentic-core/     pure-Java essence + LocalRuntime, conformance tested
  agentic-pipeline/  declarative pipeline.yaml loader + backend registry (Python); Java in
                     jagentic-core/.../pipeline
```

**Build once.** Define an agent in a `pipeline.yaml` (prompts, tools, calls to other agents,
retrieval, guardrails, hot-swappable stores) and run it on the local backend, on
[`agentic-pekko/`](../agentic-pekko/) or on Flink, see
[`docs/portability/pipelines.md`](../docs/portability/pipelines.md) and
[choosing a backend](../docs/portability/choosing-a-backend.md). External services
(Redis/Valkey, Kafka/Fluss, Postgres) sit behind interfaces and come up via
[`examples/compose/externals.yml`](../examples/compose/externals.yml).

The cores implement the engine-agnostic abstractions once, `ConversationStore`,
`KeyedStateStore`, `ToolRegistry`, `AgentContext`, `RoutedGraph`, hot+cold
`Retrieval`, `Banking` example, and hold an LLM `ChatClient` SPI (litellm / LangChain4J + Ollama/OpenAI), an `Embedder`
SPI, structured output, skills, regex **and** classifier guardrails, ≈9 listener hooks, a
`VectorStore` SPI with an in-process **HNSW** index (+ Qdrant / DuckDB), a `LongTermStore` SPI
(Postgres), Redis/Valkey conversations, an **MCP** client, an **A2A** peer-as-tool client,
saga/compensation, context-window management, a web toolkit, and a DL-inference
(`Classifier`/`Scorer`) SPI. Everything has a model-free default so the offline suites stay
green; real providers/backends are opt-in.

---

## Run it

```bash
# pure-Python core (no deps)
cd ports/pyagentic && PYTHONPATH=. python -m pytest tests/ -q

# declarative loader + backend registry
cd ports/agentic-pipeline && PYTHONPATH=.:../pyagentic python -m pytest -q

# pure-Java core
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -f ports/jagentic-core/pom.xml test
./mvnw -f agentic-pekko/pom.xml test                          # the Pekko runtime (not under ports/)
```

---

## Extending the essence (add it once)

The **two cores are the single source of truth**. Add a tool (`ToolRegistry.register(...)`), a
path (an `Agent` in the graph's paths), a router rule, or a retrieval source to `jagentic-core`
(Java) or `pyagentic` (Python), and everything on that core picks it up with no other change.

This is enforced by tests, not just convention: `pyagentic/tests/test_extensibility.py` and
`jagentic-core` `ExtensibilityTest` add a brand-new `freeze_card` tool + `fraud` path **through
the public API only** (no framework edits) and prove the core routes to and invokes them.

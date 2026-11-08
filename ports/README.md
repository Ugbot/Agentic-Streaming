# `ports/`: the shared cores

`ports/` holds three things: the conformance tested cores [`jagentic-core/`](jagentic-core/)
(JVM) and [`pyagentic/`](pyagentic/) (Python), and the portable `pipeline.yaml` CLI
[`agentic-pipeline/`](agentic-pipeline/). See [conformance results](../docs/runtimes/README.md#conformance-results).

The experimental engine adapters were moved out of this repository. The code as it stood is
at [tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/ports/experimental).

```
ports/
  pyagentic/         the `agentic` package: the Python core and LocalRuntime, conformance tested
  jagentic-core/     pure-Java essence + LocalRuntime, conformance tested
  agentic-pipeline/  pipeline.yaml loader and CLI on the `agentic` core (Python); Java in
                     jagentic-core/.../pipeline
```

**Build once.** Define an agent in a `pipeline.yaml` (prompts, tools, calls to other agents,
retrieval, guardrails, hot-swappable stores) and run it on the local backend, on
[`agentic-pekko/`](../agentic-pekko/) or on Flink, see
[`docs/portability/pipelines.md`](../docs/portability/pipelines.md) and
[choosing a backend](../docs/portability/choosing-a-backend.md). External services
(Redis/Valkey, Kafka, Postgres) sit behind interfaces and come up via
[`examples/compose/externals.yml`](../examples/compose/externals.yml).

The Java core implements the engine-agnostic abstractions once (`ConversationStore`,
`KeyedStateStore`, `ToolRegistry`, `RoutedGraph`, hot and cold `Retrieval`, an LLM `ChatClient` SPI,
an `Embedder` SPI, skills, regex and classifier guardrails, a `VectorStore` SPI with an in-process
HNSW index, MCP and A2A clients, saga and context-window management). The Python core, the `agentic`
package, runs the same `agentic/v1` documents on its event log: model clients for OpenAI, Ollama,
Claude and Gemini, MCP stdio tools, A2A peers over HTTP, an HNSW knowledge base, a lexicon classifier
guardrail, skills, a token-budget transcript and CEP; it refuses what it does not have rather than
run without it. Everything has a model-free default so the offline suites stay green; real
providers and backends are opt-in.

---

## Run it

```bash
# the Python core and the pipeline CLI
python -m pip install -e "ports/pyagentic[test]" -e "ports/agentic-pipeline[test]" "mcp<2"
python -m pytest ports/pyagentic/tests -q
python -m pytest ports/agentic-pipeline -q

# pure-Java core
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -f ports/jagentic-core/pom.xml test
./mvnw -f agentic-pekko/pom.xml test                          # the Pekko runtime (not under ports/)
```

---

## Extending the essence (add it once)

The **two cores are the single source of truth**. Add a tool, a path, a router rule, or a retrieval
source to `jagentic-core` (Java, `ToolRegistry.register(...)`) or to a workflow document the `agentic`
core runs (Python, `Agent.use_tool(...)`), and everything on that core picks it up with no other
change. `jagentic-core`'s `ExtensibilityTest` adds a new `freeze_card` tool and `fraud` path through
the public API only and proves the core routes to and invokes them.

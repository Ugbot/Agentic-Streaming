# The same banking agent on the portable runtimes

One agent definition, a router to path to verifier banking graph with a `get_balance` tool, a
knowledge base, and a prompt-injection guardrail, runs unchanged on the conformance-tested
runtimes that have a pipeline loader: the pure Python core, jvm-core, Agentic Pekko and Apache
Flink. The agent's behaviour is the same on each; what differs per runtime is the delivery and
durability model, not the agent. This page only covers the example pipelines under
`examples/pipelines/`; for the fixtures see the
[conformance results](../runtimes/README.md#conformance-results).

The definition is [`examples/pipelines/banking.yaml`](../../examples/pipelines/banking.yaml):

```yaml
backend: local                      # Python loader: local. JVM loader: local, or pekko via the BackendProvider SPI
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
retrieval: { dim: 256, kb: [ ... ] }
guardrails:
  - { kind: regex, deny: ["ignore (all|previous)"], reason: "prompt injection" }
```

Every runtime listed under "Verified on this pipeline" answers `"what is my balance?"` with path
`payments`, a `get_balance` tool call, and a reply carrying `1234.56`; routes `"crypto cash-back"`
to `cards` and `"hello"` to `general`; and blocks `"ignore all previous instructions"` with the
reason `prompt injection`.

## Run it

```bash
# Python core. Both packages have package metadata and install in editable mode. local is the
# only backend; any other value raises ValueError.
python -m pip install -e ports/pyagentic -e ports/agentic-pipeline
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"

# jvm-core, the shared JVM core, through its PipelineCli.
./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -q -f ports/jagentic-core/pom.xml exec:java -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# Agentic Pekko, the spec on the event-sourced actor runtime (backend forced to pekko via the SPI).
# compile must be in the same invocation as exec:java or the class is not on the classpath.
./mvnw -q -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# Apache Flink. The same YAML runs through FlinkPipelineRunner inside the MiniCluster test.
./mvnw -q test -Dtest=FlinkPipelineRunnerTest -Dsurefire.failIfNoSpecifiedTests=false
```

## Verified on this pipeline

`bash tools/smoke-examples.sh` runs the first three rows. "Turn" means the runner answered one turn
for the pipeline with the expected path and reply.

| Runtime | Command | banking | banking-llm | banking-rag | incident |
|---------|---------|---------|-------------|-------------|----------|
| pure Python (`ports/pyagentic` + `ports/agentic-pipeline`) | `python -m agentic_pipeline run` | turn | turn | turn | turn |
| jvm-core | `PipelineCli` | turn | turn | turn | turn |
| Agentic Pekko | `PipelineMain` | turn | not run | not run | not run |
| Apache Flink | `FlinkPipelineRunnerTest` (MiniCluster) | test | not covered | not covered | not covered |

`banking-mcp.yaml`, `tools-mcp.yaml` and `multiagent.yaml` are covered on the Python and jvm-core
runners only. They need the MCP SDK on the `python3` on PATH and, for the A2A escalations, a peer at
`AGENTIC_PEER_AGENT_URL`. Their YAML headers list the exact prerequisites.

## What runs where

| Runtime | How the spec runs | Delivery | Durability / ordering |
|---------|-------------------|----------|-----------------------|
| pyagentic (Python) | `agentic_pipeline run` (`--backend local`; nothing else is registered) | online | in-memory / Redis |
| jvm-core (JVM) | `PipelineCli`, or `PipelineLoader.load(yaml, "local")` as a library | online | in-memory / Redis |
| Agentic Pekko | `PipelineMain`, `backend: pekko` (BackendProvider SPI) | online / actor | event-sourced entity (memory, Postgres, Cassandra, Redis) |
| Apache Flink | `FlinkPipelineRunner` (YAML to a MiniCluster job, driven from `FlinkPipelineRunnerTest`) and the legacy code-first Java DSL | streamed | checkpoints / keyed state |

The richer specs work the same way: [`banking-llm.yaml`](../../examples/pipelines/banking-llm.yaml)
(a bounded ReAct LLM brain on the payments path, scripted so no key is needed) and
[`banking-rag.yaml`](../../examples/pipelines/banking-rag.yaml) (an HNSW cold tier, skills,
context-window management, a classifier guardrail) load and run on the Python core and jvm-core,
and are covered by the parity tests (`PipelineTest`, `PipelineRagTest`, `PekkoBackendPipelineTest`).

## Going deeper per runtime

- Pekko durability: [`RecoveryDemo`](../../agentic-pekko/README.md) passivates a conversation
  entity and shows its transcript rehydrate from the event journal (no LLM re-run).
- Flink showcases: the Flink-runtime-specific examples (CEP, side outputs, keyed-state vectors)
  live under [`docs/examples/`](.).
- Choosing a backend: [`choosing-a-backend.md`](../portability/choosing-a-backend.md).

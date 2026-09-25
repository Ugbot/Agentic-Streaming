# The same banking agent on the portable runtimes

One agent definition, a router to path to verifier banking graph with a `get_balance` tool, a
knowledge base, and a prompt-injection guardrail, runs unchanged on the conformance-tested
runtimes that have a pipeline loader: the pure Python core, jvm-core, Agentic Pekko, Agentic
Clojure, and Apache Flink. The agent's behaviour is the same on each; what differs per runtime is
the delivery and durability model, not the agent. Which runtime passes which `agentic/v1` fixture
is recorded in the generated matrix, [`docs/capabilities.md`](../capabilities.md); this page only
covers the example pipelines under `examples/pipelines/`.

The definition is [`examples/pipelines/banking.yaml`](../../examples/pipelines/banking.yaml):

```yaml
backend: local                      # Python loader: local | celery | nats. JVM loader: local, or pekko via the BackendProvider SPI
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

`examples-bin/run-pipeline.sh` wraps the three command-line runners below, checks the
prerequisites first (JDK 21 and the Maven wrapper, or a Python interpreter with the two packages),
and resolves the pipeline path against the repository root:

```bash
bash examples-bin/run-pipeline.sh examples/pipelines/banking.yaml --runtime python --text "what is my balance?"
bash examples-bin/run-pipeline.sh examples/pipelines/banking.yaml --runtime jvm    --text "what is my balance?"
bash examples-bin/run-pipeline.sh examples/pipelines/banking.yaml --runtime pekko  --text "what is my balance?"
```

Set `PYTHON=/path/to/venv/bin/python` when the packages live in a virtual environment. The
underlying commands, for running them by hand:

```bash
# Python core. Both packages have package metadata and install in editable mode. The registered
# backend names are local, celery, and nats; any other value raises ValueError. celery and nats
# are experimental adapters under ports/experimental/ and need their engine package plus a
# running broker (nats: a JetStream server on 127.0.0.1:4222, otherwise ConnectionRefusedError).
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

# Agentic Clojure. `clojure -M:run` drives the code-defined banking system in agentic.core; the
# shared YAML is loaded by agentic.pipeline/load-system and exercised by pipeline_test.clj.
# The Clojure CLI downloads its dependencies from Clojars and Maven Central on first use.
cd agentic-clj && clojure -M:run
cd agentic-clj && clojure -X:test :nses '[agentic.pipeline-test]'

# Apache Flink. The same YAML runs through FlinkPipelineRunner inside the MiniCluster test.
./mvnw -q test -Dtest=FlinkPipelineRunnerTest -Dsurefire.failIfNoSpecifiedTests=false
```

The Go port under `ports/experimental/go` also hosts the banking graph (`go run ./cmd/pipeline`),
but it is experimental and not conformance tested, so it is not part of the claim on this page.

## Verified on this pipeline

Each row was run from a clean checkout with JDK 21, the Maven wrapper, Python 3.12 and the Clojure
CLI. "Turn" means the runner answered one `message/send` for the pipeline with the expected path,
tool and reply.

| Runtime | Command | banking | banking-llm | banking-rag | incident |
|---------|---------|---------|-------------|-------------|----------|
| pure Python (`ports/pyagentic` + `ports/agentic-pipeline`) | `run-pipeline.sh --runtime python` | turn | turn | turn | turn |
| jvm-core | `run-pipeline.sh --runtime jvm` (PipelineCli) | turn | turn | turn | turn |
| Agentic Pekko | `run-pipeline.sh --runtime pekko` (PipelineMain) | turn | turn | turn | turn |
| Agentic Clojure | `clojure -X:test :nses '[agentic.pipeline-test]'` | test | test | test | not loaded by the Clojure tests |
| Apache Flink | `FlinkPipelineRunnerTest` (MiniCluster) | test | not covered | not covered | not covered |

`banking-mcp.yaml`, `tools-mcp.yaml` and `multiagent.yaml` are covered on the Python and jvm-core
runners only; they need the Tool Services stdio server and, for the A2A escalations, a peer at
`AGENTIC_PEER_AGENT_URL`. Their YAML headers list the exact prerequisites. Nothing on this page
claims a result for PyFlink, the JPype facade, or the experimental adapters; the generated matrix
is the source of truth for those bindings.

## What runs where

| Runtime | How the spec runs | Delivery | Durability / ordering |
|---------|-------------------|----------|-----------------------|
| pyagentic (Python) | `agentic_pipeline run` (`--backend local`, `celery`, or `nats`; nothing else is registered) | online | per backend |
| jvm-core (JVM) | `PipelineCli`, or `PipelineLoader.load(yaml, "local")` as a library | online | in-memory / Redis |
| Agentic Pekko | `PipelineMain`, `backend: pekko` (BackendProvider SPI) | online / actor | event-sourced entity (memory, Postgres, Cassandra, Redis) |
| Agentic Clojure | `agentic.pipeline/load-system` (`-M:run` demo, `pipeline_test.clj`) | online | Datomic immutable log (in-process, Pro, Cloud) |
| Apache Flink | `FlinkPipelineRunner` (YAML to a MiniCluster job, driven from `FlinkPipelineRunnerTest`) and the code-first Java/Python DSL | streamed | checkpoints / keyed state |
| Experimental adapters (not conformance tested) | each `ports/experimental/<engine>` hosts the banking graph through its own entry point, not through `backend:` in the Python loader: go, faust, kafka-streams, temporal, pulsar, spring, quarkus, ray, dask, airflow. See [`ports/experimental/README.md`](../../ports/experimental/README.md) | varies | varies, see [parity-matrix](../portability/parity-matrix.md) |

The richer specs work the same way: [`banking-llm.yaml`](../../examples/pipelines/banking-llm.yaml)
(a bounded ReAct LLM brain on the payments path, scripted so no key is needed) and
[`banking-rag.yaml`](../../examples/pipelines/banking-rag.yaml) (an HNSW cold tier, skills,
context-window management, a classifier guardrail) load and run on the Python core, jvm-core,
Pekko and Clojure, verified by each runtime's parity tests
(`PipelineTest` / `PipelineRagTest`, `pipeline_test.clj`, `PekkoBackendPipelineTest`).

## Going deeper per runtime

- Pekko durability: [`RecoveryDemo`](../../agentic-pekko/README.md) passivates a conversation
  entity and shows its transcript rehydrate from the event journal (no LLM re-run).
- Clojure time-travel: `clojure -M:time-travel` replays a conversation `as-of` an earlier point;
  the transcript is immutable Datomic datoms. See [`agentic-clj/README.md`](../../agentic-clj/README.md).
- Flink showcases: the Flink-runtime-specific examples (CEP, side outputs, keyed-state vectors)
  live under [`docs/examples/`](.) and are launched by the `examples-bin/run-*.sh` scripts.
- Choosing a backend: [`choosing-a-backend.md`](../portability/choosing-a-backend.md).

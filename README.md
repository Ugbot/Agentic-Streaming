# Agentic Streaming

> **Status: experimental and pre-release.** There are no published packages. This project is **not
> safe to use for real payments or other non-idempotent actions yet**: known failures include tool
> calls re-running after a Flink restore. See [Known failures](#known-failures).

Agentic Streaming is a portable specification for durable, auditable agents over streaming data,
with a conformance suite and runtimes on Apache Flink, plain Python and Apache Pekko. An agent's
state is a fold over an ordered log of events, with one writer per conversation.

The project was called Agentic Flink. Licensed under [Apache 2.0](LICENSE).

## Supported runtimes

Flink is the primary runtime. All three are built on the Java core or the Python core and measured
against the [reference spec](spec/v1/primitives.md).

| Runtime | Built on | What it adds | Page |
|---|---|---|---|
| **Apache Flink (Java)**, primary | `ports/jagentic-core`, in the root module | Keyed event log in Flink state, savepoint restart, CEP. **The Flink adapter blocks on model and tool calls today**: a whole turn runs synchronously on the task thread | [Flink](docs/runtimes/flink.md) |
| Plain Python | `ports/pyagentic` | No JVM and no infrastructure | [pure Python](docs/runtimes/python.md) |
| Apache Pekko | `ports/jagentic-core`, in `agentic-pekko` | One event-sourced, cluster-sharded actor per conversation | [Pekko](docs/runtimes/pekko.md) |

The contract is the `agentic/v1` spec under [`spec/v1`](spec/v1/primitives.md), its reference runtime
and the 24 fixtures under [`spec/conformance/v1`](spec/conformance/v1/README.md). The fixtures are
mostly rule brains and scripted stubs. They do not cover a real model, a crash inside a turn, or a
failed compensation, so a pass is a narrow claim; see
[conformance results](docs/runtimes/README.md#conformance-results).

Everything else was moved out of this repository. The code as it stood is at
[tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone); the
[list is here](docs/runtimes/experimental.md).

## Known failures

- **Tool calls re-run after a Flink restore.** Flink state rolls back to the last checkpoint and the
  outside world does not. A turn that ran after that checkpoint runs again on redelivery, tools and
  model calls included. Tools receive no idempotency key, so the second call looks like a new one.
  The same happens to a turn that crashes before finishing on the other runtimes.
- **A failing compensation escapes the turn.** No terminal event is written, and on Flink the job
  goes into a restart loop that repeats the forward effects.
- **`state_ttl_ms` can crash a Flink job permanently** for a conversation that stays active past the
  TTL.
- **An uncaught exception in a turn fails the whole Flink job.** There is no dead-letter path.
- **No real model on the canonical paths.** On Flink and in the `agentic` Python package only a
  scripted stub runs. The `pipeline.yaml` CLI (`ports/agentic-pipeline`) still sits on the older
  `pyagentic` package, which has real model clients but no event log.
- **The tool allowlist is not a security control, and the canonical path has no authentication.** See
  [security](docs/security.md).
- **The full Java test run is flaky.** `AgentJobGeneratorGraphTest` asserts an operator order that
  depends on process state, so it can fail in a full run and passes alone. It has failed on macOS and
  passed on Linux CI at the same commit. `AgentExecutorTest` has also failed once on timing.
- **There is no supported way to run a `pipeline.yaml` from Kafka on a Flink cluster.**

## Quick start

Needs Java 21 and Python 3.9 or later. The JVM commands use the committed wrapper (`./mvnw`).
Every command in the block below was run from a fresh clone, on Java 21 and Python 3.13 on macOS
arm64, and passed. Each answers on path `payments` with a balance of `1234.56`.

```bash
git clone https://github.com/Ugbot/Agentic-Streaming.git && cd Agentic-Streaming

# Plain Python, no model, no infrastructure
python3 -m venv .venv
.venv/bin/pip install -e ports/pyagentic -e ports/agentic-pipeline
.venv/bin/python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"

# The spec and the reference runtime
.venv/bin/python spec/tools/validate_spec.py
.venv/bin/python spec/tools/run_conformance.py

# The Java core; every JVM command below needs this first
./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -q -f ports/jagentic-core/pom.xml exec:java -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# Apache Pekko
./mvnw -q -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# Apache Flink: one turn through a real job on a local MiniCluster
./mvnw -q -P examples compile exec:exec -Dexec.mainClass=org.agentic.flink.pipeline.FlinkPipelineRunner \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# The Flink conformance binding, and the runner's tests
./mvnw test -Dtest='FlinkConformanceTest,FlinkPipelineRunnerTest'
```

**Not listed as working:** `./mvnw clean test` and `./mvnw -f reactor/pom.xml verify` cannot be relied
on to pass, because of the flaky tests under [Known failures](#known-failures). When the Flink module
fails, the reactor skips Pekko. `./mvnw -f reactor/pom.xml -DskipTests verify` passes, which shows the
build and nothing about the tests.

The walkthrough is in [the banking agent on every runtime](docs/examples/banking-everywhere.md) and
[getting started](docs/getting-started.md).

## The model

A conversation is an ordered log of events: turns, tool results, routing decisions. The agent's
state is the value you get by folding that log.

- **Event sourcing:** state is derived from the log, which gives replay and audit of what the log
  holds. Model output is not in the log.
- **CQRS:** a command ("process this turn") is an ordered mutation with one writer per conversation;
  a query reads the view.

One turn is the same pipeline on every runtime. The logic lives in the core; each runtime supplies
ordering and durability.

```
        Event in
            |
            v
        input guardrails        regex or classifier: block or allow
            |
            v
        router                  keyword: pick a path
            |
            v
        path brain              rule, or a bounded LLM loop
            |                   tools, MCP, A2A peers, retrieval, context window
            v
        verifier                validate the reply
            |
            v
        Reply out, plus events appended to the conversation log
```

| Runtime | One writer per conversation | Durability of the log |
|---|---|---|
| Flink | `keyBy` and a keyed operator | Checkpoints and keyed state, so it rolls back to the last checkpoint |
| Pekko | Actor mailbox and cluster sharding | Pekko Persistence journal |
| Plain Python | A per-conversation gate | In memory, or a JSONL file |

What the spec aims to make true: state is exactly-once, effects are at-least-once under a stable key,
and anything that cannot take a key is an explicit human decision. Only the first part holds today,
and only on Flink within its checkpoints.

## What is in the repository

| Path | What it is |
|---|---|
| [`spec/`](spec/README.md) | `agentic/v1`: the primitives, schemas, fixtures, reference runtime and conformance runner |
| [`ports/jagentic-core`](ports/jagentic-core/README.md) | The Java core |
| [`ports/pyagentic`](ports/pyagentic/README.md) | The Python core: the conformance-tested `agentic` package and the older `pyagentic` package |
| [`ports/agentic-pipeline`](ports/agentic-pipeline/README.md) | The `pipeline.yaml` CLI; `local` is its only backend |
| Root module (`src/`, `pom.xml`) | The Flink adapter (`org.agentic.flink.runtime`, `pipeline`) and, for now, the deprecated legacy Flink DSL and its examples |
| [`agentic-pekko`](agentic-pekko/README.md) | The Pekko runtime |
| [`examples/pipelines`](examples/pipelines) | Example workflow documents |
| `reactor/` | The Maven parent and aggregator for the JVM modules |

The legacy DSL (`Agent.builder()` under `org.agentic.flink.dsl`) is outside the spec and no fixture
measures it. It is still in the root module and will be removed in a later change.

## Documentation

| Page | What it covers |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | From a clone to a running agent |
| [docs/runtimes/](docs/runtimes/README.md) | What each runtime is made of, and where conformance results are |
| [docs/portability/pipelines.md](docs/portability/pipelines.md) | The `pipeline.yaml` schema |
| [docs/python.md](docs/python.md) | The Python API |
| [docs/concepts.md](docs/concepts.md) | Core concepts |
| [docs/configuration.md](docs/configuration.md) | Configuration reference |
| [docs/security.md](docs/security.md) | What the code does and does not do about security |
| [docs/guides/flink-agents-integration.md](docs/guides/flink-agents-integration.md) | How this compares with Apache Flink Agents, which is the better choice today if you are on Flink |
| [docs/testing.md](docs/testing.md) | How to run each test suite |
| [docs/versioning.md](docs/versioning.md) | Versioning and the compatibility matrix |

## Install

Nothing is published to Maven Central or PyPI. Everything is built from a checkout. The PyPI name
`pyagentic` belongs to someone else, so the Python core cannot be published under it.

```bash
./mvnw -f reactor/pom.xml -DskipTests install    # org.jagentic:jagentic-core, agentic-flink, agentic-pekko
```

For `flink run`, the shaded jar is `target/agentic-flink-1.0.0-SNAPSHOT-uber.jar`; it excludes the
Flink runtime a cluster provides. `bash tools/check-jar-launch.sh` boots it under JDK 21. The thin
`agentic-flink` jar marks its provider SDKs and drivers optional, so add the ones you use to your own
pom; see the `<optional>` entries in the root `pom.xml`.

The uber jar leaves out the Fluss client, which carries old relocated copies of Netty, Jackson, Guava
and lz4 ([docs/security.md](docs/security.md)). For a Fluss backend, build with `./mvnw -P fluss
package` or put `com.alibaba.fluss:fluss-client` on the job classpath. Without it, selecting Fluss
fails at startup and says so.

The Milvus SDK is left out for the same reason: it carries its own relocated gRPC and Netty 4.1. For
the Milvus backend, build with `./mvnw -P milvus package` or put `io.milvus:milvus-sdk-java` on the
job classpath. Without it, selecting Milvus fails at startup and says so too.

## Contributing

Contributions are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) describes the toolchain, the local
checks and what a pull request needs. Changes so far are in [CHANGELOG.md](CHANGELOG.md), and
vulnerability reporting is in [SECURITY.md](SECURITY.md). Participation is subject to the
[code of conduct](CODE_OF_CONDUCT.md).

## License

[Apache License 2.0](LICENSE).

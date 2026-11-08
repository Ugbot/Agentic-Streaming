# Agentic Streaming

> **Status: experimental and pre-release.** There are no published packages, and nothing here has had
> an outside security review, so do not run it on real payments yet. What is guaranteed now, and what
> is not, is under [Guarantees](#guarantees) and [Control](#control). **A crash during a call to a `non_idempotent` tool leaves the
> outcome unknown.** The runtime will not repeat that call, and it cannot tell you whether it landed:
> a person has to.

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
and the 76 fixtures under [`spec/conformance/v1`](spec/conformance/v1/README.md). The fixtures are
mostly rule brains and scripted stubs. They crash turns at every event and around every call, but
they do not cover a real model, so a pass is a narrow claim; see
[conformance results](docs/runtimes/README.md#conformance-results).

Everything else was moved out of this repository. The code as it stood is at
[tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone); the
[list is here](docs/runtimes/experimental.md).

## Guarantees

State is exactly-once. Effects are at-least-once under a stable key. Anything that cannot take a key
is an explicit human decision. Each line below has a crash test
([section 10 of the spec](spec/v1/primitives.md) is the contract).

| Thing | Guaranteed | Not guaranteed |
|---|---|---|
| Conversation log and state | Exactly-once. A recovered turn leaves the log an uninterrupted run would | Flink's log is keyed state, so it rolls back to the last checkpoint; the journal covers that window |
| Model calls | A recorded answer is reused by index, so a re-run takes the same path | A crash during a call asks the model again, and you pay twice |
| `effect: read` tools | A recorded result is reused | With no recorded result the read runs again and may answer differently |
| `effect: idempotent` tools | At-least-once under a stable `Idempotency-Key` | Exactly one effect, unless the tool honours the key |
| `effect: non_idempotent` tools, the default | Never called twice by the runtime. A recorded result is reused | A crash during the call leaves the outcome **unknown**. The turn suspends and a person decides. Nothing can do better, because the runtime cannot see the far side of the call. A tool that declares nothing is treated like this, so one that is safe to repeat must say `read` or `idempotent` |
| Sagas | Undoes get the forward args and result, run on any failure class, and carry on from the log after a crash | A failed undo is terminal: it is recorded and needs repair by hand |
| Resume | One state machine: the original principal keeps the turn, duplicates and unknown signals rewrite nothing, a crash after `turn_resumed` loses no approval, and only an approver's signal is honoured | Who the approver is comes from whoever delivered the event: the gateway's token, or anything that can write the input topic |

These hold only with a journal that outlives the crash. `stores.effects` selects it: `postgres` or
`kafka` on the JVM, `file` in Python. A Flink job refuses a `non_idempotent` tool, which includes any
tool, peer or MCP server that declares no effect, without one. Without a declared journal the JVM keeps one in the process, which covers a task restart at most, so a
re-run after losing the process calls tools again, under the same keys. The journal must retain
entries for longer than the longest rollback or redelivery you allow. Effects were tested with a
real TaskManager killed on a Flink 2.2.1 session cluster and the Kafka journal
(`tools/kafka-journal-tm-loss.sh`), with a rescale restore on Postgres and Kafka
(`EffectJournalStoresIT`), and with a journal that fails or stalls at each write
(`EffectJournalAttackTest`, `test_agentic_journal_attack.py`). Postgres has not been run on a real
session cluster, and Pekko's journal outage is tested only on one node.

## Control

What the conformance fixtures prove on all five runtimes (reference, Java core, Flink, Pekko, Python),
with the rules in [section 11 of the spec](spec/v1/primitives.md) and the detail in
[security](docs/security.md):

| Control | What holds |
|---|---|
| Tool boundary | One deny-by-default check on every tool call, from a brain, a saga, a timer or a CEP pattern. A path gets what its `tools` lists; `tools: []` is none. A refusal is on the log and ends the turn |
| Principal | An authenticated principal is the turn's user, recorded on its events; a `user_id` beside it means nothing |
| Approvals | Only an approver named by `policies.approvers` can resume a turn or settle an unknown outcome; `policies.tool_approval` puts a person in front of a tool |
| Prompt injection | A model that obeys a tool result still cannot reach a tool its path does not list. Tool results are marked untrusted in the prompt, which helps and is not the boundary |
| Egress | HTTP tools, peers and model calls refuse private and metadata addresses unless the operator allows them |
| Limits | Per-principal rates, budgets, tool quotas and a kill switch, on the log when they stop something. Counters are per process |
| Redaction | Credentials, and what `policies.redact` names, are gone before the log or the journal is written |

The gateway (`agentic-gateway`) is the one HTTP front door: OIDC tokens, tenancy, ownership on every
read, in front of Flink, Pekko or the Java core. Crypto-shredding covers the Java core and Flink.
Outside the fixtures, unit tests, the gateway tests and two live labs (`tools/keycloak-lab.sh`,
`tools/opa-lab.sh`) prove the rest.

## Known failures

- **A turn run again after a transient model failure calls its `read` and `idempotent` tools again**,
  under the next free indices, as section 4 of the spec says. It is the one re-run that does not reuse
  the journal. A run that reached a `non_idempotent` tool is replayed instead, so that tool is not
  repeated.
- **A real model has only been exercised against a local stand-in server.** A document can declare
  `provider: ollama`, `openai`, `anthropic` or `gemini` on Flink, Pekko and the `agentic` Python
  package. No live provider has been called; the stubs check each provider's
  documented wire format, so a change on the provider's side would not show.
- **A rollback followed by a timer coming due stops the turn.** Calls are matched by index, so a
  timer that was not due on the first run and is on the re-run shifts them. Nothing is repeated, but
  the turn ends `fatal` and needs a person, even though the payment landed.
- **Two live writers on one Kafka journal key are not excluded.** A topic has no compare-and-set, and
  a process that has been replaced but is still running can race its replacement. Postgres excludes
  them on the intent through the primary key, but later writes are plain upserts on both.
- **The `pipeline.yaml` CLI is only partly on the conformance semantics.** `ports/agentic-pipeline`
  runs a document on the `agentic` core when the core can run all of it. A document that needs MCP,
  remote peers, Redis or other external stores, classifier guardrails, skills or non-hashing
  embeddings falls back to the older `pyagentic` engine, which has no event log, no `turn_id` and no
  journal, and the CLI says so on stderr.
- **Nothing here has had an outside security review.** See [security](docs/security.md) for what is
  proved and what is left out.

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

`./mvnw -f reactor/pom.xml verify` runs every JVM test. To run an agent from Kafka on a Flink cluster,
see [running on a cluster](docs/runtimes/flink.md#running-on-a-cluster).

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

State is exactly-once, effects are at-least-once under a stable key, and anything that cannot take a
key is an explicit human decision: see [Guarantees](#guarantees) for what that does and does not
cover.

## What is in the repository

| Path | What it is |
|---|---|
| [`spec/`](spec/README.md) | `agentic/v1`: the primitives, schemas, fixtures, reference runtime and conformance runner |
| [`ports/jagentic-core`](ports/jagentic-core/README.md) | The Java core |
| [`ports/pyagentic`](ports/pyagentic/README.md) | The Python core: the conformance-tested `agentic` package and the older `pyagentic` package |
| [`ports/agentic-pipeline`](ports/agentic-pipeline/README.md) | The `pipeline.yaml` CLI; `local` is its only backend |
| Root module (`src/`, `pom.xml`) | The Flink adapter (`org.agentic.flink.runtime`, `pipeline`) |
| [`agentic-pekko`](agentic-pekko/README.md) | The Pekko runtime |
| `agentic-gateway` | The HTTP front door: authentication, tenancy, and a backend for Flink over Kafka, Pekko or the Java core |
| [`examples/pipelines`](examples/pipelines) | Example workflow documents |
| `reactor/` | The Maven parent and aggregator for the JVM modules |

## Documentation

| Page | What it covers |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | From a clone to a running agent |
| [docs/runtimes/](docs/runtimes/README.md) | What each runtime is made of, and where conformance results are |
| [docs/portability/pipelines.md](docs/portability/pipelines.md) | The `pipeline.yaml` schema |
| [docs/python.md](docs/python.md) | The Python API |
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
`agentic-flink` jar marks the embedding SDKs and the Postgres driver optional, so add the ones you use
to your own pom; see the `<optional>` entries in the root `pom.xml`.


## Contributing

Contributions are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) describes the toolchain, the local
checks and what a pull request needs. Changes so far are in [CHANGELOG.md](CHANGELOG.md), and
vulnerability reporting is in [SECURITY.md](SECURITY.md). Participation is subject to the
[code of conduct](CODE_OF_CONDUCT.md).

## License

[Apache License 2.0](LICENSE).

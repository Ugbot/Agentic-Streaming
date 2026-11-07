# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog (https://keepachangelog.com/en/1.1.0/) and the project
follows Semantic Versioning for its artifacts as described in `docs/versioning.md`. Nothing has
been released yet; every entry below is on `main` and unreleased, and the project is experimental.
Entries name the pull request that delivered them. The artifact version on `main` is
`1.0.0-SNAPSHOT` for the JVM modules; the Python artifacts take their version from a tag when one
exists.

## [Unreleased]

### Changed

- **Behaviour change: deny by default at the tool boundary.** A path may call only what its `tools`
  lists, plus its skills' tools, and leaving `tools` out means none, where before it meant all. Timers
  and CEP patterns need `policies.unattended_tools`. A refused tool is `tool_refused` and ends the turn
  with the new error class `policy`; the LLM brain no longer offers a refusal back to the model, and
  `LlmBrain.withStrictTools` is gone. Java code that builds a `RoutedGraph` must give it a `ToolPolicy`.
- **Behaviour change: nobody may resume a turn unless `policies.approvers` names them,** and a signal
  must carry an authenticated principal. A refused signal is `signal_refused`. Beside a principal,
  `user_id` is the principal's subject.
- **Behaviour change: egress is refused to private and metadata addresses** for HTTP tools, peers and
  model calls unless `AGENTIC_EGRESS_ALLOW_PRIVATE=true`. Redirects are not followed.
- **Behaviour change: `mcp[].command` starts only what `AGENTIC_MCP_ALLOWED_COMMANDS` lists.**
- **The Pekko HTTP routes are replaced by `agentic-gateway`,** which needs a token and does not start
  without authentication configured.
- New: `policies.tool_approval`, `policies.limits`, `policies.redact`, `tools[].auth` and `a2a[].auth`
  by reference, an OPA adapter, crypto-shredding with `AGENTIC_KEY_VAULT_DIR`, an OSV scan and SBOMs.

- **Behaviour change: a tool that declares no `effect` is now `non_idempotent`, not `idempotent`.**
  It is never called twice: `policies.retry` gives it one attempt, and a call with no recorded result
  becomes `unknown_outcome` and suspends for a person, where before it was made again under its key.
  The same goes for `a2a` peers, whose client retries are switched off under `non_idempotent`, and
  `mcp` servers; both now accept `effect`. A Flink job refuses an undeclared tool without a durable
  `stores.effects`. To keep the old behaviour, say `effect: idempotent` on the tool; a tool that only
  reads says `effect: read`. Every example declares its tools.
- A journal entry's `intent` is now a SHA-256 of the agent id and canonical JSON, and a model entry
  has one too. An entry a run of an earlier build left behind no longer matches and ends that turn
  `fatal`, so let turns in flight finish before upgrading.
- `on_unavailable: degrade` on `stores.effects` no longer applies to a workflow with a
  `non_idempotent` tool: the journal failing to open stops the workflow.

### Fixed

- `LlmBrain` sent the `tool` role, which OpenAI refuses with a 400 without a `tool_call_id`. An
  observation now goes back as a user message, as in Python.
- A journal could answer one call with another's result: the JVM's intent was a 32-bit hash, a model
  entry had no intent, and two workflows on one store shared entries. See `spec/v1/primitives.md`,
  section 10.
- A durable log over a journal that lost its entry made a second call on the JVM, and a re-run that
  stopped short of a recorded call completed quietly. Both are now refused.
- A new workflow version declaring a recorded `non_idempotent` call as `idempotent` repeated it.
- The Kafka journal served reads from what it held when opened, so a conversation that moved to an
  already open process missed what was written elsewhere. A Postgres that stopped answering hung the
  turn for good. A journal outage on Pekko spent the `turn_id`.

### Added

- `provider: anthropic` and `provider: gemini` on the JVM, so on Flink and Pekko, declared in the
  document, keys from `ANTHROPIC_API_KEY` and `GEMINI_API_KEY`. Tested against local stubs only.
- `tools/kafka-journal-tm-loss.sh`: the Kafka journal across a killed TaskManager.
- The effect journal (`spec/v1/primitives.md`, section 10): every tool and model call is recorded
  outside the runtime's state and reused by index on a re-run, so a Flink restore no longer repeats a
  payment. Stores: in memory, file (Python), Postgres and a compacted Kafka topic (JVM). Tools declare
  `effect: read`, `idempotent` or `non_idempotent` and receive a deterministic `Idempotency-Key`. A
  `non_idempotent` call with no recorded result becomes `unknown_outcome` and waits for a person.
- Events `llm_called`, `tool_intent`, `compensation_failed` and `unknown_outcome`, and fixtures 32 to
  76 with a `crash_at` harness verb. Every runtime now goes through one comparator, on the whole event
  sequence.
- One resume state machine in the spec. Undoes receive the forward args and result, compensate on any
  failure class, and carry on from the log after a crash.

- `agentic/v1`: the versioned primitive contract (`spec/v1/primitives.md`), the workflow IR
  schema (`spec/v1/workflow.schema.json`), the normalized result schema and the shared
  conformance fixtures under `spec/conformance/v1` (#17). Fixtures 16 to 22 cover the
  capability ids llm_brain, context_window, timers, event_time, cep, checkpoint_recovery and
  parallelism, with a JSON binding output contract for external runners (#29). Fixtures 23 and
  24 cover the per-path verifier (#40).
- `ports/jagentic-core`: the canonical JVM implementation of the spec: dense per-conversation
  event log with the state fold as the only definition of state, single writer per
  conversation, `turn_id` idempotency, attempt-numbered retries, bounded verification,
  reverse-order saga compensation, suspend/resume and replay after restart, a workflow
  validator with the seven rules, and a JUnit conformance binding that reads the fixtures in
  place (#19).
- Flink: the canonical core as a keyed event-log operator with spec conformance on a
  MiniCluster (#21).
- `agentic-pekko`: an event-sourced Pekko runtime over the canonical core, with a Redis
  durability profile implemented as a Pekko persistence journal and snapshot store that passes
  the persistence TCK (#26).
- `agentic-clj`: a conformant Clojure runtime on Datomic with a schema-guided EDN/YAML/JSON
  loader and a direct binding of the shared fixtures (#27).
- `pyflink/` (`agentic-pyflink`): runs the portable workflow document on Flink from Python
  through a JSON to Event bridge jar under `pyflink/java` (#28).
- `ports/pyagentic` (`pyagentic`): the pure Python core, local runtime and shared Python API
  surface (#25).
- `python/` (`agentic-flink`): JVM-backed implementation of the shared Python runtime contract
  over JPype, registering `local-jvm` and `flink-jvm` runtimes (#30). The `flink-jvm` lane
  gained durable restart through savepoints so the replay and suspend/resume fixtures pass
  (#46).
- Conformance matrix runner (`spec/tools/conformance_matrix.py`), the generated capability
  table `docs/capabilities.md` and a conformance workflow (#24); the runner discovers the
  PyFlink and JPype facade bindings and the Pekko and Clojure fixture tests are count-agnostic
  (#31).
- Scripted LLM brain (`llm.provider: stub`) on jvm-core, flink, pekko, clojure and the Python
  bindings, so fixture 16 passes on all nine bindings without a model (#43).
- Context window compaction (`context.compaction: window`) in jagentic-core, agentic-clj and
  pyagentic, inherited by Flink, Pekko and the Python bindings (#42).
- CEP sequence patterns as an in-turn fold over the conversation log on every binding (#45).
- Parallel conversations with per-conversation single writers on every binding, with a
  randomized concurrency test per runtime (#47).
- Per-path verifier: `paths.<name>.verifier` overrides `agent.verifier` in the spec and in
  every runtime (#40).
- Security: bearer authentication on the A2A gateway, tool services and FastAPI gateway, task
  and conversation ownership checks, an outbound URL egress policy with redirect validation,
  agent tool allowlist enforcement, hardened compose stacks and `docs/security.md` (#36).
- Legacy Flink DSL path: async bounded execution, turn deduplication, keyed vector memory,
  chat factory injection, structured tool calls, Kafka dead letters and metric registration
  (#37).
- Packaging without publishing: tag-derived versions for the four Python distributions
  through setuptools-scm, sdist and wheel builds with `twine check`, uv lockfiles, a release
  dry-run script, and a tools.build `build.clj` for `agentic-clj` (#50).
- Maven reactor `reactor/pom.xml` as the parent and aggregator of every first-class JVM
  module under one groupId (`org.jagentic`) and one version, with enforcer rules for Maven 3.9
  and Java 21; CI runs the reactor once and audits skipped tests (#53).
- Runtime pages under `docs/runtimes/`, two-level Python documentation with executable
  snippets, generated per-page matrix excerpts (`docs/tools/matrix_excerpt.py`) and the
  documentation checker `docs/tools/test_docs.py` (#51).
- Examples: Podman-only scripts with shared prerequisite checks, `examples-bin/run-pipeline.sh`,
  `run-ollama.sh`, the markets stack scripts and `tools/smoke-examples.sh` (#54).

### Changed

- The project is marked experimental. The README, `docs/security.md` and the Flink Agents guide now
  state what the code does today, and the claims that did not hold are gone (#99).
- `tools/smoke-examples.sh` and `tools/check-jar-launch.sh` no longer need `examples-bin`.
  `tools-mcp.yaml` and `banking-mcp.yaml` spawn `examples/pipelines/mcp_util_server.py` (#99).
- Java 21 is the baseline for every JVM module, enforced by the build, with a committed Maven
  wrapper (#15).
- Documentation was rewritten in plain prose without en and em dashes, emoji or marketing
  copy (#16), and states the conformance-tested runtimes exactly: the nine bindings of the
  generated matrix; engine adapters under `ports/` are labelled experimental (#33).
- Non-conformant engine adapters moved from `ports/` to `ports/experimental/`; `ports/` holds
  only jagentic-core, pyagentic and agentic-pipeline (#38).
- Python packaging: the `agentic-flink` wheel bundles the shaded framework jar, distinct
  `agentic.runtimes` entry points (`flink-jvm`, `pyflink`), `agentic-pipeline` has a
  `pyproject.toml`, routers follow the spec keyword semantics, and real LLM clients require an
  explicit model name (#35).
- `LlmBrain` in jagentic-core refuses tool calls the agent did not declare (#39).
- Examples resolve the jagentic-core version from its pom instead of hard-coding it, use real
  model defaults and repo-relative paths (#54).

### Fixed

- Dependencies: no open OSV advisory in the resolved trees of the kept modules or either `uv.lock`.
  Jackson and pgjdbc are one version across the reactor, Milvus 2.6 sheds the Hadoop closure, and
  Tika, DJL, jsoup, LangChain4j, Qdrant, Pekko, Netty and the rest move forward. What Flink and the
  Fluss client shade is listed in `docs/security.md` (#101).
- The Fluss client is out of the default uber jar, which loses about 64 MB and the client's old
  relocated Netty, Jackson, Guava and lz4. `-P fluss` puts it back. Selecting a Fluss backend
  without the client now fails with a message rather than falling back to the in-JVM store (#101).
- The Milvus SDK is out of the default uber jar too, which loses about 15 MB, the SDK's relocated
  gRPC and Netty 4.1, and the okhttp, okio, Kotlin and commons-text only it brought. `-P milvus` puts
  them back. Selecting Milvus without the SDK fails with a message (#101).
- `QdrantVectorStore` reads vectors back from a current Qdrant server, which returns them in the
  `dense` variant, and the tests, CI and compose example move to Qdrant v1.20.0 (#101).
- gRPC moves to 1.84.2, which takes the relocated Netty 4.1.127 out of the Qdrant client's path in
  the uber jar. The Milvus SDK's own relocated copy is opt-in, as `docs/security.md` records (#101).
- Flink storage and inference correctness: PostgreSQL `ON CONFLICT` upserts, reopenable stores,
  exact VALID/INVALID verdicts and nested tool-argument JSON (#22).
- Build, conformance gate and stale documentation fixes from review: nested `Entry` type in
  `InMemoryHotVectorIndex`, the conformance staleness gate, the vendored workflow schema in
  pyagentic (#32).
- Example showcase mains: event timestamps and watermarks so bounded CEP input closes,
  moderation threshold, support triage state machine exits, DJL reranker artifact, pyagentic
  A2A peer registration (#54).

### Removed

- The legacy Flink DSL: `Agent.builder()`, `AgentJobGenerator` and everything only they used, in
  all about 58,000 lines of main code and 25,000 of tests. That takes the Java examples, the old
  `a2a`, `inference`, `web`, `retrieve`, `corpus`, `ingest`, `channel`, `storage` and `memory`
  packages, the Fluss and Milvus backends with `-P fluss` and `-P milvus`, the Anthropic and Gemini
  chat connections, and the `@Experimental` annotation. A workflow document on
  `WorkflowTurnFunction` is the one way to define an agent on Flink. The code as it stood is at tag
  `pre-cornerstone`. `OutboundUrlPolicy` moved to `jagentic-core`, and pgvector came across as
  `retrieval.vector_store` kind `pgvector`.
- `pyflink/`, `python/`, `agentic-clj/`, `tool-services/`, `a2a-gateway/`, `banking-job/`,
  `banking-kb/`, `notebooks/` and `examples-bin/`, with the compose files, Dockerfile, env examples
  and scripts that only served them. Their entries in the reactor, CI, Dependabot and the conformance
  runner went too. The code as it stood is at
  [tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone) (#99).
- The publish workflows and release scripts: nothing is published, and the PyPI name `pyagentic`
  belongs to someone else (#99).
- `plugins/flintagents`, the `flink-agents` Maven profile, and the root module's `plan`, `python`,
  `session` and `compile` packages, which only the moved Python facades reached (#99).
- `docs/capabilities.md` and the matrix excerpts: the file named a commit outside this repository and
  reported a scripted stub as a supported LLM brain. Results now come from the Conformance workflow
  (#99).
- `docs/audit-backlog.md`, `docs/release.md`, `docs/inventory.md`, `docs/compose.md` and the OpenAI
  guide for the removed plugin (#99).
- `ports/experimental/` and everything that needed it: the advisory CI jobs, the Dependabot
  entries, and the docs pages for Celery, Kafka Streams, NATS, Pulsar, Temporal and the retired
  parity matrix. The code as it stood is at
  [tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/ports/experimental)
  (#99).
- The `celery`, `nats` and `ray` backends of `ports/agentic-pipeline`, its engine extras and
  `BackendUnavailableError`. `local` is the only backend; any other name, including the
  demonstration-only adapters, raises `ValueError`. The NATS quick start no longer works (#99).
- `docs/parity-matrix.md` in favour of the generated `docs/capabilities.md` (#51).
- The never-consumed `ToolAllowlistUpdate` and `ToolAllowlistAction` control types (#36).
- The Java sources of `ports/pekko`, replaced by a README pointer to `agentic-pekko/` (#38).

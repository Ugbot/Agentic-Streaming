# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog (https://keepachangelog.com/en/1.1.0/) and the project
follows Semantic Versioning for its artifacts as described in `docs/versioning.md`. Nothing has
been released yet; every entry below is on `main` and unreleased. Entries name the pull request
that delivered them. The artifact version on `main` is `1.0.0-SNAPSHOT` for the JVM modules;
the Python and Clojure artifacts take their version from the release tag when one exists.

## [Unreleased]

### Added

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

- Flink storage and inference correctness: PostgreSQL `ON CONFLICT` upserts, reopenable stores,
  exact VALID/INVALID verdicts and nested tool-argument JSON (#22).
- Build, conformance gate and stale documentation fixes from review: nested `Entry` type in
  `InMemoryHotVectorIndex`, the conformance staleness gate, the vendored workflow schema in
  pyagentic (#32).
- Example showcase mains: event timestamps and watermarks so bounded CEP input closes,
  moderation threshold, support triage state machine exits, DJL reranker artifact, pyagentic
  A2A peer registration (#54).

### Removed

- `docs/parity-matrix.md` in favour of the generated `docs/capabilities.md` (#51).
- The never-consumed `ToolAllowlistUpdate` and `ToolAllowlistAction` control types (#36).
- The Java sources of `ports/pekko`, replaced by a README pointer to `agentic-pekko/` (#38).

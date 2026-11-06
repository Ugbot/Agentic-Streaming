# Module ownership map

Phase 0.2 of the cross-runtime plan: what each module is, who owns it, and what happens
to it. Classifications are `canonical` (the one implementation of a concept), `adapter`
(a runtime binding of a canonical implementation), `duplicate` (a second implementation
of something already canonical), `experimental` (a demonstration, not on the critical
path), `tooling`, or `docs`.

File counts are source files (`.java`, `.py`, `.clj`, `.md`) at the time of
writing and are indicative, not exact.

## Tier 1

| Module | Files | Class | Owner workstream | Disposition |
|---|---:|---|---|---|
| `spec/` | new | canonical | common model | the versioned primitive spec, workflow IR, and fixtures; changes need JVM, Clojure, and Python review |
| `ports/jagentic-core/` | 118 | canonical | JVM core | the runtime-free JVM model: `Agent`, `Event`, `TurnResult`, `RoutedGraph`, `ToolRegistry`, `GraphBuilder`, `PipelineLoader`. Grows the spec's turn id, structured tool calls, and event log |
| `src/main/java/org/agentic/flink/` | 410 | adapter, plus a Flink-only API | Flink runtime | contains the Flink runtime (keyed state, CEP, connectors, checkpoints), the `agentic/v1` binding (`runtime/`, `pipeline/FlinkPipelineRunner`), and the pure-Flink `AgentBuilder` DSL (`dsl/Agent`, `tool/ToolRegistry`, `memory/conversation/ConversationStore`) that the Python facade uses. The DSL is kept as a supported Flink-only API; it is outside the `agentic/v1` conformance path |
| `agentic-pekko/` | 29 | adapter | Pekko runtime | actor-per-conversation, event-sourced persistence, sharding, HTTP and Kafka front doors. The Pekko runtime of record |
| `agentic-clj/` | 49 | canonical for Clojure | Clojure runtime | idiomatic Clojure implementation plus Datomic persistence. Reads the same IR, runs the same fixtures |

The JVM has two families of the same nouns, `org.agentic.flink.*` and `org.jagentic.core.*`,
with `pipeline/FlinkPipelineRunner`, `pipeline/FlinkGraphFunction`, and the `runtime/` package
bridging them. The project owner's decision is to keep both: `org.jagentic.core` plus the
Flink binding is the `agentic/v1` conformance path, and the `org.agentic.flink` `AgentBuilder`
DSL stays as the pure-Flink flavor (it is what the Python facade drives). "The same workflow
on another runtime" means the `agentic/v1` workflow path; the DSL is Flink-only by design.

## Tier 2

| Module | Files | Class | Owner workstream | Disposition |
|---|---:|---|---|---|
| `python/agentic_flink/` | 47 | adapter | Python facade | JPype-backed facade over the JVM. Owns type translation, exceptions, futures, tool callbacks, JAR discovery |
| `ports/pyagentic/` | 51 | canonical for Python | pure Python | pure-Python implementation of the same model. Must track the IR and the fixtures, not drift into a separate dialect |
| `ports/agentic-pipeline/` | 8 | tooling | pure Python | the portable `pipeline.yaml` CLI and backend selection. Becomes the pure-Python fixture entry point |
| `python/agentic_flink/pyflink/` | part of 47 | adapter | PyFlink | Python-authored Flink jobs, job submission, connector and checkpoint configuration |

## Runtime demonstrations

The experimental adapters were moved out of this repository. The code as it stood is available
at [tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/ports/experimental).

## Supporting modules

| Module | Files | Class | Disposition |
|---|---:|---|---|
| `a2a-gateway/` | 16 | adapter | standalone Quarkus inbound A2A gateway; built outside the reactor |
| `tool-services/` | 36 | adapter | standalone tool services |
| `docs/portability/` | 15 | docs | design notes per engine. Capability claims move to the generated matrix; the notes keep the reasoning |
| `notebooks/`, `examples/`, `examples-bin/` | - | docs | examples must run from a clean checkout (Phase 7.3) |

## Deprecation and deletion list

This is the list Phase 2 and Phase 3 work against, so that each removal happens in the
workstream that owns the replacement (item 3 is the only one done so far):

1. `org.agentic.flink.dsl.Agent`, `org.agentic.flink.tool.ToolRegistry`, and
   `org.agentic.flink.core.ToolDefinition`: kept. They are the supported Flink-only API
   outside the `agentic/v1` conformance path, not a removal candidate.
2. `org.agentic.flink.storage.memory.InMemoryShortTermStore`: already marked a legacy
   path in `CLAUDE.md`; remove once `memory/` covers it.
3. `ports/pekko/`: done; superseded by `agentic-pekko/`, and its Java was deleted.
4. Any builder option that is accepted and ignored: the plan forbids silent no-ops, so
   each one either gains behavior or is rejected at build time. Phase 2.1 enumerates them.
5. Manually maintained capability tables in `docs/`: replaced by the generated matrix in
   Phase 5.2. Until then they may not call a capability native without a proving test.

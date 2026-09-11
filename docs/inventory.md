# Module ownership map

Phase 0.2 of the cross-runtime plan: what each module is, who owns it, and what happens
to it. Classifications are `canonical` (the one implementation of a concept), `adapter`
(a runtime binding of a canonical implementation), `duplicate` (a second implementation
of something already canonical), `experimental` (a demonstration, not on the critical
path), `tooling`, or `docs`.

File counts are source files (`.java`, `.py`, `.clj`, `.go`, `.md`) at the time of
writing and are indicative, not exact.

## Tier 1

| Module | Files | Class | Owner workstream | Disposition |
|---|---:|---|---|---|
| `spec/` | new | canonical | common model | the versioned primitive spec, workflow IR, and fixtures; changes need JVM, Clojure, and Python review |
| `ports/jagentic-core/` | 118 | canonical | JVM core | the runtime-free JVM model: `Agent`, `Event`, `TurnResult`, `RoutedGraph`, `ToolRegistry`, `GraphBuilder`, `PipelineLoader`. Grows the spec's turn id, structured tool calls, and event log |
| `src/main/java/org/agentic/flink/` | 410 | duplicate and adapter | Flink runtime | contains both the Flink runtime (keyed state, CEP, connectors, checkpoints) and a parallel agent model (`dsl/Agent`, `tool/ToolRegistry`, `memory/conversation/ConversationStore`) that restates `jagentic-core`. The runtime parts stay; the parallel model is reduced to Flink bindings of the core |
| `agentic-pekko/` | 29 | adapter | Pekko runtime | actor-per-conversation, event-sourced persistence, sharding, HTTP and Kafka front doors. The Pekko runtime of record |
| `agentic-clj/` | 49 | canonical for Clojure | Clojure runtime | idiomatic Clojure implementation plus Datomic persistence. Reads the same IR, runs the same fixtures |

The central finding for Phase 2 is the JVM duplication: two families of the same nouns,
`org.agentic.flink.*` and `org.jagentic.core.*`, with only `pipeline/FlinkPipelineRunner`
and `pipeline/FlinkGraphFunction` bridging them. Everything else in the Flink module
carries its own agent, tool registry, and conversation store. Consolidating on
`org.jagentic.core` and leaving `org.agentic.flink` as the Flink binding is the largest
single piece of Phase 2 work, and until it lands, "the same workflow on another runtime"
means the YAML path only.

## Tier 2

| Module | Files | Class | Owner workstream | Disposition |
|---|---:|---|---|---|
| `python/agentic_flink/` | 47 | adapter | Python facade | JPype-backed facade over the JVM. Owns type translation, exceptions, futures, tool callbacks, JAR discovery |
| `ports/pyagentic/` | 51 | canonical for Python | pure Python | pure-Python implementation of the same model. Must track the IR and the fixtures, not drift into a separate dialect |
| `ports/agentic-pipeline/` | 8 | tooling | pure Python | the portable `pipeline.yaml` CLI and backend selection. Becomes the pure-Python fixture entry point |
| `ports/gateway-fastapi/` | 7 | adapter | pure Python | HTTP front door for the Python implementations |
| `python/agentic_flink/pyflink/` | part of 47 | adapter | PyFlink | Python-authored Flink jobs, job submission, connector and checkpoint configuration |

## Runtime demonstrations

These are ports in the original sense: one file or a handful, showing the model on an
engine. They are not on the Tier 1 or Tier 2 critical path, they are not expected to pass
the full fixture set, and documentation must not describe them as supported runtimes.

| Module | Files | Class | Disposition |
|---|---:|---|---|
| `ports/pekko/` | 5 | duplicate | superseded by `agentic-pekko/`; keep only as a minimal example or delete, do not maintain two Pekko paths |
| `ports/temporal/` | 6 | experimental | keep, label experimental |
| `ports/pulsar/` | 8 | experimental | keep, label experimental |
| `ports/kafka-streams/` | 3 | experimental | keep, label experimental |
| `ports/spring/`, `ports/quarkus/` | 10 | experimental | keep, label experimental |
| `ports/celery/`, `ports/nats/`, `ports/ray/`, `ports/dask/`, `ports/faust/`, `ports/airflow/` | 6 | experimental | single-file backends for the Python pipeline CLI; keep, label experimental |
| `ports/go/` | 70 | experimental | outside the critical path until the Tier 1 and Tier 2 contracts are stable, per the plan |

## Supporting modules

| Module | Files | Class | Disposition |
|---|---:|---|---|
| `a2a-gateway/` | 16 | adapter | standalone Quarkus inbound A2A gateway; built outside the reactor |
| `tool-services/` | 36 | adapter | standalone tool services |
| `docs/portability/` | 21 | docs | design notes per engine. Capability claims move to the generated matrix; the notes keep the reasoning |
| `notebooks/`, `examples/`, `examples-bin/` | - | docs | examples must run from a clean checkout (Phase 7.3) |

## Deprecation and deletion list

Nothing is deleted in this change. This is the list Phase 2 and Phase 3 work against, so
that each removal happens in the workstream that owns the replacement:

1. `org.agentic.flink.dsl.Agent`, `org.agentic.flink.tool.ToolRegistry`, and
   `org.agentic.flink.core.ToolDefinition`: fold into `org.jagentic.core` equivalents,
   keep Flink-specific builders as thin bindings.
2. `org.agentic.flink.storage.memory.InMemoryShortTermStore`: already marked a legacy
   path in `CLAUDE.md`; remove once `memory/` covers it.
3. `ports/pekko/`: remove or reduce to an example that points at `agentic-pekko/`.
4. Any builder option that is accepted and ignored: the plan forbids silent no-ops, so
   each one either gains behavior or is rejected at build time. Phase 2.1 enumerates them.
5. Manually maintained capability tables in `docs/`: replaced by the generated matrix in
   Phase 5.2. Until then they may not call a capability native without a proving test.

# Flink runtime

Apache Flink 2.2.1 is the first-class runtime. The main module (root `pom.xml`, artifact
`org.agentic.flink:agentic-flink`) carries two things that must not be confused:

| Surface | Package | Contract | Matrix column |
|---|---|---|---|
| `agentic/v1` event-sourced runtime | `org.agentic.flink.runtime` | `spec/v1`, one workflow document, normalized results | `flink` (and `python-flink` through the [Python facade](python-facade.md)) |
| Legacy Flink DSL | `org.agentic.flink.dsl`, `execution`, `job`, `statemachine` | `Agent.builder()`, `ToolExecutor`, Flink-state memory | none |

The first is described here. The second is a kept, supported, pure-Flink API and has its
[own section](#legacy-flink-dsl-pure-flink-flavor) at the end.

## How a turn runs

`WorkflowTurnFunction` (`src/main/java/org/agentic/flink/runtime/WorkflowTurnFunction.java`) is a
`KeyedProcessFunction<String, Event, TurnResult>`. The stream must be keyed by `conversation_id`;
Flink's per-key ordering is the single writer of
[common primitives](common-primitives.md#one-writer-per-conversation). The function holds the
workflow document as its only serialized configuration and rebuilds the routed graph, tool
registry and retriever from `ports/jagentic-core` in `open()` on every start and restore. The graph
is `org.jagentic.core.RoutedGraph`, the same code the `jvm-core` and `pekko` columns run.

## The event log is keyed state

`KeyedConversationLog` (`KeyedConversationLog.java`) implements the core's `ConversationLog` over
two pieces of keyed state: a `ListState<LogEvent>` holding the conversation's events in order and a
`ValueState<Long>` holding the next dense `sequence`. Every event the graph appends during a turn
goes into that list. There is no separate conversation store: state, transcript, pending
suspension and memory are all folds over the list, recomputed when the key is next touched. Log
entries are serialized with `JsonTypeInfo`, results with `TurnResultTypeInfo`; nothing falls back
to Kryo (`RuntimeSerializationTest`).

A redelivered `turn_id` is found in the log and answered as `duplicate` without running the graph
(`WorkflowTurnFunctionMiniClusterTest.savepointRestartRestoresLogAndKeepsIdempotency`).

## Savepoint restore is the restart

A restart on Flink is stop-with-savepoint followed by a new job restored from that savepoint.
`LocalWorkflowSession` (`LocalWorkflowSession.java`) does exactly this for the embedded, turn by
turn driver used by the Python facade's `flink-jvm` runtime and by tests: `restart()` stops the
running job with a savepoint, shuts the local cluster down, and starts a fresh job from the
savepoint. Only checkpointed keyed state survives, which is the point: the next turn on a
conversation rebuilds it from `KeyedConversationLog` without re-running brains or tools
(`LocalWorkflowSessionTest.restartRestoresTheLogAndDoesNotCallToolsAgain`,
`suspendedTurnSurvivesRestartAndResumes`). The conformance binding (`FlinkConformanceTest` on a
`MiniCluster`) maps the fixture verb `restart_runtime` onto the same sequence, so the `flink`
column's `replay-after-restart` and `suspend-resume` passes in the
[matrix](../capabilities.md#fixtures) are savepoint restores.

## State TTL

`runtime.flink.state_ttl_ms` in the workflow document (read by `FlinkRuntimeOptions`) applies a
Flink `StateTtlConfig` to the log, the sequence counter and the timer registry, with
`OnCreateAndWrite` updates and `NeverReturnExpired` visibility. TTL bounds how long an idle
conversation's log is retained; it does not change turn semantics
(`WorkflowTurnFunctionMiniClusterTest.stateTtlExpiresTheConversationLog`). Without the option,
retention is unbounded.

```yaml
runtime:
  flink:
    state_ttl_ms: 86400000
    resume_after_ms: 5000
    timer_domain: processing_time
```

## Timers

The operator has timer support for suspended turns: with `runtime.flink.resume_after_ms` set, a
suspended turn registers a Flink processing-time or event-time timer, appends `timer_scheduled`,
and on firing appends `timer_fired` and resumes the turn with a `{kind: "timer"}` signal. Pending
timers are kept in keyed state and survive a savepoint restore
(`WorkflowTurnFunctionMiniClusterTest.suspendedTurnResumesFromRegisteredTimer`,
`registeredTimerSurvivesSavepointRestart`,
`WorkflowTurnFunctionHarnessTest.eventTimeTimerResumesSuspendedTurnWhenWatermarkPasses`).

That is not the same thing as the spec's declared `timers` block, and the `flink` binding skips
the three timer fixtures. The matrix therefore records `timers` and `checkpoint_recovery` as
unsupported and `event_time` and `durable_store` as partial for this column; see the
[flink notes](../capabilities.md#flink). This page does not claim otherwise.

## Where it stands

<!-- matrix: flink -->
Derived from [capabilities.md](../capabilities.md) (run 2026-09-14T16:33:10+00:00, commit `ec936052943c`) by
`docs/tools/matrix_excerpt.py`; do not edit by hand. Every capability not listed below is
[supported](../capabilities.md#capabilities) for the binding, meaning every fixture that requires it passed.

Binding `flink`: 21 passed, 0 failed, 3 skipped: `timer-fires` skipped, `event-time-timer` skipped, `timer-survives-restart` skipped.

| Capability | flink |
|---|---|
| `tools` | [partial](../capabilities.md#flink) |
| `timers` | [unsupported](../capabilities.md#flink) |
| `event_time` | [partial](../capabilities.md#flink) |
| `checkpoint_recovery` | [unsupported](../capabilities.md#flink) |
| `durable_store` | [partial](../capabilities.md#flink) |
<!-- /matrix -->

## Running it

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests   # always first
./mvnw package -DskipTests
./mvnw test -Dtest=FlinkConformanceTest                     # the flink column of the matrix
```

From Python, the same operator is reached through the facade's `flink-jvm` runtime
(`FlinkRuntime(...).deploy(spec)`), documented on the [Python facade page](python-facade.md) and
in [docs/python.md](../python.md). The PyFlink binding (`agentic-pyflink`) places the same operator
inside a PyFlink job through a bridge jar and is described on the [PyFlink page](pyflink.md).

## Legacy Flink DSL (pure-Flink flavor)

The `AgentBuilder` DSL (`Agent.builder().withId(...).withSystemPrompt(...).withTools(...).build()`
under `org.agentic.flink.dsl`, with `execution`, `job` and `statemachine`) is a supported, kept API
for writing agents as Flink jobs directly: LangChain4J chat models, `ToolExecutor` tools,
Flink-state short-term memory, the conversation store SPI, CEP, sagas and the A2A gateway.
[docs/getting-started.md](../getting-started.md), [docs/memory.md](../memory.md) and the Flink-runtime
showcases under `docs/examples/` describe it. The Python facade exposes it as
`agentic_flink.Agent.builder()` with `@tool` ([docs/python.md](../python.md#the-legacy-flink-dsl-pure-flink-flavor)),
and [docs/pyflink-integration.md](../pyflink-integration.md) describes its PyFlink plan integration.

It is a different contract from `agentic/v1`: no workflow document, no `spec/v1` event set, no
normalized result, and no column in the [matrix](../capabilities.md). Nothing on this page above
this section applies to it, and nothing in the matrix measures it.

# Flink runtime

Apache Flink 2.2.1 is the primary runtime. The main module (root `pom.xml`, artifact
`org.jagentic:agentic-flink`) carries two things that must not be confused:

| Surface | Package | Contract | Conformance binding |
|---|---|---|---|
| `agentic/v1` event-sourced runtime | `org.agentic.flink.runtime` | `spec/v1`, one workflow document, normalized results | `flink` |
| Legacy Flink DSL | `org.agentic.flink.dsl`, `execution`, `job`, `statemachine` | `Agent.builder()`, `ToolExecutor`, Flink-state memory | none |

The first is described here. The second is deprecated legacy code, kept for now, and has its
[own section](#legacy-flink-dsl-pure-flink-flavor) at the end.

## What this runtime does not do yet

- **It blocks.** `WorkflowTurnFunction.processElement` runs the whole turn synchronously on the Flink
  task thread, including model calls, tool calls and retry sleeps. One slow call stalls every key on
  that subtask and delays checkpoint barriers. Size parallelism accordingly.
- **Tool calls re-run after a restore.** The log is checkpointed keyed state, so it rolls back up to
  one checkpoint interval while the outside world does not. A turn that ran after the last completed
  checkpoint runs again on restore, tools included, and the tool receives no idempotency key. Do not
  use this for payments or other non-idempotent actions.
- **A workflow document cannot declare a real model on Flink.** Only the scripted stub runs on the
  canonical path.
- **There is no supported way to run a `pipeline.yaml` from Kafka on a cluster.** `FlinkPipelineRunner`
  has no Kafka source.

## How a turn runs

`WorkflowTurnFunction` (`src/main/java/org/agentic/flink/runtime/WorkflowTurnFunction.java`) is a
`KeyedProcessFunction<String, Event, TurnResult>`. The stream must be keyed by `conversation_id`;
Flink's per-key ordering is the single writer of
[common primitives](common-primitives.md#one-writer-per-conversation). The function holds the
workflow document as its only serialized configuration and rebuilds the routed graph, tool
registry and retriever from `ports/jagentic-core` in `open()` on every start and restore. The graph
is `org.jagentic.core.RoutedGraph`, the same code the `jvm-core` and `pekko` bindings run.

## The event log is keyed state

`KeyedConversationLog` (`KeyedConversationLog.java`) implements the core's `ConversationLog` over
two pieces of keyed state: a `ListState<LogEvent>` holding the conversation's events in order and a
`ValueState<Long>` holding the next dense `sequence`. Every event the graph appends during a turn
goes into that list. There is no separate conversation store: state, transcript, pending
suspension and memory are all folds over the list, recomputed when the key is next touched. Log
entries are serialized with `JsonTypeInfo`, results with `TurnResultTypeInfo`; nothing falls back
to Kryo (`RuntimeSerializationTest`).

A redelivered `turn_id` whose turn is in the log is answered as `duplicate` without running the
graph (`WorkflowTurnFunctionMiniClusterTest.savepointRestartRestoresLogAndKeepsIdempotency`). That
holds only for a turn that was checkpointed; see above.

## Savepoint restore is the restart

A restart on Flink is stop-with-savepoint followed by a new job restored from that savepoint.
`LocalWorkflowSession` (`LocalWorkflowSession.java`) does exactly this for the embedded, turn by
turn driver used by tests: `restart()` stops the
running job with a savepoint, shuts the local cluster down, and starts a fresh job from the
savepoint. Only checkpointed keyed state survives, which is the point: the next turn on a
conversation rebuilds it from `KeyedConversationLog` without re-running brains or tools
(`LocalWorkflowSessionTest.restartRestoresTheLogAndDoesNotCallToolsAgain`,
`suspendedTurnSurvivesRestartAndResumes`). The conformance binding (`FlinkConformanceTest` on a
`MiniCluster`) maps the fixture verb `restart_runtime` onto the same sequence, so the `flink`
binding's `replay-after-restart` and `suspend-resume` passes are savepoint restores. Savepoints are
taken between turns, so these do not exercise a failure in the window after the last checkpoint.

## Idle expiry

`runtime.flink.state_ttl_ms` in the workflow document (read by `FlinkRuntimeOptions`) is how long an
idle conversation is kept. It is one processing-time timer per conversation, moved forward by every
turn. When it fires, the log, the sequence counter and the timer registry are cleared together.
Without the option, retention is unbounded.

An active conversation can outlive the value for as long as it likes
(`WorkflowTurnIdleExpiryTest.anActiveConversationOutlivesTheTtl`). A conversation with a suspended
turn is kept and looked at again one interval later, so an approval is not lost, which also means a
turn nobody ever answers is retained
(`aSuspendedTurnIsNeverExpired`). Expiry does end the `turn_id` dedupe window: a turn redelivered
after its conversation expired runs again
(`WorkflowTurnFunctionMiniClusterTest.stateTtlExpiresTheConversationLog`).

This is deliberately not Flink state TTL, which tracks list elements one by one: the oldest events
expired under a live conversation and the log stopped folding. A savepoint taken while that was in
place restores (`aSavepointTakenWithFlinkTtlRestoresIntoTheIdleTimer`, on the hashmap and RocksDB
backends), but a restored conversation has no idle timer until its next turn, so one that never
speaks again is not expired.

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

The spec's declared `timers` block runs on the same operator: `WorkflowTurnFunction` fires every
due timer before processing the turn that observes the deadline, appends `timer_fired`, reads
processing time from a pluggable `ProcessingClock` (Flink's clock in production, a fixture clock
in the harness) and event time from the conversation watermark folded from `event_time_ms`.
Pending timers and the clock reading are rebuilt from keyed state after a savepoint restore, so a
timer fires exactly once across a restart. The `flink` binding runs the three timer fixtures.

## Running it

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests   # always first
./mvnw package -DskipTests
./mvnw test -Dtest=FlinkConformanceTest                     # the flink conformance binding
```

## Legacy Flink DSL (pure-Flink flavor)

The `AgentBuilder` DSL (`Agent.builder().withId(...).withSystemPrompt(...).withTools(...).build()`
under `org.agentic.flink.dsl`, with `execution`, `job` and `statemachine`) is deprecated legacy
code, kept for now, for writing agents as Flink jobs directly: LangChain4J chat models, `ToolExecutor`
tools, Flink-state short-term memory, the conversation store SPI, CEP, sagas and outbound A2A.
[docs/getting-started.md](../getting-started.md), [docs/memory.md](../memory.md) and the Flink-runtime
showcases under `docs/examples/` describe it.

It is a different contract from `agentic/v1`: no workflow document, no `spec/v1` event set, no
normalized result, and no conformance binding. Nothing on this page above this section applies to
it, and no fixture measures it.

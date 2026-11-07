# Flink runtime

Apache Flink 2.2.1 is the primary runtime. The main module (root `pom.xml`, artifact
`org.jagentic:agentic-flink`) is the `agentic/v1` event-sourced runtime: `org.agentic.flink.runtime`
and `org.agentic.flink.pipeline`, with one workflow document, normalized results and the `flink`
conformance binding.

## What this runtime does not do yet

- **It blocks.** `WorkflowTurnFunction.processElement` runs the whole turn synchronously on the Flink
  task thread, including model calls, tool calls and retry sleeps. One slow call stalls every key on
  that subtask and delays checkpoint barriers. Size parallelism accordingly.
- **Tool calls re-run after a restore.** The log is checkpointed keyed state, so it rolls back up to
  one checkpoint interval while the outside world does not. A turn that ran after the last completed
  checkpoint runs again on restore, tools included, and the tool receives no idempotency key. Do not
  use this for payments or other non-idempotent actions.
- **A real model on Flink has been tested against a local stand-in server, not a live provider.**
  See [Real models](#real-models).

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

## Running on a cluster

`FlinkPipelineRunner` with `--input-topic` runs a `pipeline.yaml` as a long-running job between
Kafka topics (`KafkaPipelineJob`). Submit the uber jar to a session cluster over REST, or with
`flink run`, where `/opt/agentic/examples` is the mount in `docker-compose-session.yml`:

```bash
flink run -c org.agentic.flink.pipeline.FlinkPipelineRunner \
  target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
  /opt/agentic/examples/pipelines/banking.yaml --input-topic agentic.turns
```

| Flag | Default |
|---|---|
| `--input-topic` | none; this is what selects the Kafka job. The topics must exist |
| `--kafka-bootstrap` | `KAFKA_BOOTSTRAP` in the environment of the process running `main` |
| `--output-topic` | `<input>.results` |
| `--failures-topic` | `<output>.failures` |
| `--group-id` | `agentic-<agent id>` |

A turn is a JSON record. Key it by `conversation_id`, so that a conversation stays on one partition
and its turns stay in order:

```json
{"conversation_id": "c1", "turn_id": "t1", "user_id": "alice", "text": "what is my balance?"}
{"conversation_id": "c1", "turn_id": "t1", "signal": {"approved": true}}
```

`turn_id` is required, not minted: it is what makes a redelivery a `duplicate`. Results are the
`result.schema.json` document, keyed by conversation and written at-least-once. A record that cannot
be a turn reaches the failures topic as `{"kind": "malformed_record", "reason", "record"}` and the
job carries on. A turn ending `fatal` or `transient` reaches it as `{"kind": "turn_failure",
"result"}` as well as the results topic.

`tools/kafka-flink-e2e.sh` proves this on a real Flink 2.2.1 cluster and a real broker, with Docker
or Podman: it submits `banking.yaml`, sends a poison record and a redelivery, kills the TaskManager
after a checkpoint and checks that the conversation carries on from the restored log.

From your own job, `KafkaPipelineJob.assemble` puts the same graph on your environment:

```java complete
import java.nio.file.Path;
import java.util.Map;
import org.agentic.flink.pipeline.FlinkPipelineRunner;
import org.agentic.flink.pipeline.KafkaPipelineJob;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class BankingJob {
  public static void main(String[] args) throws Exception {
    Map<String, Object> spec = FlinkPipelineRunner.loadYaml(Path.of(args[0]));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    KafkaPipelineJob.assemble(
        env,
        spec,
        new KafkaPipelineJob.Options(
            "localhost:9092",
            "agentic.turns",
            "agentic.turns.results",
            "agentic.turns.results.failures",
            "agentic-banking"));
    env.execute("banking");
  }
}
```

To run a document once on a local cluster, in its own JVM:

```bash
bash tools/run-pipeline.sh examples/pipelines/banking.yaml --text "what is my balance?"
```

## Failed turns

Nothing a turn does fails the job. Whatever escapes a turn ends it `failed` with class `fatal`, as
`turn_failed` on the log and a result on the main output, and a redelivery is a `duplicate` of that
failure. Turns ending `fatal` or `transient` also go to the side output
`WorkflowTurnFunction.TURN_FAILURES`. A failing compensation ends the turn `fatal`, after the
remaining compensations have run, and needs repair by hand. Only a log that cannot be reached, which
is a state backend failure, still fails the job (`WorkflowTurnBoundaryTest`).

## Real models

`ChatClientFactories.fromWorkflow()` builds the client the document's `llm` block declares, with the
same code the cores use (`PipelineLoader.chatClient`): `provider: ollama` or `openai`, with `model`,
`base_url`, `temperature`, `max_tokens` and `timeout_ms`. `FlinkPipelineRunner` uses it. Code calling
`assembleResults` or `new WorkflowTurnFunction` passes it explicitly; their default refuses an `llm`
brain when the job is built.

Credentials are not part of the document. The document ships with the job and lands in logs and
savepoints, so `OPENAI_API_KEY` is read from the TaskManager's environment when the operator opens,
and a document with a key that looks like a credential is refused when the job is built, naming the
field and not its value (`WorkflowTurnRealModelTest`).

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

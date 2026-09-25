# Pekko runtime

`agentic-pekko` (`org.jagentic:agentic-pekko`, module directory `agentic-pekko/`) runs `agentic/v1`
workflows on Apache Pekko: one persistent actor per conversation, Pekko Persistence as the event
log, Cluster Sharding as the distributed single writer. It reuses `ports/jagentic-core` for the
graph, tools, guardrails and memory, so the `pekko` column of the
[matrix](../capabilities.md) measures the same `RoutedGraph` as `jvm-core` and `flink`, hosted
in actors. The module README (`agentic-pekko/README.md`) has the class table, build and demo
commands; this page is about how the primitives map.

## The journal is the event log

`entity/ConversationEntity` is a Pekko `EventSourcedBehavior`, one per conversation. Every
`LogEvent` the graph appends is one journal entry with the dense zero-based `sequence`;
conversation state is nothing but `ConversationState.fold(journal)`. `entity/StagedLog` is the
`ConversationLog` the graph writes during a turn: the committed journal plus a staged tail whose
sequences continue densely, persisted atomically when the turn finishes. Recovery replays the
journal without re-invoking brains, tools, guardrails or memory; `ConversationEntityTest` fails if
it does. A redelivered `turn_id` is answered from the fold as `duplicate`, appends nothing and runs
nothing. The conformance verb `restart_runtime` passivates the entities, so the `pekko` column's
`replay-after-restart` and `suspend-resume` passes are journal replays.

## One entity, one writer

The graph runs off the actor thread (a blocking dispatcher and `pipeToSelf`) and concurrent turns
for the same conversation are stashed, so the mailbox is the per-conversation single writer of
[common primitives](common-primitives.md#one-writer-per-conversation). Different conversations run
in parallel (`runtime/ParallelConversationsTest`, fixture `parallel-conversations`).

## Sharding

`cluster/ConversationSharding` wires Cluster Sharding so there is one live entity per conversation
id across nodes, migrated on failover: the single writer becomes distributed without any change to
the entity (`cluster/ConversationShardingTest`, single node).

## Durable timers

`ScheduleTimer` journals `timer_scheduled` with the event to deliver, typically a resume signal;
expiry journals `timer_fired` and processes it as an ordinary turn. Pending timers are re-armed
from the fold after recovery, so a restart or a passivation cannot lose one
(`entity/DurableTimerTest.timerResumesASuspendedTurnAfterRestart`,
`timerFiresAcrossPassivationWhenItExpiresWhileTheEntityIsDown`; with a real Redis,
`RedisJournalIT.durableTimerSurvivesASystemRestartAndFiresExactlyOnce`).

These are runtime timers behind the suspend and resume path. The spec's declared `timers` block
and its three fixtures are skipped by the `pekko` binding, so the matrix records `timers` and
`checkpoint_recovery` as unsupported and `event_time` and `durable_store` as partial for this
column ([pekko notes](../capabilities.md#pekko)). This page does not claim otherwise.

## Journals: memory, Postgres, Cassandra, Redis

`durability/DurabilityProfile` selects the journal by configuration; the entity is journal
agnostic and every profile is a real event journal.

| Profile | Plugin | Configuration |
|---|---|---|
| `memory` | Pekko in-memory journal, dev and test | `application.conf` |
| `postgres` | `pekko-persistence-jdbc` | `application-cluster-jdbc.conf`, `AGENTIC_PG_URL` |
| `cassandra` | `pekko-persistence-cassandra` | `application-cluster-cassandra.conf`, contact points |
| `redis` | in-module `RedisJournal` and `RedisSnapshotStore` (`persistence/redis/`) | `application-redis.conf`, `AGENTIC_REDIS_URL` |

`DurabilityProfile.config()` validates the selected plugin and its required settings and refuses
to start otherwise (`durability/DurabilityProfileTest`); there is no silent fallback. On the
default in-memory journal the events live for the `ActorSystem` lifetime; for a restart across
processes, use one of the durable journals.

### Redis and the AOF requirement

No maintained Pekko persistence plugin for Redis exists, so the module ships one. `RedisJournal`
is an `AsyncWriteJournal` and `RedisSnapshotStore` a snapshot store; both pass the Pekko
persistence TCK (`RedisJournalTckSpec`, `RedisSnapshotStoreTckSpec`). Each `AtomicWrite` is one
Lua script, so all events of a write land together or not at all, and an already present sequence
number fails the write instead of overwriting the log.

A cache-only Redis would lose every conversation on restart, so durability is checked, not
assumed. At start, before the actor system boots and again in the plugin actors,
`RedisDurabilityCheck` runs `CONFIG GET appendonly` and refuses to start unless it is `yes`.
`appendfsync` is reported, not enforced: `always` loses nothing on power failure, `everysec` may
lose up to about one second of acknowledged events, `no` is OS dependent. Run the server with
`redis-server --appendonly yes --appendfsync always`. `agentic-redis-journal.durability-check = off`
exists for managed deployments that forbid `CONFIG` and is logged at WARN on every start.

## Where it stands

<!-- matrix: pekko -->
Derived from [capabilities.md](../capabilities.md) (run 2026-09-14T17:15:17+00:00, commit `eef9c63ea3ff`) by
`docs/tools/matrix_excerpt.py`; do not edit by hand. Every capability not listed below is
[supported](../capabilities.md#capabilities) for the binding, meaning every fixture that requires it passed.

Binding `pekko`: 24 passed, 0 failed, 0 skipped.

| Capability | pekko |
|---|---|
<!-- /matrix -->

Pekko is also reachable from Python through the facade's `pekko` runtime name, but that path is
`not_tested` and unsupported because of a Jackson version conflict with the shaded Flink jar; see
the [Python facade page](python-facade.md#pekko-unsupported).

## Running it

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests   # always first
./mvnw -f agentic-pekko/pom.xml test                        # includes PekkoConformanceTest, the pekko column
./mvnw -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"
```

The real Redis integration tests (`RedisJournalIT` and the TCK specs) run under the
`integration-tests` profile with Testcontainers on Podman; see the module README.

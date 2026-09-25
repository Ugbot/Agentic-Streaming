# Agentic Pekko: a first-class Apache Pekko agent runtime

A genuinely actor-native realization of the agent essence: **one event-sourced, cluster-sharded
entity per conversation**, single-writer + durable + recoverable the *proper* Pekko way, with
the agent brain reused verbatim from the Flink-free `jagentic-core` (router→path→verifier,
tools, RAG, guardrails, LLM brains, the declarative pipeline). A peer to the Flink framework, not
a thin port.

> Supersedes the `ports/pekko/` proof-of-concept (in-memory state, unused persistence dep).

This module is the `pekko` column of the generated [capability matrix](../docs/capabilities.md).
The runtime page, [docs/runtimes/pekko.md](../docs/runtimes/pekko.md), describes the journal as
event log, durable timers, sharding, the durability profiles and where the runtime stands in the
matrix; this README keeps the module-level detail and the commands.

## Why Pekko fits the essence

The agent essence is *one durable thing per conversation, processed in order, surviving failure.*
That is exactly a **sharded, event-sourced actor**: the mailbox gives single-writer ordering; the
event journal gives durability + recovery; Cluster Sharding gives one live entity per
`conversationId` across the cluster. No locks, no keyBy, it's the model.

## Architecture

| Piece | What |
|-------|------|
| `entity/ConversationEntity` | Pekko `EventSourcedBehavior`, one per conversation. Runs the canonical `RoutedGraph.handle` **off the actor thread** (blocking dispatcher + `pipeToSelf`) and **stashes** concurrent turns, so the mailbox is the per-conversation single writer. The journal *is* the spec's event log: every `LogEvent` the graph appends is one journal entry with the dense zero-based `sequence`; state is nothing but `ConversationState.fold(journal)`. Recovery replays the journal **without re-invoking brains, tools, guardrails or memory** (`ConversationEntityTest` fails if it does). Every `turn_id` is an idempotency key: a redelivery is answered from the fold with status `duplicate`, appends nothing and runs nothing. |
| `entity/StagedLog` | The `ConversationLog` the graph writes during a turn: committed journal + staged tail, sequences continue densely; the staged tail is persisted atomically when the turn finishes. |
| Durable timers | `ScheduleTimer` journals `timer_scheduled` with the event to deliver (typically a resume signal); expiry journals `timer_fired` and processes it as an ordinary turn. Pending timers are re-armed from the fold after recovery. |
| `cluster/ConversationSharding` | Cluster Sharding wiring, the distributed single-writer (one entity per id across nodes, migrated on failover). |
| `runtime/PekkoRuntime` | Implements the core `Runtime` SPI; `PekkoBackendProvider` makes **`backend: pekko`** work in any `pipeline.yaml` (via a core `BackendProvider` ServiceLoader hook). |
| `runtime/TurnWire` | The one JSON codec for both front doors: input carries `conversation_id`, explicit `turn_id`, `user_id`, `text`, structured `metadata`/`signal`; output is the normalized `spec/v1/result.schema.json` document. |
| `http/` | Pekko HTTP front door: Agent Card (`/.well-known/agent-card.json`), `POST /agent` (normalized result), `GET /conversations/{id}` (folded state + journal), `/healthz`. Malformed input is a 400, never an invented turn. |
| `kafka/AgentStream` + `KafkaStreamApp` | Pekko Streams: a backpressured `mapAsync` ask-the-entity flow; Kafka ingress/egress (committable source → ask → producer sink, at-least-once + `turn_id` dedupe, so redelivery yields `duplicate`). |
| `durability/` | Pluggable durability (see below). |

## Durability profiles (`DurabilityProfile`)

| Profile | Strategy | Mechanism |
|---------|----------|-----------|
| `memory` | event-sourced | in-memory journal (dev/test), `application.conf` |
| `postgres` | event-sourced | `pekko-persistence-jdbc`, `application-cluster-jdbc.conf` |
| `cassandra` | event-sourced | `pekko-persistence-cassandra`, `application-cluster-cassandra.conf` |
| `redis` | event-sourced | in-module `RedisJournal` + `RedisSnapshotStore` (`persistence/redis/`), `application-redis.conf` |

All profiles are config-only (the entity is journal-agnostic) and every one is a real event
journal. `DurabilityProfile.config()` validates the selected plugin and its required settings
(`AGENTIC_PG_URL`, Cassandra contact points, `AGENTIC_REDIS_URL`) and refuses to start otherwise;
there is no silent fallback. The former Redis write-through profile (a transcript kept outside the
journal) is gone; Redis is now a journal like the others.

### Redis journal

No maintained Pekko persistence plugin for Redis exists (the Akka-era ones are archived and never
got a Pekko build), so this module ships one: `RedisJournal` (`AsyncWriteJournal`) and
`RedisSnapshotStore`, selected purely by `pekko.persistence.journal.plugin = agentic-redis-journal`.
Both pass the Pekko persistence TCK (`RedisJournalTckSpec`, `RedisSnapshotStoreTckSpec`).

Layout, per `persistenceId`: a hash `{prefix}:j:{id}` (field = sequenceNr, value = serialized
`PersistentRepr`), a string `{prefix}:hi:{id}` holding the highest sequenceNr ever written (kept
across `deleteMessagesTo`, so numbers are never reused), and a hash `{prefix}:s:{id}` of snapshots.
Each `AtomicWrite` is one Lua script: all events land together or not at all, and an already
present sequence number fails the write instead of overwriting the log.

**Durability is verified, not assumed.** At start (`DurabilityProfile.preflight`, before the actor
system boots, and again in the plugin actors) the journal runs `CONFIG GET appendonly` and refuses
to start with an actionable message when it is not `yes`: a cache-only Redis would lose every
conversation on restart. `appendfsync` is reported, not enforced; what you get:

| `appendfsync` | Acknowledged events lost on power failure / kernel panic |
|---------------|-----------------------------------------------------------|
| `always`      | none: the write is fsynced before Redis replies, so before the entity treats it as persisted |
| `everysec` (Redis default with AOF on) | up to ~1 s of acknowledged events |
| `no`          | OS-dependent, typically up to ~30 s |

None of these lose data on a Redis *process* crash alone (the kernel still holds the written
bytes); the window is about the machine dying. Set `agentic-redis-journal.durability-check = off`
only for managed deployments (Redis Enterprise, Valkey with persistence) that forbid `CONFIG`; it
is logged at WARN on every start.

```bash
AGENTIC_REDIS_URL=redis://localhost:6379/0 \
  java -Dconfig.resource=application-redis.conf -cp ... org.jagentic.pekko.Main
# the server must run with:  redis-server --appendonly yes --appendfsync always
```

## Run

```bash
mvn -q -f ports/jagentic-core/pom.xml install -DskipTests
mvn -f agentic-pekko/pom.xml package

# console demo (banking graph, in-memory journal)
mvn -f agentic-pekko/pom.xml exec:java

# HTTP front door
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.http.HttpMain
curl localhost:8080/.well-known/agent-card.json
curl -XPOST localhost:8080/agent -H 'content-type: application/json' \
  -d '{"conversation_id":"c1","user_id":"u","text":"what is my balance?"}'

# run ANY shared pipeline.yaml on the actor runtime (backend: pekko, via the BackendProvider SPI)
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# durability / recovery showcase: run turns, passivate the entity, watch it rehydrate
# its transcript from the event journal (no LLM re-run)
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.RecoveryDemo
```

`PipelineMain` proves the declarative parity: `banking.yaml`, `banking-llm.yaml` (stub ReAct),
and `banking-rag.yaml` (HNSW cold-tier recall, skills, context-window, classifier guardrail) all
run unchanged on the event-sourced actor runtime, see `PekkoBackendPipelineTest`.

`RecoveryDemo` passivates a conversation entity and shows the message count survive the restart:
on the default in-memory journal the events live for the `ActorSystem` lifetime; for cross-process
restart use a durable journal (below).

Production profiles: `-Dconfig.resource=application-cluster-jdbc.conf` (+ `AGENTIC_PG_URL` etc.),
`application-cluster-cassandra.conf`, or `application-redis.conf` (+ `AGENTIC_REDIS_URL`).

## Conformance

`PekkoConformanceTest` reads `spec/conformance/v1/fixtures/*.yaml` **in place** (one dynamic test
per fixture), drives each turn through `PekkoSystem`/`PekkoRuntime` (so the entity, journal and
recovery path are exercised, `restart_runtime` passivates the entities), emits the normalized
`result.schema.json` shape and applies the comparison rules of `spec/conformance/v1/README.md`
(regex replies, error class, ordered tool calls, event subsequence / exclusion, state subset).
A fixture whose `requires` the runtime does not declare is recorded as a skip, never a pass.
The current per-fixture outcomes for the `pekko` binding are in the generated
[capability matrix](../docs/capabilities.md#pekko), excerpted on the
[runtime page](../docs/runtimes/pekko.md).

## Tests (all offline by default)

ActorTestKit + PersistenceTestKit, JUnit 5, randomized data: dense journal sequences and
fold-derived state, **recovery that fails if a brain/tool/guardrail runs**, duplicate `turn_id`
delivery (in flight, across turns, after restart, for rejected turns), mailbox ordering under
concurrency, durable timers (fire once, survive passivation), supervision (exception → failed turn;
fatal error → restart with journal intact), single-node Cluster Sharding, profile selection, HTTP
and Pekko Streams/Kafka front doors, `backend: pekko` pipeline parity, and the shared conformance
fixtures. Live Postgres/Kafka round trips run when `AGENTIC_PEKKO_INTEGRATION=true` and fail
clearly if the backing service is not configured.

The default in-memory journal runs with `test-serialization = on`, so every event is round-tripped
through `jackson-cbor` exactly as a durable journal would store it.

### Integration tests (real Redis via Testcontainers on Podman)

```bash
DOCKER_HOST=unix:///run/user/1000/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true \
  mvn -f agentic-pekko/pom.xml test -P integration-tests
```

Runs everything tagged `integration`: `RedisJournalIT` (profile selection, dense sequences and
what lives in Redis, whole-system restart replaying without brain/tool/guardrail, duplicate
`turn_id` after restart, durable timer surviving a restart and firing once, refusal of a
cache-only server and of a missing URL) plus the Pekko persistence TCK for the journal and the
snapshot store. They fail, never skip, when no container runtime is reachable.

# Agentic Pekko: a first-class Apache Pekko agent runtime

A genuinely actor-native realization of the agent essence: **one event-sourced, cluster-sharded
entity per conversation**, single-writer + durable + recoverable the *proper* Pekko way, with
the agent brain reused verbatim from the Flink-free `jagentic-core` (router→path→verifier,
tools, RAG, guardrails, LLM brains, the declarative pipeline). A peer to the Flink framework, not
a thin port.

> Supersedes the `ports/pekko/` proof-of-concept (in-memory state, unused persistence dep).

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

All profiles are config-only (the entity is journal-agnostic) and every one is a real event
journal. `DurabilityProfile.config()` validates the selected plugin and its required settings
(`AGENTIC_PG_URL`, Cassandra contact points) and refuses to start otherwise; there is no silent
fallback. The former Redis write-through profile was removed: it kept a transcript rather than the
spec's event log and could not satisfy replay or dense-sequence guarantees.

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
or `application-cluster-cassandra.conf`.

## Conformance

`PekkoConformanceTest` reads `spec/conformance/v1/fixtures/*.yaml` **in place** (one dynamic test
per fixture), drives each turn through `PekkoSystem`/`PekkoRuntime` (so the entity, journal and
recovery path are exercised, `restart_runtime` passivates the entities), emits the normalized
`result.schema.json` shape and applies the comparison rules of `spec/conformance/v1/README.md`
(regex replies, error class, ordered tool calls, event subsequence / exclusion, state subset).
A fixture whose `requires` the runtime does not declare is recorded as a skip, never a pass.
All 15 fixtures currently pass on Pekko.

## Tests (all offline by default)

ActorTestKit + PersistenceTestKit, JUnit 5, randomized data: dense journal sequences and
fold-derived state, **recovery that fails if a brain/tool/guardrail runs**, duplicate `turn_id`
delivery (in flight, across turns, after restart, for rejected turns), mailbox ordering under
concurrency, durable timers (fire once, survive passivation), supervision (exception → failed turn;
fatal error → restart with journal intact), single-node Cluster Sharding, profile selection, HTTP
and Pekko Streams/Kafka front doors, `backend: pekko` pipeline parity, and the shared conformance
fixtures. Live Postgres/Kafka round trips run when `AGENTIC_PEKKO_INTEGRATION=true` and fail
clearly if the backing service is not configured.

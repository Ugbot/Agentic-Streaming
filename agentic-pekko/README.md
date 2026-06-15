# Agentic Pekko — a first-class Apache Pekko agent runtime

A genuinely actor-native realization of the agent essence: **one event-sourced, cluster-sharded
entity per conversation** — single-writer + durable + recoverable the *proper* Pekko way — with
the agent brain reused verbatim from the Flink-free `jagentic-core` (router→path→verifier,
tools, RAG, guardrails, LLM brains, the declarative pipeline). A peer to the Flink framework, not
a thin port.

> Supersedes the `ports/pekko/` proof-of-concept (in-memory state, unused persistence dep).

## Why Pekko fits the essence

The agent essence is *one durable thing per conversation, processed in order, surviving failure.*
That is exactly a **sharded, event-sourced actor**: the mailbox gives single-writer ordering; the
event journal gives durability + recovery; Cluster Sharding gives one live entity per
`conversationId` across the cluster. No locks, no keyBy — it's the model.

## Architecture

| Piece | What |
|-------|------|
| `entity/ConversationEntity` | Pekko `EventSourcedBehavior`, one per conversation. Runs `RoutedGraph.handle` **off the actor thread** (blocking dispatcher + `pipeToSelf`), **stashes** concurrent turns, and persists one `TurnCommitted` event per turn. Recovery replays events to rebuild state **without re-invoking the LLM**. `turnId` dedupe makes ingress idempotent. |
| `entity/Recording*Store` + `TurnMutations` | A per-turn buffering overlay over the committed transcript view: the pipeline's mid-turn read-backs work, writes are captured as the replayable event payload. |
| `cluster/ConversationSharding` | Cluster Sharding wiring — the distributed single-writer (one entity per id across nodes, migrated on failover). |
| `runtime/PekkoRuntime` | Implements the core `Runtime` SPI; `PekkoBackendProvider` makes **`backend: pekko`** work in any `pipeline.yaml` (via a core `BackendProvider` ServiceLoader hook). |
| `http/` | Pekko HTTP front door: Agent Card (`/.well-known/agent-card.json`) + `POST /agent` — A2A-interoperable. |
| `kafka/AgentStream` + `KafkaStreamApp` | Pekko Streams: a backpressured `mapAsync` ask-the-entity flow; Kafka ingress/egress (committable source → ask → producer sink, at-least-once + turnId dedupe). |
| `durability/` | Pluggable durability (see below). |

## Durability profiles (`DurabilityProfile`)

| Profile | Strategy | Mechanism |
|---------|----------|-----------|
| `memory` | event-sourced | in-memory journal (dev/test) — `application.conf` |
| `postgres` | event-sourced | `pekko-persistence-jdbc` — `application-cluster-jdbc.conf` |
| `cassandra` | event-sourced | `pekko-persistence-cassandra` — `application-cluster-cassandra.conf` |
| `redis` | **write-through** | `WriteThroughConversationEntity` over jagentic-core's `RedisConversationStore` — `application-redis.conf` |

The first three are config-only (the entity is journal-agnostic). Redis has no maintained Pekko
journal, so it's a write-through entity (same `Command` protocol; transcript lives in Redis).

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

# the same pipeline.yaml on the actor runtime
#   backend: pekko   (resolved via the BackendProvider SPI)
```

Production profiles: `-Dconfig.resource=application-cluster-jdbc.conf` (+ `AGENTIC_PG_URL` etc.),
`application-cluster-cassandra.conf`, or `application-redis.conf` (+ `AGENTIC_REDIS_URL`).

## Tests (11, all offline)

ActorTestKit + PersistenceTestKit: routing, multi-turn persisted transcript, extended-graph through
the seam, **recovery-without-replaying-the-pipeline**, turnId dedupe; `backend: pekko` pipeline parity;
HTTP (real server, JDK client); Pekko Streams ordering; Redis write-through (broker-free); single-node
Cluster Sharding. Live Postgres/Cassandra/Redis/Kafka are opt-in integration tests.

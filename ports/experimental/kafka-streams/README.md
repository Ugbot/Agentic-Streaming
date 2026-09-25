# agentic-kafka-streams

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

A minimal port of the **Agentic-Flink** essence onto **Kafka Streams** (Processor API),
reusing the pure-Java core `org.jagentic:jagentic-core:1.0.0-SNAPSHOT` byte-for-byte with **no
Flink dependency**. See the design it follows: `docs/portability/kafka-streams.md`.

## How it maps

The banking `router -> path -> verifier` graph (`Banking.buildGraph()`) is hosted by a
single `Processor` in a Kafka Streams `Topology`. The source topic `banking.requests` is
**keyed by `conversationId`**, so each conversation lands on one partition handled by one
`StreamThread`, the analogue of Flink's `keyBy` (single-writer-per-conversation, in
order). Inside `process()` the processor builds an `AgentContext` over two persistent,
changelog-backed `KeyValueStore`s: (a) `banking-short-term`, adapted to the core's
`KeyedStateStore` SPI (the analogue of Flink keyed `ValueState`, one record per scalar slot),
and (b) `banking-conversations`, adapted to the core's `ConversationStore` SPI by
`KeyValueConversationStore` (the cross-turn transcript, attributes and user index, one record
per message so an append never rewrites the whole transcript; history is bounded to the newest
200 messages per conversation). Nothing about a conversation is held on the heap between
records. It then runs `RoutedGraph.handle(event, ctx)` and forwards the verified reply to the
sink topic `banking.responses`. Both stores are registered via `StoreBuilder`s over
`Stores.persistentKeyValueStore` and wired into the agent node with `addStateStore`, so
`addSource -> addProcessor -> addStateStore -> addSink` *is* the DAG (Flink's
`StreamExecutionEnvironment` graph).

The `userId` of a turn is read from the `userId` record header when present and otherwise
derived from the record key as `user-<conversationId>`. The core `Event` is built through the
adapter's `EventBuilder` (named fields), so no call site depends on the positional order of
`userId` and `text` in the core constructors.

Because the banking brain is rule-based it runs in-thread; a real LLM/A2A path
would split the call across a **response topic** (async-completion, doc §3.6) so the
`StreamThread` is never blocked, the seam is marked at the `graph.handle(...)` call.

## Build, test and run

```
./mvnw -f reactor/pom.xml -DskipTests install                          # installs jagentic-core 1.0.0-SNAPSHOT
./mvnw -f ports/experimental/kafka-streams/pom.xml test                # TopologyTestDriver suites, no broker needed
./mvnw -f ports/experimental/kafka-streams/pom.xml exec:java           # print topology.describe() (no broker needed)
```

The module is not part of the reactor, so it carries its own enforcer rule (JDK 21 or newer,
Maven 3.9 or newer). The tests run the topology in `TopologyTestDriver`: `BankingTopologyTest`
covers routing and tool calls for the default and an injected graph; `KeyValueConversationStoreTest`
covers the store layout (arbitrary message content, bounded history, attributes, user index,
per-conversation `clear`, separator rejection); `BankingTopologyRecoveryTest` closes the topology
after randomized turns, opens a new driver with empty local state, replays the changelog records
the first instance produced for `banking-conversations`, and checks that the transcript and user
index are back and that the next turn continues the same conversation. `TopologyTestDriver.close()`
wipes the local state directory, so this changelog replay is the recovery path the driver can
exercise; it is also the path a real instance without local state takes.

`exec:java` runs `org.jagentic.ports.kafkastreams.BankingTopology#main`, which builds the
`Topology` and prints its `describe()`, the static DAG, no live Kafka broker required.

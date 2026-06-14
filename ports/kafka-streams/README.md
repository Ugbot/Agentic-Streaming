# agentic-kafka-streams

A minimal port of the **Agentic-Flink** essence onto **Kafka Streams** (Processor API),
reusing the pure-Java core `org.jagentic:jagentic-core:0.1.0` byte-for-byte with **no
Flink dependency**. See the design it follows: `docs/portability/kafka-streams.md`.

## How it maps

The banking `router -> path -> verifier` graph (`Banking.buildGraph()`) is hosted by a
single `Processor` in a Kafka Streams `Topology`. The source topic `banking.requests` is
**keyed by `conversationId`**, so each conversation lands on one partition handled by one
`StreamThread` — the analogue of Flink's `keyBy` (single-writer-per-conversation, in
order). Inside `process()` the processor builds an `AgentContext` over (a) a
changelog-backed `KeyValueStore` adapted to the core's `KeyedStateStore` SPI — the
analogue of Flink keyed `ValueState` — and (b) a `ConversationStore` for the cross-turn
transcript/attributes, then runs `RoutedGraph.handle(event, ctx)` and forwards the
verified reply to the sink topic `banking.responses`. The state store is registered via a
`StoreBuilder` and wired into the agent node with `addStateStore`, so `addSource ->
addProcessor -> addStateStore -> addSink` *is* the DAG (Flink's `StreamExecutionEnvironment`
graph). Because the banking brain is rule-based it runs in-thread; a real LLM/A2A path
would split the call across a **response topic** (async-completion, doc §3.6) so the
`StreamThread` is never blocked — the seam is marked at the `graph.handle(...)` call.

## Build & run

```
mvn -f ports/kafka-streams/pom.xml compile          # compile (downloads deps online)
mvn -f ports/kafka-streams/pom.xml exec:java         # print topology.describe() (no broker needed)
```

`exec:java` runs `org.jagentic.ports.kafkastreams.BankingTopology#main`, which builds the
`Topology` and prints its `describe()` — the static DAG, no live Kafka broker required.

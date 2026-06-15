# Content-moderation walkthrough

> **Flink-runtime showcase** — exercises Flink-only capabilities (**OutputTag side outputs** + a
> classifier hard-gate). Not the portable baseline; for the agent that runs unchanged on every
> runtime see [the banking agent on every runtime](banking-everywhere.md).

> Source: `src/main/java/org/agentic/flink/example/moderation/ContentModerationExample.java`
> Inline README: `src/main/java/org/agentic/flink/example/moderation/README.md`

## Why this shape

Moderation has the opposite cost shape from triage: most content is fine, a
small fraction needs to be blocked. The classifier has to be the first thing
each post sees — running an LLM and *then* throwing the output away would be
indefensible at scale.

The pipeline is therefore "classifier first, LLM second, audit always":

```
Post
  │
  ▼  Classifier — Toxic-BERT (~50 ms CPU per post)
  │
  ├──── unsafe  ─►  side output  ─►  AuditingListener (HTTP / Postgres / Kafka)
  │
  └──── safe    ─►  LLM summary  ─►  main output
```

Toxic-BERT exposes four labels: `toxic`, `severe_toxic`, `obscene`, `threat`.
Any of them in the block-set sends the post to the side output without an LLM
call.

## Why a side output, not a filter?

A `DataStream<Post>` filter would silently drop the blocked posts. We want the
audit trail. Flink's `OutputTag<BlockedPost>` keeps the blocked stream
addressable as a separate sink — straightforward to write to Postgres via the
framework's `LongTermMemoryStore`, push to Kafka with a `KafkaSink`, or hit an
audit HTTP endpoint (the demo's choice, for zero infra).

## Listener choice

Two listeners are wired in the example:

- `MetricsAgentEventListener` — in-memory counters for `getInferences()`,
  `getGuardrailBlocks()`, etc. Hook these to Flink's `MetricGroup` for
  Prometheus / OpenTelemetry.
- `AuditingListener` — POSTs each block to an HTTP audit endpoint. In
  production, replace this with a `LongTermMemoryStore`-backed listener — see
  cookbook recipe #9.

The listeners fire from inside the per-key `ProcessFunction`, not the SPI's
own emission sites, because this example wires the classifier directly rather
than through a guardrail. To use the framework's built-in guardrail emission
instead, register a `ClassifierGuardrail` on the agent and call the LLM via
`LLMClient.withGuardrails(...)` — see the support-triage example.

## Kafka source

The demo uses `env.fromElements(...)` for determinism. To switch to Kafka:

```java
KafkaSource<Post> source = KafkaSource.<Post>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("user-posts")
    .setGroupId("moderator")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new JsonPostDeserializer())
    .build();
DataStream<Post> posts =
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-posts");
```

Add `flink-connector-kafka` to your local pom (the framework marks it
optional). The compose snippet at the bottom of this doc spins up a Kafka +
Zookeeper pair for local testing.

## Backpressure shape

Toxic-BERT inference dominates the per-record cost (~50 ms). LLM calls only
happen for safe posts (~80%+ of the stream typically) and are async-friendly.
If you find the operator buffering up, raise parallelism on the keyBy and
consider a smaller classifier — `unitary/unbiased-toxic-roberta` is 2× faster
on CPU at a tiny recall cost.

## Compose snippet for Kafka

```yaml
# docker-compose-kafka.yml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Boot with `docker compose -f docker-compose.yml -f docker-compose-kafka.yml up
-d`.

# Content-moderation example

Real-time pipeline that splits incoming posts into a "safe → summarize" main
output and a "blocked → audit" side output, driven entirely by a toxicity
classifier upstream of the LLM.

```
Posts (DataStream / Kafka)
  │
  ▼  Classifier (Toxic-BERT, four labels)
  │
  ├──── label ∈ {toxic, threat, …}  ─►  side output  ─►  HTTP audit endpoint
  │
  └──── safe  ─►  LLM summary  ─►  main output
```

## What's interesting

- **Toxicity classifier as a hard gate** rather than a soft guardrail — blocked
  posts never reach the LLM, saving cost and latency.
- **Real Flink streaming wrapper** — the agent operator's `open()` binds DJL
  and Ollama once per subtask; the spec is the only thing in the job graph.
- **Side outputs for audit** — `OutputTag<BlockedPost>` keeps the audit trail
  out of the main path. The `AuditingListener` POSTs each block to an HTTP
  audit endpoint; swap for `LongTermMemoryStore` or Kafka in production.
- **MetricsAgentEventListener** counts every classification, every chat call,
  and every guardrail block — wire up to Prometheus / OpenTelemetry through
  Flink's `MetricGroup`.

## Prerequisites

```bash
docker compose up -d ollama postgres
docker compose exec ollama ollama pull qwen2.5:3b
```

Add a DJL native binary to your local pom:

```xml
<dependency>
  <groupId>ai.djl.pytorch</groupId>
  <artifactId>pytorch-native-cpu</artifactId>
  <version>0.30.0</version>
  <scope>runtime</scope>
</dependency>
```

For the Kafka-backed variant: `docker compose -f docker-compose.yml -f
docker-compose-kafka.yml up -d`.

## Run

```bash
mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.moderation.ContentModerationExample"
```

Or:

```bash
./examples-bin/run-moderation.sh
```

## Swapping in a Kafka source

```java
KafkaSource<Post> source = KafkaSource.<Post>builder()
    .setBootstrapServers("localhost:9092")
    .setTopics("user-posts")
    .setGroupId("moderator")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new JsonPostDeserializer())
    .build();
DataStream<Post> posts =
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-posts");
```

## Expected output

```
p-001 | alice | Positive feedback on the new release.
BLOCKED p-002 by bob label=toxic score=0.91
p-003 | carol | Asking how to export to CSV.
BLOCKED p-004 by dave label=threat score=0.78
```

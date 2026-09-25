# Content-moderation walkthrough

> **Flink-runtime showcase**, exercises Flink-only capabilities (**OutputTag side outputs** + a
> classifier hard-gate). Not the portable baseline; for the agent that runs unchanged on every
> runtime see [the banking agent on every runtime](banking-everywhere.md).

> Source: `src/main/java/org/agentic/flink/example/moderation/ContentModerationExample.java`
> Inline README: `src/main/java/org/agentic/flink/example/moderation/README.md`

## Running it

```bash
bash examples-bin/run-ollama.sh      # Ollama in Podman on 127.0.0.1:11434, pulls qwen2.5:3b
bash examples-bin/run-moderation.sh
```

Prerequisites: JDK 21, the Maven wrapper, Podman (for Ollama), and outbound internet on the
first run: DJL downloads `unitary/toxic-bert` from Hugging Face plus the PyTorch CPU native
runtime (about 700 MB) into its cache directory (`DJL_CACHE_DIR`, or `.djl.ai` under the home
directory). No API key. `AUDIT_ENDPOINT` (default `http://localhost:8081/audit`) receives the
blocked posts; when nothing listens there the POST failure is logged and the job continues.
The script resolves the provided-scope Flink dependencies and runs the example in a forked JVM,
see `examples-bin/_common.sh`.

The input is a fixed list of four posts. Safe posts print one `id | user | summary` line each on
the main output (`safe-output`); toxic posts print from the side output as
`BLOCKED <id> by <user> label=<label> score=<score>` lines. With the fixed input, `p-001` and
`p-003` are summarized and `p-002` and `p-004` are blocked with `label=toxic`.

## Why this shape

Moderation has the opposite cost shape from triage: most content is fine, a
small fraction needs to be blocked. The classifier has to be the first thing
each post sees, running an LLM and *then* throwing the output away would be
indefensible at scale.

The pipeline is therefore "classifier first, LLM second, audit always":

```
Post
  │
  ▼  Classifier - Toxic-BERT (~50 ms CPU per post)
  │
  ├──── unsafe  ─►  side output  ─►  AuditingListener (HTTP / Postgres / Kafka)
  │
  └──── safe    ─►  LLM summary  ─►  main output
```

Toxic-BERT is a multi-label model, so every post comes back with a top label even when the
score is close to zero. A post goes to the side output without an LLM call only when its top
label is in the block-set (`toxic`, `severe_toxic`, `obscene`, `threat`) and the score is at
least `0.5` (`ContentModerationExample.BLOCK_THRESHOLD`, covered by
`ContentModerationExampleTest`).

## Why a side output, not a filter?

A `DataStream<Post>` filter would silently drop the blocked posts. We want the
audit trail. Flink's `OutputTag<BlockedPost>` keeps the blocked stream
addressable as a separate sink, straightforward to write to Postgres via the
framework's `LongTermMemoryStore`, push to Kafka with a `KafkaSink`, or hit an
audit HTTP endpoint (the demo's choice, for zero infra).

## Listener choice

Two listeners are wired in the example:

- `MetricsAgentEventListener`, in-memory counters for `getInferences()`,
  `getGuardrailBlocks()`, etc. Hook these to Flink's `MetricGroup` for
  Prometheus / OpenTelemetry.
- `AuditingListener`. POSTs each block to an HTTP audit endpoint. In
  production, replace this with a `LongTermMemoryStore`-backed listener, see
  cookbook recipe #9.

The listeners fire from inside the per-key `ProcessFunction`, not the SPI's
own emission sites, because this example wires the classifier directly rather
than through a guardrail. To use the framework's built-in guardrail emission
instead, register a `ClassifierGuardrail` on the agent and call the LLM via
`LLMClient.withGuardrails(...)`, see the support-triage example.

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
optional). `docker-compose-kafka.yml` in the repository root brings up a Kafka plus
Zookeeper pair for local testing, see the end of this page.

## Backpressure shape

Toxic-BERT inference dominates the per-record cost (~50 ms). LLM calls only
happen for safe posts (~80%+ of the stream typically) and are async-friendly.
If you find the operator buffering up, raise parallelism on the keyBy and
consider a smaller classifier, `unitary/unbiased-toxic-roberta` is 2× faster
on CPU at a tiny recall cost.

## Kafka for local testing

`docker-compose-kafka.yml` (repository root) defines a single Confluent 7.4.0 broker plus
Zookeeper on the shared `agentic-flink-network`, advertised to the host as `localhost:9092`
and to other containers as `kafka:29092`. Boot it with Podman:

```bash
bash examples-bin/setup-network.sh
export POSTGRES_PASSWORD=... REDIS_PASSWORD=...     # required by the services in docker-compose.yml
podman-compose -f docker-compose.yml -f docker-compose-kafka.yml up -d
```

Use `podman compose` instead of `podman-compose` where the compose provider plugin is
installed. `bash examples-bin/down-all.sh` stops everything.

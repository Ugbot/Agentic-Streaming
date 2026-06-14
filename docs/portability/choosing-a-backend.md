# Choosing a backend

You build the agent once (in code or a [`pipeline.yaml`](pipelines.md)); the backend
decides *how it runs*. Pick along three axes, in order.

## 1. Delivery model — how do turns arrive?

- **A live keyed event stream** (high throughput, per‑conversation ordering): **Flink**
  (first‑class, richest), **Kafka Streams**, **Faust** (pure Python), or **Pulsar
  Functions**. These give single‑writer‑per‑conversation natively.
- **Online request/response turns** (a service answers a turn): **Local** (embedded),
  **NATS JetStream**, **Celery**, **Ray**, **Pekko**, **Temporal**, **Spring**. Great
  behind an HTTP gateway.
- **Durable, long‑running, retried, human‑in‑the‑loop workflows**: **Temporal** (strongest
  durability) or **Pekko** (actor entities). Turns can span minutes/days and survive
  restarts.
- **Batch / scheduled** (offline RAG ingest, eval sweeps, branching DAGs): **Dask** (data
  plane) or **Airflow** (orchestration). Not the live conversational loop.

## 2. Durability + ordering — native or external?

- **Native C1+C2+C3** (durable keyed state + ordering + fault tolerance, no external DB):
  **Flink** (checkpoints), **Pekko** (sharding + persistence), **Temporal**
  (event‑sourced), **Pulsar Functions** (BookKeeper state). **NATS JetStream** gives
  durable KV + stream with ordering as a convention.
- **Assembled** (keyed state in an external store, ordering via partitions/queue):
  **Kafka Streams**/**Faust**/**Quarkus**/**Spring** (Kafka partitions + Redis/Postgres),
  **Celery** (routed queue + Redis), **Ray** (in‑memory + write‑through).

Whichever you choose, the durable store is **hot‑swappable behind the SPI**
(`stores.conversation: {kind: redis|memory, url: ...}`); bring it up with
[`examples/compose/externals.yml`](../../examples/compose/externals.yml).

## 3. Your stack / language

- **Pure Python**: Faust, Ray, Celery, NATS, Dask, Airflow.
- **JVM**: Flink, Kafka Streams, Pekko, Temporal, Pulsar, Quarkus, Spring.
- **Go**: the `goagentic` core + NATS + Temporal + the stdlib gateway.
- **Just embed it / a service**: the in‑process **Local** runtime in any of the three
  languages, optionally behind the **FastAPI** or **Go** gateway.

## Quick picks

| You want… | Use |
|-----------|-----|
| The richest, battle‑tested streaming agent platform | **Flink** (first‑class) |
| The same essence, pure Python, live stream | **Faust** |
| Durable multi‑step / HITL workflows | **Temporal** |
| Actor entity per conversation on the JVM | **Pekko** |
| Lightweight durable streaming, edge‑friendly | **NATS JetStream** |
| Agents as tasks on a queue you already run | **Celery** |
| Offline RAG indexing / eval at scale | **Dask** |
| Scheduled / triggered agentic DAGs | **Airflow** |
| Embed in a service / quickest start | **Local** + a gateway |

See the **[parity matrix](parity-matrix.md)** for the per‑backend feature/limitation
table, and **[pipelines](pipelines.md)** for the one YAML that runs on any of them.

## Don't lock into Flink

Flink is the first‑class runtime, but the agent — prompts, tools, calls to other agents,
retrieval, memory, guardrails — is engine‑agnostic. Prototype on **Local**, move to a
streaming/durable backend for production by changing `backend:` in your YAML, and keep
external services (Redis/Valkey, Kafka/Fluss, Postgres) behind their interfaces so they
swap without touching agent code.

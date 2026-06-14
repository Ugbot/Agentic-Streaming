# Portability design notes — Agentic-Flink off Flink

What would this project be on a *different* engine? These notes name the
engine-agnostic essence, the capabilities Flink was providing, and an honest
per-engine mapping (what fits, what's awkward, what to drop). Design intent, not a
migration plan.

**Start here:**
- [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md) —
  the essence, the capability inventory, the engine-agnostic core + Engine SPI, the
  capability matrix, and the ranked fit. Every engine doc is written against it.

**Per-engine deep-dives** (same 6-section template; ranked best-fit first):

| Doc | Engine | Lang | Verdict in one line |
|-----|--------|------|---------------------|
| [`faust.md`](faust.md) | Faust (faust-streaming) | Python | Native fit: faust *agents* + *Tables* ≈ our agent + ConversationStore. |
| [`kafka-streams.md`](kafka-streams.md) | Kafka Streams | Java | Closest analog: state stores + partitions + EOS; bridge async I/O. |
| [`pekko.md`](pekko.md) | Apache Pekko | Java | Actor-per-conversation via Cluster Sharding (C1+C2) + Persistence (C3) — all native. |
| [`temporal.md`](temporal.md) | Temporal | Java | Entity workflow per conversation; event-sourced C1+C2+C3 — strongest durability. |
| [`pulsar.md`](pulsar.md) | Apache Pulsar Functions | Java | State store (C1+C3) + Key_Shared (C2) — native, in Flink's topic-in/topic-out shape. |
| [`ray.md`](ray.md) | Ray | Python | Actor-per-conversation = stateful agent; durability + streaming are external. |
| [`nats.md`](nats.md) | NATS JetStream | Python | Durable KV state (C1) + persistent stream (C3); C2 via subject + KV CAS. Lightweight, online. |
| [`quarkus.md`](quarkus.md) | Quarkus | Java | Reactive messaging + Mutiny; keyed state external. Already our proxy. |
| [`spring.md`](spring.md) | Spring | Java | Cloud Stream + StateMachine + Spring AI; dataflow → message channels. |
| [`celery.md`](celery.md) | Celery | Python | One turn = one task; online request/response; C2 via routed queue + lock, state external. |
| [`dask.md`](dask.md) | Dask | Python | Great for batch/parallel RAG + eval; weak for live keyed streaming. |
| [`airflow.md`](airflow.md) | Airflow | Python | Routed graph → DAG; batch/scheduled agentic + RAG ingestion; not streaming. |

For the Python engines (Faust, Ray, NATS JetStream, Celery, Dask, Airflow) the target
is **pure Python**. For the JVM engines (Kafka Streams, Pekko, Temporal, Pulsar
Functions, Quarkus, Spring) the existing Java core types are reused behind the Engine
SPI.

**A third core, in Go.** [`ports/go/`](../../ports/go/) is a pure-Go realization of the
same essence, with its own **NATS JetStream** and **Temporal** engines (so both have
two implementations — Python/Go and Java/Go) and a stdlib HTTP gateway. Together with
the [FastAPI gateway](../../ports/gateway-fastapi/) over `pyagentic`, two HTTP front
doors expose the agent under an identical A2A-style Agent Card — the essence is portable
across Python, the JVM, and Go, behind one contract.

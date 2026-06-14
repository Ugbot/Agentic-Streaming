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
| [`ray.md`](ray.md) | Ray | Python | Actor-per-conversation = stateful agent; durability + streaming are external. |
| [`quarkus.md`](quarkus.md) | Quarkus | Java | Reactive messaging + Mutiny; keyed state external. Already our proxy. |
| [`spring.md`](spring.md) | Spring | Java | Cloud Stream + StateMachine + Spring AI; dataflow → message channels. |
| [`dask.md`](dask.md) | Dask | Python | Great for batch/parallel RAG + eval; weak for live keyed streaming. |
| [`airflow.md`](airflow.md) | Airflow | Python | Routed graph → DAG; batch/scheduled agentic + RAG ingestion; not streaming. |

For the Python engines (Faust, Ray, Dask, Airflow) the target is **pure Python**.
For the JVM engines (Kafka Streams, Quarkus, Spring) the existing Java core types
are reused behind the Engine SPI.

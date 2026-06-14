# `ports/` — Agentic-Flink, built on engines other than Flink

Working implementations of the designs in [`docs/portability/`](../docs/portability/).
Everything builds on one **pure-Python essence core** (`pyagentic/`) — the
engine-agnostic abstractions (per-conversation memory, keyed state, tools, the
`router→path→verifier` routed graph, hot+cold retrieval) — and each engine adapter
is a thin **runtime seam** wiring that core onto its engine.

```
ports/
  pyagentic/      the engine-agnostic core + LocalRuntime  (runs + tested, no deps)
  faust/          Faust agent + Table-backed ConversationStore        (Python)
  ray/            actor-per-conversation runtime                       (Python)
  dask/           batch RAG ingest + eval + routed-graph replay        (Python)
  airflow/        router→path→verifier as a branching DAG + ingest DAG (Python)
  kafka-streams/  Processor API topology, state-store keyed state      (Java)
  spring/         Spring Integration EIP + StateMachine                (Java)
  quarkus/        SmallRye Reactive Messaging + Mutiny                 (Java)
```

## Status (what's verified)

| Port | Lang | State of this version | Verified |
|------|------|------------------------|----------|
| **pyagentic** (core) | Python | Full essence: memory, tools, retrieval, routed graph, LocalRuntime | ✅ 6 tests pass (no deps) |
| **dask** | Python | Parallel RAG ingest + recall@k eval + routed-graph replay | ✅ runs on **real Dask** (and sequential fallback) |
| **airflow** | Python | `routed_triage` branching DAG + `agentic_ingestion` DAG; `simulate()` | ✅ logic runs; DAGs load with Airflow |
| **faust** | Python | `@app.agent` + Table-backed `ConversationStore` | ✅ imports clean (engine-guarded); runs with Kafka + faust-streaming |
| **ray** | Python | actor-per-conversation `RayRuntime` (+ write-through durability) | ✅ imports clean (engine-guarded); runs with `ray[default]` |
| **kafka-streams** | Java | Processor topology, `KeyValueStore` keyed state, async-completion split | ✅ compiles (`mvn -q compile`) |
| **spring** | Java | Spring Integration router→channels→verifier + StateMachine phase | ✅ compiles |
| **quarkus** | Java | Reactive-messaging agent over Kafka + Redis/in-mem state | ✅ compiles |

The Python engines target **pure Python**. Faust/Ray need their engine + a broker/
cluster to *run* (unavailable here on Python 3.14, which predates faust/ray wheels),
so they're import-checked + engine-guarded; their agent logic is the tested core.

## Run it

```bash
# core (no deps)
cd ports/pyagentic && PYTHONPATH=. python -m pytest tests/ -q

# adapters' portable logic (Dask runs on the real engine if installed)
cd ports && PYTHONPATH=pyagentic python -m pytest tests/ -q

# individual demos
python ports/dask/agentic_dask.py            # batch RAG + eval (+real Dask if installed)
python ports/airflow/agentic_banking_dag.py  # routing simulate (no scheduler)
# faust:  faust -A agentic_faust:app worker -l info     (needs Kafka + faust-streaming)
# ray:    python ports/ray/agentic_ray.py               (needs ray[default])
```

## The shared design

Read [`docs/portability/00-essence-and-core-abstractions.md`](../docs/portability/00-essence-and-core-abstractions.md)
first. The throughline these ports prove out: the agent logic is engine-agnostic;
only the **operator/state/DAG seam** differs, and **Redis or Fluss** is the recurring
durable-state answer once Flink's checkpointed keyed state is gone (here the
`ConversationStore`/`KeyedStateStore` SPIs).

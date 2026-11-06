# Choosing a backend

You build the agent once (in code or a [`pipeline.yaml`](pipelines.md)); the backend
decides *how it runs*. Pick along three axes, in order.

## 1. Delivery model: how do turns arrive?

- **A live keyed event stream** (high throughput, per-conversation ordering): **Flink**
  (first-class, richest). It gives single-writer-per-conversation natively.
- **Online request/response turns** (a service answers a turn): **Local** (embedded) or
  **Pekko**.
- **Durable, long-running, retried, human-in-the-loop workflows**: **Pekko** (actor entities).
  Turns can span minutes or days and survive restarts.

## 2. Durability + ordering: native or external?

- **Native C1+C2+C3** (durable keyed state + ordering + fault tolerance, no external DB):
  **Flink** (checkpoints), **Pekko** (sharding + persistence).
- **Local** keeps a lock per conversation in-process; durability comes from the store you
  configure.

Whichever you choose, the durable store is **hot-swappable behind the SPI**
(`stores.conversation: {kind: redis|memory, url: ...}`); bring it up with
[`examples/compose/externals.yml`](../../examples/compose/externals.yml).

## 3. Your stack / language

- **Pure Python**: the in-process **Local** runtime (`pyagentic`).
- **JVM**: Flink, Pekko, or **Local** (`jagentic-core`).

## Quick picks

| You want... | Use |
|-----------|-----|
| Agents over a stream, with Flink state and CEP | **Flink** (the primary runtime) |
| Actor entity per conversation on the JVM | **Pekko** |
| Durable multi-step / HITL workflows | **Pekko** |
| Embed in a service / quickest start | **Local** |

See the **[conformance results](../runtimes/README.md#conformance-results)** for what each runtime passes,
and **[pipelines](pipelines.md)** for the YAML.

## Don't lock into Flink

Flink is the primary runtime, but the agent, prompts, tools, calls to other agents,
retrieval, memory, guardrails, is engine-agnostic. Prototype on **Local**, take the same file to
Pekko or Flink, and keep external services (Redis/Valkey, Kafka/Fluss, Postgres)
behind their interfaces so they swap without touching agent code.

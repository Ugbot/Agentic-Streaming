# Agentic-Flink on NATS JetStream

> Per the keystone [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md).
> NATS JetStream adds persistent streams + a durable **key-value store** on top of the
> NATS messaging system. A working port lives in [`../../ports/nats/`](../../ports/nats/)
> — it **runs live** against a JetStream server and is covered by the adapter suite. A
> pure-Go peer (same KV-state + stream design) lives in
> [`../../ports/go/engines/natsjs/`](../../ports/go/engines/natsjs/).

## 1. Verdict

NATS JetStream is a strong, lightweight fit: it gives **native durable keyed state**
via its **KV store** (C1) and a **persistent, ordered stream** for the turn transport
(C3), all from a single small server. A per-conversation envelope (transcript +
attributes) lives under one KV key keyed by `conversationId`; turns are published to a
JetStream stream and a consumer processes them in publish order, acking after the work
(at-least-once with redelivery). The one soft spot is C2: NATS has no Kafka-style
partition→single-consumer assignment, so strict single-writer-per-conversation is a
convention — route by subject and/or use the KV's **revision (compare-and-set)** as an
optimistic-concurrency backstop. The agent logic is the unchanged pure-Python
`pyagentic` core, run in a load → handle → save bracket around the KV (the same shape
as the Pulsar Function's state-store access). It's online and low-latency, and the KV
gives durability without a separate database.

## 2. Capability mapping (C1..C12)

| Cap | How NATS JetStream supplies it |
|-----|----------------------------------|
| **C1** durable keyed state | **N** — the JetStream **KV store** (a materialized stream): one durable, revisioned envelope per `conversationId`. |
| **C2** per-key ordered processing | **L** — no native per-key consumer assignment; route by subject (`agentic.turn.<cid>`) + KV revision CAS for single-writer. |
| **C3** fault tolerance / durability | **N** — JetStream persists messages (file/memory store), redelivers un-acked ones; the KV envelope makes a redelivered turn idempotent. |
| **C4** async I/O | **N** — the `nats-py` client is asyncio-native. |
| **C5** backpressure | **N** — JetStream flow control + consumer `max_ack_pending` / pull batches. |
| **C6** connectors | **L** — NATS subjects + JetStream sources/mirrors; bridges to other systems. |
| **C7** side outputs | **N** — publish to another subject. |
| **C8** broadcast state | **L** — a KV bucket all consumers read, or a fanout subject. |
| **C9** event-time / windows | **—** — not an event-time engine. |
| **C10** CEP | **—** — out of scope. |
| **C11** distributed scale | **N** — clustered JetStream; consumers scale horizontally. |
| **C12** topology builder | **L** — wire streams/consumers/subjects; no declarative DAG. |

## 3. The core abstractions on NATS JetStream

- **Durable keyed state (C1) = JetStream KV.** [`NatsRuntime`](../../ports/nats/agentic_nats.py)
  stores the per-conversation envelope (transcript + attributes + owner) as one KV value
  under `conv_<cid>`, loaded before the turn and saved after:

  ```python
  async def handle_turn(self, cid, text, user_id):
      store, _owner, revision = await self._load(cid)        # hydrate from JetStream KV
      ctx = AgentContext(cid, user_id, store, self._state, self.tools, self.retriever)
      result = self.graph.handle(Event(cid, text, user_id), ctx)   # the portable essence
      await self._save(cid, store, user_id, revision)        # CAS on revision (C2 backstop)
      return {...}
  ```

- **Transport / ordering = a JetStream stream + consumer.** Turns publish to
  `agentic.turn.<cid>` on the `AGENTIC_TURNS` stream; `run_worker` consumes them in
  publish order, runs `handle_turn`, publishes the reply, and acks (at-least-once).
- **Single-writer per conversation (C2).** Convention, not a guarantee: subject-route a
  conversation and/or use `kv.update(key, val, last=revision)` so a stale writer fails —
  optimistic concurrency standing in for Flink's keyBy.
- **Idempotency (C3).** The KV envelope is the source of truth, so a redelivered turn
  re-runs against the same state; acks happen after the save.
- **Async (C4).** Everything is `asyncio` — the worker, KV ops, and (in a real agent)
  the LLM/A2A calls.
- **Inbound edge.** A producer publishes a turn to `agentic.turn.<cid>`; the reply comes
  back on `agentic.reply.<cid>`.

## 4. Worked example — banking router→path→verifier

[`agentic_nats.py`](../../ports/nats/agentic_nats.py) runs the full round-trip against a
live JetStream server (`podman run -p 4222:4222 nats:latest -js`):

```
[c1] path=cards    ok=True reply='[cards] We offer three card types: classic, gold, and platinum...'
[c1] path=cards    ok=True reply='[cards] Crypto cash-back can be redeemed to a linked wallet...'
[c2] path=payments ok=True reply='[payments] Your balance is 1234.56.'  tools=['get_balance']
[c3] path=general  ok=True reply='[general] ...'

c1 persisted message count = 4 (state durable in JetStream KV)
```

`c1`'s two turns persist to the same KV envelope (4 messages), recovered from the KV
store — proving C1.

## 5. What doesn't fit

- **Strict single-writer ordering.** Without partition→consumer assignment, C2 is a
  convention (subject routing + KV CAS). Under heavy concurrency for one conversation
  you lean on the revision check; a contended writer must retry.
- **Event-time / windows / CEP.** Out of scope — NATS is messaging + KV, not an
  analytics engine.
- **Large transcripts in KV.** KV values should stay modest; bound the transcript (this
  port does) or offload the long tail to a long-term store.
- **Exactly-once.** JetStream is at-least-once; rely on the idempotent KV envelope.

## 6. When to choose NATS JetStream

Choose NATS JetStream when you want a **lightweight, online, durable** home for the
agent essence — native durable keyed state (KV) + a persistent stream from one small
binary, asyncio-native, easy to run at the edge or embedded. It's a great fit when you
already run NATS, or want Kafka-like durability without Kafka's operational weight. If
you need strict per-key single-writer ordering as an engine guarantee, Kafka
Streams/Faust (partitions) or an actor/entity model (Pekko/Temporal) is stronger; NATS
trades that for simplicity and a built-in KV.

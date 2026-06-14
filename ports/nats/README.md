# agentic-nats — Agentic-Flink on NATS JetStream

The agent essence on **NATS JetStream** (persistent streams + durable KV), reusing the
Flink-free `pyagentic` core. See the design in
[`../../docs/portability/nats.md`](../../docs/portability/nats.md).

**The fit:** JetStream's **KV store** is native durable keyed state (**C1**) — one
revisioned envelope per `conversationId`; a **persistent stream + consumer** is the
ordered, redelivering turn transport (**C3**); `nats-py` is asyncio-native (**C4**).
Single-writer (**C2**) is a convention — subject routing + the KV revision
compare-and-set. The turn runs the portable graph in a load → handle → save bracket
around the KV (the same shape as the Pulsar Function's state access).

| Symbol | Role |
|--------|------|
| `NatsRuntime` | `pyagentic.Runtime` over JetStream; KV-backed state + stream transport. Injectable graph/tools/retriever |
| `handle_turn` | load KV envelope → run the portable graph → save envelope (durable C1) |
| `run_worker` | a JetStream consumer: process turns in order, reply, ack (C3) |
| `_load` / `_save` | per-conversation envelope on JetStream KV, with revision CAS (C2 backstop) |

## Run

```bash
# start a JetStream server (podman):
podman run -d --name nats-js -p 4222:4222 nats:latest -js

# run the live stream→worker→KV→reply round-trip:
python ports/nats/agentic_nats.py
# ->
# [c1] path=cards    ok=True reply='[cards] We offer three card types...'
# [c1] path=cards    ok=True reply='[cards] Crypto cash-back can be redeemed...'
# [c2] path=payments ok=True reply='[payments] Your balance is 1234.56.' tools=['get_balance']
# [c3] path=general  ok=True reply='[general] ...'
# c1 persisted message count = 4 (state durable in JetStream KV)
```

Covered by the adapter suite (`ports/tests/test_adapters.py`): a live KV-roundtrip +
extended-graph-through-the-seam test that **skips** when no JetStream server is
reachable. Point at a remote server with `AGENTIC_NATS_URL`.

"""Agentic-Flink on **NATS JetStream** — pure Python.

See ../../docs/portability/nats.md. NATS JetStream is a persistent streaming + KV
layer on NATS. The essence maps cleanly:

  - **JetStream KV** = durable keyed state (C1). A per-conversation envelope
    (transcript + attributes + owner) lives under one KV key ``conv.<cid>`` in a
    JetStream KV bucket — file-backed and revisioned, so it survives restarts and the
    revision enables compare-and-set single-writer (C2 backstop).
  - **A JetStream stream + consumer** is the transport. Turns are published to
    ``agentic.turn.<cid>`` on a persistent stream; a consumer delivers them in publish
    order and the worker acks after processing — at-least-once with redelivery (C3).
    The KV envelope makes a redelivered turn idempotent.
  - **asyncio** is native, giving the async stage (C4) for free.

The turn itself runs the portable router->path->verifier graph from ``pyagentic``,
hydrated from KV before and flushed to KV after — so the durable state is JetStream
KV while the agent logic is the shared, model-free core (the load/run/save bracket,
exactly like the Pulsar Function's state-store access).

Run (needs a JetStream server, e.g. `podman run -p 4222:4222 nats:latest -js`):
    python agentic_nats.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Make the sibling pure-Python core importable (or `pip install -e ../pyagentic`).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pyagentic"))

from pyagentic.banking import build_banking_graph, default_tools, seed_kb  # noqa: E402
from pyagentic.core import AgentContext, Event, RoutedGraph, TurnResult  # noqa: E402
from pyagentic.memory import (  # noqa: E402
    ChatMessage,
    ConversationStore,
    InMemoryConversationStore,
    InMemoryKeyedStateStore,
)
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever  # noqa: E402
from pyagentic.tools import ToolRegistry  # noqa: E402

try:
    import nats
    from nats.js.errors import BucketNotFoundError
    if not hasattr(nats, "connect"):  # guard against a namespace-package shadow
        nats = None
except ImportError:  # keep importable without the engine installed
    nats = None
    BucketNotFoundError = Exception  # type: ignore


DEFAULT_URL = os.environ.get("AGENTIC_NATS_URL", "nats://127.0.0.1:4222")
STREAM = "AGENTIC_TURNS"
TURN_SUBJECT_PREFIX = "agentic.turn."
REPLY_SUBJECT_PREFIX = "agentic.reply."
KV_BUCKET = "agentic_conversations"


# ---- durable per-conversation state on JetStream KV (C1) ------------------------

def _dump_envelope(store: ConversationStore, cid: str, owner: Optional[str]) -> bytes:
    msgs = [[m.role, m.content, m.tool_name, m.tool_call_id] for m in store.history(cid)]
    return json.dumps({"messages": msgs, "attrs": store.attributes(cid), "owner": owner}).encode()


def _hydrate(envelope: Optional[bytes], cid: str) -> Tuple[InMemoryConversationStore, Optional[str]]:
    """Replay a KV envelope into a fresh in-memory store under ``cid``, via the public SPI."""
    store = InMemoryConversationStore()
    if not envelope:
        return store, None
    data = json.loads(envelope.decode())
    for role, content, tool_name, tool_call_id in data.get("messages", []):
        store.append(cid, ChatMessage(role, content, tool_name, tool_call_id))
    for k, v in data.get("attrs", {}).items():
        store.put_attribute(cid, k, v)
    owner = data.get("owner")
    if owner:
        store.associate_user(cid, owner)
    return store, owner


class NatsRuntime:
    """``pyagentic.Runtime`` over NATS JetStream. State is durable in a KV bucket; the
    turn transport is a persistent stream. Stateless graph deps are built once."""

    def __init__(self, url: str = DEFAULT_URL, graph=None, tools=None, retriever=None):
        if nats is None:
            raise RuntimeError("nats-py not installed: pip install nats-py")
        self.url = url
        # Defaults to the shared banking essence; injectable so a new tool/path added to
        # the core (or an extended graph) flows through this engine with no other change.
        self.graph: RoutedGraph = graph if graph is not None else build_banking_graph()
        self.tools: ToolRegistry = tools if tools is not None else default_tools()
        if retriever is not None:
            self.retriever = retriever
        else:
            hot = InMemoryHotVectorIndex()
            seed_kb(hot)
            self.retriever = TwoTierRetriever(hot, None, 4, 4)
        self._state = InMemoryKeyedStateStore()
        self._nc = None
        self._js = None
        self._kv = None

    async def connect(self) -> None:
        self._nc = await nats.connect(self.url)
        self._js = self._nc.jetstream()
        # Idempotent stream + KV bucket creation.
        try:
            await self._js.add_stream(name=STREAM, subjects=[TURN_SUBJECT_PREFIX + "*"])
        except Exception:
            pass  # already exists
        try:
            self._kv = await self._js.key_value(bucket=KV_BUCKET)
        except BucketNotFoundError:
            self._kv = await self._js.create_key_value(bucket=KV_BUCKET)

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.drain()

    def _kv_key(self, cid: str) -> str:
        # KV keys can't contain '.' as a literal segment we want; map cid -> safe key.
        return "conv_" + cid.replace(".", "_")

    async def _load(self, cid: str) -> Tuple[InMemoryConversationStore, Optional[str], Optional[int]]:
        try:
            entry = await self._kv.get(self._kv_key(cid))
            store, owner = _hydrate(entry.value, cid)
            return store, owner, entry.revision
        except Exception:
            store, owner = _hydrate(None, cid)
            return store, owner, None

    async def _save(self, cid: str, store: ConversationStore, owner: Optional[str], revision: Optional[int]) -> None:
        payload = _dump_envelope(store, cid, owner)
        key = self._kv_key(cid)
        if revision is None or revision == 0:
            await self._kv.put(key, payload)
        else:
            # Compare-and-set on the last revision = optimistic single-writer (C2 backstop).
            await self._kv.update(key, payload, last=revision)

    async def handle_turn(self, cid: str, text: str, user_id: str) -> dict:
        """Load KV envelope -> run the portable graph -> persist envelope."""
        store, _owner, revision = await self._load(cid)
        ctx = AgentContext(cid, user_id, store, self._state, self.tools, self.retriever)
        result: TurnResult = self.graph.handle(Event(cid, text, user_id), ctx)
        await self._save(cid, store, user_id, revision)
        return {
            "conversation_id": result.conversation_id,
            "reply": result.reply,
            "path": result.path,
            "ok": result.ok,
            "tool_calls": result.tool_calls,
        }

    async def submit(self, event: Event) -> TurnResult:
        """Direct (non-streamed) submit — load/run/save against KV. Used by the runtime
        Protocol; the streamed path is :meth:`worker` + :meth:`publish_turn`."""
        d = await self.handle_turn(event.conversation_id, event.text, event.user_id)
        return TurnResult(d["conversation_id"], d["reply"], d["path"], d["ok"], d["tool_calls"])

    # ---- streamed transport: publish a turn, a worker consumes + replies ----

    async def publish_turn(self, cid: str, text: str, user_id: str) -> None:
        payload = json.dumps({"conversation_id": cid, "text": text, "user_id": user_id}).encode()
        await self._js.publish(TURN_SUBJECT_PREFIX + self._kv_key(cid), payload)

    async def run_worker(self, stop: asyncio.Event) -> None:
        """A durable JetStream consumer: process turns in publish order, reply, ack."""
        sub = await self._js.subscribe(TURN_SUBJECT_PREFIX + "*", durable="agentic-worker")
        try:
            while not stop.is_set():
                try:
                    msg = await sub.next_msg(timeout=0.5)
                except Exception:
                    continue
                req = json.loads(msg.data.decode())
                d = await self.handle_turn(req["conversation_id"], req["text"], req["user_id"])
                await self._nc.publish(REPLY_SUBJECT_PREFIX + self._kv_key(req["conversation_id"]),
                                       json.dumps(d).encode())
                await msg.ack()
        finally:
            await sub.unsubscribe()


async def _demo() -> None:
    rt = NatsRuntime()
    await rt.connect()

    replies: List[dict] = []
    got = asyncio.Event()

    async def on_reply(msg):
        replies.append(json.loads(msg.data.decode()))
        if len(replies) >= 4:
            got.set()

    rsub = await rt._nc.subscribe(REPLY_SUBJECT_PREFIX + "*", cb=on_reply)
    stop = asyncio.Event()
    worker = asyncio.create_task(rt.run_worker(stop))

    turns = [
        ("c1", "what card types do you offer?"),
        ("c2", "what is my balance?"),
        ("c1", "tell me about crypto cash-back"),
        ("c3", "where is the nearest branch?"),
    ]
    for cid, text in turns:
        await rt.publish_turn(cid, text, "demo")

    try:
        await asyncio.wait_for(got.wait(), timeout=15)
    finally:
        stop.set()
        await worker
        await rsub.unsubscribe()

    for r in sorted(replies, key=lambda r: r["conversation_id"]):
        print(f"[{r['conversation_id']}] path={r['path']} ok={r['ok']} "
              f"reply={r['reply']!r} tools={r['tool_calls']}")
    # Prove C1: c1's two turns persisted to the same KV envelope.
    store, _o, _rev = await rt._load("c1")
    print(f"\nc1 persisted message count = {store.message_count('c1')} (state durable in JetStream KV)")
    await rt.close()


if __name__ == "__main__":  # pragma: no cover
    if nats is None:
        print("nats-py not installed. `pip install nats-py` and run a JetStream server,")
        print("e.g. `podman run -p 4222:4222 nats:latest -js`, then: python agentic_nats.py")
    else:
        asyncio.run(_demo())

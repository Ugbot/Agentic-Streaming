"""Agentic Streaming on **NATS JetStream**, pure Python.

See ../../../docs/portability/nats.md. NATS JetStream is a persistent streaming + KV
layer on NATS. The essence maps cleanly:

  - **JetStream KV** = durable keyed state (C1). A per-conversation envelope
    (transcript + attributes + owner) lives under one KV key ``conv_<cid>`` in a
    JetStream KV bucket, file-backed and revisioned, so it survives restarts and the
    revision enables compare-and-set single-writer (C2 backstop).
  - **A JetStream stream + consumer** is the transport. Turns are published to
    ``agentic.turn.<cid>`` on a persistent stream; a consumer delivers them in publish
    order and the worker acks after processing: at-least-once with redelivery (C3).
    The KV envelope makes a redelivered turn idempotent.
  - **asyncio** is native, giving the async stage (C4) for free.

The turn itself runs the portable router->path->verifier graph from ``pyagentic``,
hydrated from KV before and flushed to KV after, so the durable state is JetStream
KV while the agent logic is the shared, model-free core (the load/run/save bracket,
exactly like the Pulsar Function's state-store access).

Run (needs a JetStream server, e.g. ``podman run -p 4222:4222 nats:latest -js``):
    pip install 'agentic-nats[nats]'
    python -m agentic_nats
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import List, Optional, Tuple

from pyagentic.banking import build_banking_graph, default_tools, seed_kb
from pyagentic.core import AgentContext, Event, RoutedGraph, TurnResult
from pyagentic.memory import (
    ChatMessage,
    ConversationStore,
    InMemoryConversationStore,
    InMemoryKeyedStateStore,
)
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever
from pyagentic.tools import ToolRegistry

try:
    from nats.errors import TimeoutError as NatsTimeoutError
    from nats.js.errors import BadRequestError, BucketNotFoundError, KeyNotFoundError

    import nats
except ImportError as exc:
    raise ImportError(
        "agentic-nats needs the optional 'nats' extra: pip install 'agentic-nats[nats]'"
    ) from exc

__all__ = [
    "DEFAULT_URL",
    "KV_BUCKET",
    "NatsRuntime",
    "REPLY_SUBJECT_PREFIX",
    "STREAM",
    "TURN_SUBJECT_PREFIX",
    "conversation_key",
]

DEFAULT_URL = os.environ.get("AGENTIC_NATS_URL", "nats://127.0.0.1:4222")
STREAM = "AGENTIC_TURNS"
TURN_SUBJECT_PREFIX = "agentic.turn."
REPLY_SUBJECT_PREFIX = "agentic.reply."
KV_BUCKET = "agentic_conversations"


def conversation_key(conversation_id: str) -> str:
    """KV key / subject token for a conversation. ``.`` is a subject separator in NATS,
    so it is mapped to ``_``; the mapping is a pure function of the id, so every
    producer and worker agrees on it."""
    return "conv_" + conversation_id.replace(".", "_")


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
        store.append(cid, ChatMessage(role=role, content=content, tool_name=tool_name, tool_call_id=tool_call_id))
    for k, v in data.get("attrs", {}).items():
        store.put_attribute(cid, k, v)
    owner = data.get("owner")
    if owner:
        store.associate_user(cid, owner)
    return store, owner


class NatsRuntime:
    """``pyagentic.Runtime`` over NATS JetStream. State is durable in a KV bucket; the
    turn transport is a persistent stream. Stateless graph deps are built once."""

    def __init__(
        self,
        url: str = DEFAULT_URL,
        graph: Optional[RoutedGraph] = None,
        tools: Optional[ToolRegistry] = None,
        retriever: Optional[TwoTierRetriever] = None,
        bucket: str = KV_BUCKET,
        stream: str = STREAM,
    ):
        self.url = url
        self.bucket = bucket
        self.stream = stream
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
        # Idempotent stream + KV bucket creation: CREATE on an existing stream with the
        # same subjects succeeds; a differing definition is a 400 we treat as "exists".
        try:
            await self._js.add_stream(name=self.stream, subjects=[TURN_SUBJECT_PREFIX + "*"])
        except BadRequestError:
            await self._js.stream_info(self.stream)
        try:
            self._kv = await self._js.key_value(bucket=self.bucket)
        except BucketNotFoundError:
            self._kv = await self._js.create_key_value(bucket=self.bucket)

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.drain()
            self._nc = None

    async def __aenter__(self) -> "NatsRuntime":
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def _load(self, cid: str) -> Tuple[InMemoryConversationStore, Optional[str], Optional[int]]:
        try:
            entry = await self._kv.get(conversation_key(cid))
        except KeyNotFoundError:
            store, owner = _hydrate(None, cid)
            return store, owner, None
        store, owner = _hydrate(entry.value, cid)
        return store, owner, entry.revision

    async def _save(self, cid: str, store: ConversationStore, owner: Optional[str], revision: Optional[int]) -> None:
        payload = _dump_envelope(store, cid, owner)
        key = conversation_key(cid)
        if revision is None or revision == 0:
            await self._kv.put(key, payload)
        else:
            # Compare-and-set on the last revision = optimistic single-writer (C2 backstop).
            await self._kv.update(key, payload, last=revision)

    async def handle_turn(self, cid: str, text: str, user_id: str) -> dict:
        """Load KV envelope -> run the portable graph -> persist envelope."""
        store, _owner, revision = await self._load(cid)
        ctx = AgentContext(
            conversation_id=cid,
            user_id=user_id,
            store=store,
            state=self._state,
            tools=self.tools,
            retriever=self.retriever,
        )
        result: TurnResult = self.graph.handle(Event(conversation_id=cid, text=text, user_id=user_id), ctx)
        await self._save(cid, store, user_id, revision)
        return {
            "conversation_id": result.conversation_id,
            "reply": result.reply,
            "path": result.path,
            "ok": result.ok,
            "tool_calls": list(result.tool_calls),
        }

    async def submit(self, event: Event) -> TurnResult:
        """Direct (non-streamed) submit: load/run/save against KV. Used by the runtime
        Protocol; the streamed path is :meth:`run_worker` + :meth:`publish_turn`."""
        d = await self.handle_turn(event.conversation_id, event.text, event.user_id)
        return TurnResult(
            conversation_id=d["conversation_id"],
            reply=d["reply"],
            path=d["path"],
            ok=d["ok"],
            tool_calls=d["tool_calls"],
        )

    async def message_count(self, cid: str) -> int:
        store, _owner, _revision = await self._load(cid)
        return store.message_count(cid)

    # ---- streamed transport: publish a turn, a worker consumes + replies ----

    async def publish_turn(self, cid: str, text: str, user_id: str) -> None:
        payload = json.dumps({"conversation_id": cid, "text": text, "user_id": user_id}).encode()
        await self._js.publish(TURN_SUBJECT_PREFIX + conversation_key(cid), payload)

    async def run_worker(self, stop: asyncio.Event, durable: str = "agentic-worker") -> None:
        """A durable JetStream consumer: process turns in publish order, reply, ack."""
        sub = await self._js.subscribe(TURN_SUBJECT_PREFIX + "*", durable=durable, stream=self.stream)
        try:
            while not stop.is_set():
                try:
                    msg = await sub.next_msg(timeout=0.5)
                except NatsTimeoutError:
                    continue
                req = json.loads(msg.data.decode())
                d = await self.handle_turn(req["conversation_id"], req["text"], req["user_id"])
                await self._nc.publish(
                    REPLY_SUBJECT_PREFIX + conversation_key(req["conversation_id"]),
                    json.dumps(d).encode(),
                )
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
    count = await rt.message_count("c1")
    print(f"\nc1 persisted message count = {count} (state durable in JetStream KV)")
    await rt.close()

"""agentic-nats: the banking graph over NATS JetStream (durable KV state, streamed turns).

The pure parts (key mapping, envelope round trip) always run. The live tests connect to
``AGENTIC_NATS_URL`` (default nats://127.0.0.1:4222; ``podman run -p 4222:4222 nats:latest -js``)
and skip with the connection error when no JetStream server answers.

A nats connection is bound to the event loop it was created on, so each live scenario runs
inside one ``asyncio.run``.
"""

from __future__ import annotations

import asyncio
import json
import random

import pytest
from adapter_support import (
    BALANCE_TEXTS,
    CARD_TEXTS,
    FRAUD_TEXTS,
    GENERAL_TEXTS,
    import_adapter,
    pick,
    random_conversation_id,
)
from pyagentic.core import Event
from pyagentic.memory import ChatMessage, InMemoryConversationStore

pytestmark = pytest.mark.nats


@pytest.fixture
def na():
    return import_adapter("agentic_nats")


def test_conversation_key_is_a_pure_subject_safe_mapping(na):
    for _ in range(20):
        cid = random_conversation_id("k") + "." + str(random.randint(0, 999))
        key = na.conversation_key(cid)
        assert key == na.conversation_key(cid)
        assert key.startswith("conv_") and "." not in key
        assert key == "conv_" + cid.replace(".", "_")


def test_envelope_round_trips_history_attributes_and_owner(na, conversation_id):
    store = InMemoryConversationStore()
    store.append(conversation_id, ChatMessage(role="user", content="hi"))
    tool_msg = ChatMessage(role="tool", content="1234.56", tool_name="get_balance", tool_call_id="t9")
    store.append(conversation_id, tool_msg)
    store.put_attribute(conversation_id, "tier", "gold")
    payload = na._dump_envelope(store, conversation_id, "alice")
    assert json.loads(payload)["owner"] == "alice"

    hydrated, owner = na._hydrate(payload, conversation_id)
    assert owner == "alice"
    assert hydrated.message_count(conversation_id) == 2
    assert hydrated.history(conversation_id)[1].tool_call_id == "t9"
    assert hydrated.attributes(conversation_id) == {"tier": "gold"}
    assert hydrated.conversations_for_user("alice") == [conversation_id]

    empty, owner = na._hydrate(None, conversation_id)
    assert owner is None and empty.message_count(conversation_id) == 0


def run_live(na, scenario):
    """Run ``scenario(runtime)`` on a connected runtime in one loop; skip when no server."""

    async def main():
        rt = na.NatsRuntime()
        try:
            await asyncio.wait_for(rt.connect(), timeout=3)
        except (OSError, asyncio.TimeoutError, na.nats.errors.Error) as exc:
            return ("skip", f"no NATS JetStream server reachable at {rt.url}: {exc!r}")
        try:
            return ("ok", await scenario(rt))
        finally:
            await rt.close()

    status, value = asyncio.run(main())
    if status == "skip":
        pytest.skip(value)
    return value


def test_live_kv_state_persists_across_turns_and_runtimes(na):
    cid_a, cid_b = random_conversation_id("na"), random_conversation_id("nb")

    async def first(rt):
        cards = await rt.submit(Event(conversation_id=cid_a, text=pick(CARD_TEXTS), user_id="demo"))
        await rt.submit(Event(conversation_id=cid_a, text=pick(CARD_TEXTS), user_id="demo"))
        pay = await rt.submit(Event(conversation_id=cid_b, text=pick(BALANCE_TEXTS), user_id="demo"))
        return cards.path, pay.path, list(pay.tool_calls), await rt.message_count(cid_a)

    cards_path, pay_path, pay_tools, count = run_live(na, first)
    assert cards_path == "cards" and pay_path == "payments" and "get_balance" in pay_tools
    assert count == 4

    async def second(rt):
        store, owner, revision = await rt._load(cid_a)
        return store.message_count(cid_a), owner, revision

    count_again, owner, revision = run_live(na, second)
    assert count_again == 4 and owner == "demo" and revision is not None and revision > 0


def test_live_streamed_turns_are_processed_by_a_worker_in_order(na):
    cid = random_conversation_id("w")
    texts = [pick(CARD_TEXTS), pick(BALANCE_TEXTS), pick(GENERAL_TEXTS)]

    async def scenario(rt):
        replies = []
        done = asyncio.Event()

        async def on_reply(msg):
            replies.append(json.loads(msg.data.decode()))
            if len(replies) >= len(texts):
                done.set()

        sub = await rt._nc.subscribe(na.REPLY_SUBJECT_PREFIX + na.conversation_key(cid), cb=on_reply)
        stop = asyncio.Event()
        worker = asyncio.create_task(rt.run_worker(stop, durable="agentic-test-" + random_conversation_id("d")))
        for text in texts:
            await rt.publish_turn(cid, text, "demo")
        try:
            await asyncio.wait_for(done.wait(), timeout=20)
        finally:
            stop.set()
            await worker
            await sub.unsubscribe()
        return [r["path"] for r in replies], await rt.message_count(cid)

    paths, count = run_live(na, scenario)
    assert paths == ["cards", "payments", "general"]
    assert count == 2 * len(texts)


def test_live_extended_core_graph_flows_through_the_same_seam(na, extension):
    graph, tools = extension
    cid = random_conversation_id("nf")

    async def main():
        rt = na.NatsRuntime(graph=graph, tools=tools)
        try:
            await asyncio.wait_for(rt.connect(), timeout=3)
        except (OSError, asyncio.TimeoutError, na.nats.errors.Error) as exc:
            return None, f"no NATS JetStream server reachable at {rt.url}: {exc!r}"
        try:
            return await rt.submit(Event(conversation_id=cid, text=pick(FRAUD_TEXTS), user_id="alice")), None
        finally:
            await rt.close()

    res, skip = asyncio.run(main())
    if skip:
        pytest.skip(skip)
    assert res.path == "fraud" and "freeze_card" in res.tool_calls and "FRZ-alice" in res.reply


def test_nats_import_resolves_to_the_installed_distribution(na):
    import nats

    assert "agentic_nats" not in nats.__file__
    assert na.__file__.endswith("agentic_nats/__init__.py")

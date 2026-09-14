"""The ``parallelism`` primitive on ``local-jvm``: N conversations with M turns each, handed to
``JvmLocalRuntime.submit_async`` (``LocalRuntime.submitAsync``) in one shuffled interleaving,
run at the same time across conversations while each conversation stays a serial, isolated
writer. The conformance driver delivers ``concurrent_with`` groups the same way."""

from __future__ import annotations

import random
import string
import threading
from typing import Any

import pytest

from agentic_flink import Event
from agentic_flink import WorkflowAgent as Agent
from agentic_flink._contract import get_runtime
from agentic_flink.conformance import _drive

pytestmark = pytest.mark.usefixtures("af")

MEET = "meet"
PROBE = "probe"

Planned = tuple[str, str, str, str | None]  # conversation, turn, text, expected tool


def _rand(n: int = 6) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


def _plan(conversations: list[str], turns_each: int) -> list[Planned]:
    """One random interleaving that keeps every conversation's own order; each conversation's
    first turn meets the others at the barrier."""
    queues: dict[str, list[Planned]] = {}
    for cid in conversations:
        turns: list[Planned] = [(cid, f"{cid}-t0", f"rendezvous from {cid}", MEET)]
        for i in range(1, turns_each):
            probes = random.random() < 0.5
            turns.append((cid, f"{cid}-t{i}", f"{'charge' if probes else 'hello'} {i} from {cid}",
                          PROBE if probes else None))
        queues[cid] = turns
    plan: list[Planned] = []
    remaining = list(conversations)
    while remaining:
        cid = random.choice(remaining)
        plan.append(queues[cid].pop(0))
        if not queues[cid]:
            remaining.remove(cid)
    return plan


def _spec(agent_id: str, meet, probe):
    return (Agent(agent_id)
            .route(rules={"main": ["charge"]}, default="main")
            .path("main", tools=[MEET, PROBE], tool_triggers={"rendezvous": MEET, "charge": PROBE})
            .use_tool(MEET, meet)
            .use_tool(PROBE, probe)
            .verify("none")
            .build())


def test_local_jvm_runs_conversations_concurrently_and_keeps_each_one_serial_and_isolated():
    n = random.randint(3, 7)
    m = random.randint(3, 6)
    conversations = [f"c{i}-{_rand()}" for i in range(n)]
    plan = _plan(conversations, m)

    # Trips only when all N conversations are inside their first turn at the same time.
    barrier = threading.Barrier(n, timeout=30)
    probes_by_user: dict[str, int] = {}
    guard = threading.Lock()

    def meet(user: str) -> str:
        barrier.wait()
        return f"met:{user}"

    def probe(user: str) -> str:
        with guard:
            probes_by_user[user] = probes_by_user.get(user, 0) + 1
            return f"probe:{user}:{probes_by_user[user]}"

    rt = get_runtime("local-jvm")
    assert rt.capabilities()["parallelism"] == "supported"
    rt.deploy(_spec(f"parallel-{_rand()}", meet, probe))
    try:
        # The user id doubles as the conversation id so tool args reveal which conversation called.
        futures = [rt.submit_async(Event.turn(cid, tid, text, user_id=cid)) for cid, tid, text, _ in plan]
        results = [f.result(timeout=120) for f in futures]
        logs = {cid: rt.events(cid) for cid in conversations}
    finally:
        rt.close()
    assert not barrier.broken, "every conversation reached the barrier inside its first turn together"

    for (cid, tid, _, tool), result in zip(plan, results):
        assert result["status"] == "completed", (tid, result.get("error"))
        assert (result["conversation_id"], result["turn_id"]) == (cid, tid)
        assert len(result["tool_calls"]) == (1 if tool else 0), tid
        for call in result["tool_calls"]:
            assert call["tool"] == tool
            assert call["args"] == {"user": cid}, "tool args belong to the calling conversation"
            assert cid in str(call["result"])

    by_conversation: dict[str, list[tuple[Planned, dict[str, Any]]]] = {}
    for planned, result in zip(plan, results):
        by_conversation.setdefault(planned[0], []).append((planned, result))
    for cid, own in by_conversation.items():
        assert [p[1] for p, _ in own] == [f"{cid}-t{i}" for i in range(m)]
        sequence = 0
        for i, (planned, result) in enumerate(own):
            types = [e["type"] for e in result["events"]]
            assert types[0] == "turn_received" and types[-1] == "turn_completed", (planned[1], types)
            for event in result["events"]:
                assert event["sequence"] == sequence, f"{cid} has one gapless sequence across its turns"
                sequence += 1
            assert result["state"]["turn_count"] == i + 1, f"turn_count counts only {cid}"
            assert result["state"]["transcript_length"] == 2 * (i + 1), f"transcript holds only {cid}'s messages"
        assert len(logs[cid]) == sequence, f"{cid}'s log holds exactly its own events"
        assert [e["sequence"] for e in logs[cid]] == list(range(sequence))
        assert probes_by_user.get(cid, 0) == sum(1 for p, _ in own if p[3] == PROBE), f"probe calls attributed to {cid}"
    assert sum(probes_by_user.values()) == sum(1 for p in plan if p[3] == PROBE), "no probe ran twice or went missing"


def test_conformance_driver_delivers_a_concurrent_group_together_on_local_jvm():
    n = random.randint(3, 6)
    conversations = [f"c{i}-{_rand()}" for i in range(n)]
    barrier = threading.Barrier(n, timeout=30)

    def meet(user: str) -> str:
        barrier.wait()
        return f"met:{user}"

    def probe(user: str) -> str:
        return f"probe:{user}"

    group = [f"{cid}-t0" for cid in conversations]
    turns = [
        {"conversation_id": cid, "turn_id": tid, "text": f"rendezvous from {cid}",
         "concurrent_with": [other for other in group if other != tid]}
        for cid, tid in zip(conversations, group)
    ] + [{"conversation_id": cid, "turn_id": f"{cid}-t1", "text": "hello again"} for cid in conversations]

    rt = get_runtime("local-jvm")
    rt.deploy(_spec(f"parallel-{_rand()}", meet, probe))
    try:
        results = _drive(rt, turns)
    finally:
        rt.close()
    assert not barrier.broken, "the concurrent_with group was in flight together"
    assert [r["turn_id"] for r in results] == [t["turn_id"] for t in turns], "results in declared order"
    for turn, result in zip(turns, results):
        assert result["status"] == "completed" and result["conversation_id"] == turn["conversation_id"]
        assert result["state"]["turn_count"] == (1 if turn["turn_id"].endswith("-t0") else 2)

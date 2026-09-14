"""The `parallelism` primitive on the local runtime and its conformance delivery: N conversations
with M turns each, submitted in one shuffled interleaving through `submit_async`, run at the same
time across conversations while each conversation stays a serial, isolated writer."""

from __future__ import annotations

import random
import threading
import uuid
from concurrent.futures import Future
from typing import Any, Dict, List, Optional, Tuple

from agentic import Agent, Turn
from agentic.conformance import concurrent_batches, run_fixture_document
from agentic.runtime import LocalRuntime

MEET = "meet"
PROBE = "probe"


def rid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def shuffled_plan(conversations: List[str], turns_each: int) -> List[Tuple[str, str, str, Optional[str]]]:
    """(conversation, turn, text, tool) for every turn, in one random interleaving that keeps each
    conversation's own order; the first turn of each conversation meets at the barrier."""
    per_conversation: Dict[str, List[Tuple[str, str, str, Optional[str]]]] = {}
    for cid in conversations:
        turns = [(cid, f"{cid}-t0", f"rendezvous from {cid}", MEET)]
        for i in range(1, turns_each):
            probes = random.random() < 0.5
            turns.append((cid, f"{cid}-t{i}", f"{'charge' if probes else 'hello'} {i} from {cid}",
                          PROBE if probes else None))
        per_conversation[cid] = turns
    interleaved: List[Tuple[str, str, str, Optional[str]]] = []
    remaining = list(conversations)
    while remaining:
        cid = random.choice(remaining)
        interleaved.append(per_conversation[cid].pop(0))
        if not per_conversation[cid]:
            remaining.remove(cid)
    return interleaved


def test_conversations_run_concurrently_while_each_stays_a_serial_isolated_writer():
    n = random.randint(3, 7)
    m = random.randint(3, 7)
    conversations = [rid(f"c{i}") for i in range(n)]
    plan = shuffled_plan(conversations, m)

    # Trips only when all N conversations are inside their first turn at the same time.
    barrier = threading.Barrier(n, timeout=20)
    probes_by_user: Dict[str, int] = {}
    probes_guard = threading.Lock()

    def meet(user: str) -> str:
        barrier.wait()
        return f"met:{user}"

    def probe(user: str) -> str:
        threading.Event().wait(random.random() / 500)
        with probes_guard:
            probes_by_user[user] = probes_by_user.get(user, 0) + 1
            return f"probe:{user}:{probes_by_user[user]}"

    spec = (Agent(rid("parallel"))
            .route(rules={"main": ["charge"]}, default="main")
            .path("main", tool_triggers={"rendezvous": MEET, "charge": PROBE})
            .use_tool(MEET, meet)
            .use_tool(PROBE, probe)
            .verifier("none")
            .build())
    rt = LocalRuntime()
    rt.deploy(spec)
    assert rt.capabilities()["parallelism"] == "supported"
    try:
        # The user id doubles as the conversation id so tool args reveal which conversation called.
        futures: List[Future[Dict[str, Any]]] = [
            rt.submit_async(Turn(cid, tid, text, user_id=cid)) for cid, tid, text, _ in plan]
        results = [f.result(timeout=60) for f in futures]
    finally:
        rt.close()
    assert not barrier.broken, "every conversation reached the barrier inside its first turn together"

    for (cid, tid, _, tool), result in zip(plan, results):
        assert result["status"] == "completed", (tid, result.get("error"))
        assert result["conversation_id"] == cid and result["turn_id"] == tid
        assert len(result["tool_calls"]) == (1 if tool else 0), tid
        for call in result["tool_calls"]:
            assert call["tool"] == tool
            assert call["args"] == {"user": cid}, "tool args belong to the calling conversation"
            assert cid in str(call["result"])
        for event in result["events"]:
            assert event["payload"].get("turn_id", tid) == tid

    by_conversation: Dict[str, List[Tuple[Tuple[str, str, str, Optional[str]], Dict[str, Any]]]] = {}
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
        assert probes_by_user.get(cid, 0) == sum(1 for p, _ in own if p[3] == PROBE), f"probe calls attributed to {cid}"
    assert sum(probes_by_user.values()) == sum(1 for p in plan if p[3] == PROBE), "no probe ran twice or went missing"


def test_submit_async_keeps_arrival_order_within_a_conversation_when_callers_race():
    order: List[str] = []
    hold = threading.Event()

    def slow(user: str) -> str:
        if not order:
            hold.wait(2)
        order.append(user)
        return user

    spec = Agent(rid("a")).path("main", tool_triggers={"go": "slow"}).use_tool("slow", slow).verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    cid = rid("c")
    k = random.randint(4, 12)
    try:
        futures = [rt.submit_async(Turn(cid, f"t{i}", "go", user_id=f"u{i}")) for i in range(k)]
        threading.Event().wait(0.05)
        hold.set()
        results = [f.result(timeout=10) for f in futures]
    finally:
        rt.close()
    assert order == [f"u{i}" for i in range(k)]
    assert [r["state"]["turn_count"] for r in results] == list(range(1, k + 1))
    sequences = [e["sequence"] for r in results for e in r["events"]]
    assert sequences == list(range(len(sequences)))


def test_a_conversation_with_a_long_queue_does_not_starve_the_others():
    # More queued turns than the pool has workers (at most 32), all behind one held first turn.
    hold = threading.Event()
    cid = rid("busy")

    def block(user: str) -> str:
        if user == f"{cid}-t0":
            hold.wait(20)
        return user

    spec = Agent(rid("a")).path("main", tool_triggers={"go": "block"}).use_tool("block", block).verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    queued = random.randint(40, 60)
    try:
        busy = [rt.submit_async(Turn(cid, f"t{i}", "go", user_id=f"{cid}-t{i}")) for i in range(queued)]
        other = rt.submit_async(Turn(rid("other"), "t0", "go", user_id="other"))
        result = other.result(timeout=10)
        assert result["status"] == "completed" and not hold.is_set()
        assert not any(f.done() for f in busy)
        hold.set()
        results = [f.result(timeout=20) for f in busy]
    finally:
        hold.set()
        rt.close()
    assert [r["state"]["turn_count"] for r in results] == list(range(1, queued + 1))


def test_conformance_batches_group_only_consecutive_concurrent_turns():
    turns = [
        {"turn_id": "a1", "conversation_id": "a"},
        {"turn_id": "x1", "conversation_id": "x", "concurrent_with": ["y1", "z1"]},
        {"turn_id": "y1", "conversation_id": "y", "concurrent_with": ["x1", "z1"]},
        {"turn_id": "z1", "conversation_id": "z", "concurrent_with": ["x1", "y1"]},
        {"turn_id": "a2", "conversation_id": "a"},
        {"turn_id": "x2", "conversation_id": "x", "concurrent_with": ["y2"]},
        {"turn_id": "y2", "conversation_id": "y", "concurrent_with": ["x2"]},
    ]
    assert [[t["turn_id"] for t in batch] for batch in concurrent_batches(turns)] == [
        ["a1"], ["x1", "y1", "z1"], ["a2"], ["x2", "y2"]]


def test_conformance_delivers_a_concurrent_group_together_and_reports_in_declared_order():
    n = random.randint(3, 6)
    conversations = [rid(f"c{i}") for i in range(n)]
    barrier = threading.Barrier(n, timeout=20)

    def meet(user: str) -> str:
        barrier.wait()
        return f"met:{user}"

    spec = (Agent(rid("parallel"))
            .path("main", tool_triggers={"rendezvous": MEET})
            .use_tool(MEET, meet)
            .verifier("none")
            .build())
    group = [f"{cid}-t0" for cid in conversations]
    fixture = {
        "id": rid("fixture"),
        "requires": ["parallelism", "ordering", "memory"],
        "workflow": spec,
        "turns": [
            {"conversation_id": cid, "turn_id": tid, "text": f"rendezvous from {cid}",
             "concurrent_with": [other for other in group if other != tid]}
            for cid, tid in zip(conversations, group)
        ] + [{"conversation_id": cid, "turn_id": f"{cid}-t1", "text": "hello again"} for cid in conversations],
        "expect": [
            {"turn_id": tid, "conversation_id": cid, "status": "completed",
             "tool_calls": [{"tool": MEET}], "state_includes": {"turn_count": 1, "transcript_length": 2}}
            for cid, tid in zip(conversations, group)
        ] + [{"turn_id": f"{cid}-t1", "conversation_id": cid, "status": "completed",
              "state_includes": {"turn_count": 2, "transcript_length": 4}} for cid in conversations],
    }
    outcome = run_fixture_document(fixture, LocalRuntime)
    assert outcome.status == "pass", outcome.problems
    assert not barrier.broken, "the group was in flight together"
    assert [r["turn_id"] for r in outcome.results] == group + [f"{cid}-t1" for cid in conversations]

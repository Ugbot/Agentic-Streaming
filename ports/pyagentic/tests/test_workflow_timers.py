"""Workflow timers on the local runtime: the manual processing clock, the event-time
watermark, timers folded from the log, and their exactly-once recovery across restart."""

from __future__ import annotations

import random
from typing import Any, Dict, List, Optional

import pytest

from agentic import Agent, Turn, ValidationError
from agentic.events import InMemoryEventLog, reduce_state
from agentic.runtime import LocalRuntime
from agentic.workflow_timers import ManualClock, TimerState, event_time_of


def _doc(timers: List[Dict[str, Any]]) -> Dict[str, Any]:
    doc = (Agent(f"timers-{random.randrange(1 << 30)}")
           .route(rules={"billing": ["balance"]}, default="general")
           .path("billing")
           .path("general")
           .tool("notify", "constant", value="sent")
           .tool("escalate", "constant", value="raised")
           .verifier("prefix")
           .document())
    doc["timers"] = timers
    return doc


def _runtime(timers: List[Dict[str, Any]], clock: Optional[ManualClock] = None,
             log: Optional[InMemoryEventLog] = None) -> LocalRuntime:
    runtime = LocalRuntime(log=log or InMemoryEventLog(), clock=clock or ManualClock())
    runtime.deploy(_doc(timers))
    return runtime


def _turn(cid: str, n: int, event_time: Optional[int] = None) -> Turn:
    metadata = {"event_time_ms": str(event_time)} if event_time is not None else {}
    return Turn(conversation_id=cid, turn_id=f"{cid}-t{n}", text="hello", metadata=metadata)


def _types(result: Dict[str, Any]) -> List[str]:
    return [e["type"] for e in result["events"]]


# -- clock ---------------------------------------------------------------------------

def test_manual_clock_only_moves_forward():
    start = random.randint(0, 10_000)
    clock = ManualClock(start)
    assert clock() == start
    total = start
    for _ in range(20):
        step = random.randint(0, 5_000)
        total += step
        assert clock.advance(step) == total
        assert clock() == total
    with pytest.raises(ValidationError):
        clock.advance(-1)
    with pytest.raises(ValidationError):
        ManualClock(-random.randint(1, 100))


def test_event_time_is_read_from_metadata_as_a_decimal_string():
    t = random.randint(0, 1 << 40)
    assert event_time_of(_turn("c", 1, t)) == t
    assert event_time_of(_turn("c", 1)) is None
    with pytest.raises(ValidationError):
        event_time_of(Turn(conversation_id="c", turn_id="x", metadata={"event_time_ms": "soon"}))


# -- processing timers ----------------------------------------------------------------

def test_processing_timer_fires_once_before_the_turn_that_observes_the_deadline():
    after = random.randint(100, 5_000)
    clock = ManualClock()
    runtime = _runtime([{"id": "remind", "after_ms": after, "tool": "notify", "payload": {"channel": "email"}}], clock)
    cid = f"c{random.randrange(1000)}"

    first = runtime.submit(_turn(cid, 1))
    assert _types(first)[:2] == ["turn_received", "timer_scheduled"]
    assert first["tool_calls"] == []
    scheduled = next(e for e in first["events"] if e["type"] == "timer_scheduled")["payload"]
    assert scheduled == {"timer_id": "remind", "clock": "processing", "due_ms": after}

    clock.advance(after - 1)
    early = runtime.submit(_turn(cid, 2))
    assert "timer_fired" not in _types(early)
    assert early["tool_calls"] == []

    clock.advance(1)
    due = runtime.submit(_turn(cid, 3))
    assert _types(due)[:3] == ["timer_fired", "tool_called", "turn_received"]
    assert "timer_scheduled" not in _types(due)
    assert due["tool_calls"][0]["tool"] == "notify"
    assert due["tool_calls"][0]["args"] == {"channel": "email"}
    assert due["tool_calls"][0]["index"] == 0
    assert due["state"]["fired_timers"] == ["remind"]

    clock.advance(random.randint(1, 10_000))
    later = runtime.submit(_turn(cid, 4))
    assert "timer_fired" not in _types(later)
    assert later["state"]["fired_timers"] == ["remind"]


def test_timers_are_per_conversation_and_scheduled_once():
    after = random.randint(10, 500)
    clock = ManualClock()
    runtime = _runtime([{"id": "t", "after_ms": after, "tool": "notify"}], clock)
    clock.advance(random.randint(0, 100))
    a_start = clock()
    runtime.submit(_turn("a", 1))
    clock.advance(after // 2 + 1)
    b_start = clock()
    runtime.submit(_turn("b", 1))
    assert TimerState.fold(runtime.events("a")).pending["t"].due_ms == a_start + after
    assert TimerState.fold(runtime.events("b")).pending["t"].due_ms == b_start + after

    clock.advance(after - (clock() - a_start))
    assert "timer_fired" in _types(runtime.submit(_turn("a", 2)))
    assert "timer_fired" not in _types(runtime.submit(_turn("b", 2)))
    assert [e.type for e in runtime.events("a")].count("timer_scheduled") == 1


def test_due_timers_fire_in_deadline_then_id_order_as_the_first_tool_calls():
    specs = []
    for i in random.sample(range(100), 5):
        specs.append({"id": f"t{i:02d}", "after_ms": random.choice([200, 400]),
                      "tool": random.choice(["notify", "escalate"]), "payload": {"n": i}})
    clock = ManualClock()
    runtime = _runtime(specs, clock)
    runtime.submit(Turn(conversation_id="c", turn_id="c-1", text="balance"))
    clock.advance(400)
    result = runtime.submit(Turn(conversation_id="c", turn_id="c-2", text="balance"))
    expected = [s["id"] for s in sorted(specs, key=lambda s: (s["after_ms"], s["id"]))]
    fired = [e["payload"]["timer_id"] for e in result["events"] if e["type"] == "timer_fired"]
    assert fired == expected
    assert result["state"]["fired_timers"] == expected
    calls = result["tool_calls"]
    by_id = {s["id"]: s for s in specs}
    assert [c["tool"] for c in calls[:5]] == [by_id[t]["tool"] for t in expected]
    assert [c["args"] for c in calls[:5]] == [by_id[t]["payload"] for t in expected]
    assert [c["index"] for c in calls[:5]] == [0, 1, 2, 3, 4]
    assert _types(result)[:10] == ["timer_fired", "tool_called"] * 5
    assert _types(result)[10] == "turn_received"


# -- event-time timers --------------------------------------------------------------

def test_event_timer_reads_the_watermark_and_ignores_processing_time():
    after = random.randint(100, 1_000)
    base = random.randint(1_000, 100_000)
    clock = ManualClock()
    runtime = _runtime([{"id": "ev", "after_ms": after, "clock": "event", "tool": "escalate"}], clock)
    cid = "c"
    first = runtime.submit(_turn(cid, 1, base))
    scheduled = next(e for e in first["events"] if e["type"] == "timer_scheduled")["payload"]
    assert scheduled == {"timer_id": "ev", "clock": "event", "due_ms": base + after}
    assert first["state"]["watermark_ms"] == base

    clock.advance(after * 10)
    r2 = runtime.submit(_turn(cid, 2, base + after - 1))
    assert "timer_fired" not in _types(r2)
    assert r2["state"]["watermark_ms"] == base + after - 1

    late = runtime.submit(_turn(cid, 3, base - random.randint(1, base)))
    assert "timer_fired" not in _types(late)
    assert late["state"]["watermark_ms"] == base + after - 1

    r4 = runtime.submit(_turn(cid, 4, base + after))
    assert _types(r4)[:3] == ["timer_fired", "tool_called", "turn_received"]
    assert r4["tool_calls"][0]["tool"] == "escalate"
    assert r4["state"]["watermark_ms"] == base + after
    assert r4["state"]["fired_timers"] == ["ev"]


def test_processing_timer_ignores_event_time():
    runtime = _runtime([{"id": "p", "after_ms": 50, "tool": "notify"}])
    runtime.submit(_turn("c", 1, 1_000))
    r = runtime.submit(_turn("c", 2, 1_000_000))
    assert "timer_fired" not in _types(r)


def test_watermark_never_moves_backwards():
    times = [random.randint(0, 10_000) for _ in range(12)]
    runtime = _runtime([])
    high = None
    for i, t in enumerate(times, 1):
        result = runtime.submit(_turn("w", i, t))
        high = t if high is None else max(high, t)
        assert result["state"]["watermark_ms"] == high
    assert reduce_state(runtime.events("w"))["watermark_ms"] == max(times)


# -- fold and recovery ----------------------------------------------------------------

def test_fold_derives_pending_from_scheduled_minus_fired():
    log = InMemoryEventLog()
    cid = "fold"
    ids = [f"t{i}" for i in range(random.randint(2, 6))]
    log.append(cid, "t1", "turn_received", {"turn_id": "t1", "processing_time_ms": 7, "event_time_ms": 40}, 0, {})
    for i, tid in enumerate(ids):
        log.append(cid, "t1", "timer_scheduled", {"timer_id": tid, "clock": "processing", "due_ms": 100 + i}, 0, {})
    fired = random.sample(ids, len(ids) // 2)
    for tid in fired:
        log.append(cid, "t2", "timer_fired", {"timer_id": tid, "due_ms": 0}, 0, {})
    log.append(cid, "t2", "timer_fired", {"timer_id": "never-scheduled", "due_ms": 0}, 0, {})
    log.append(cid, "t2", "turn_received", {"turn_id": "t2", "processing_time_ms": 900, "event_time_ms": 10}, 0, {})
    state = TimerState.fold(log.read(cid))
    assert set(state.pending) == set(ids) - set(fired)
    assert state.fired == fired
    assert state.processing_time_ms == 900
    assert state.watermark_ms == 40
    assert ManualClock.recovered_from(log)() == 900


def test_timer_survives_restart_and_fires_exactly_once():
    after = random.randint(500, 5_000)
    before = random.randint(1, after - 1)
    log = InMemoryEventLog()
    clock = ManualClock()
    runtime = _runtime([{"id": "remind", "after_ms": after, "tool": "notify", "payload": {"k": "v"}}], clock, log)
    cid = "durable"
    runtime.submit(_turn(cid, 1))
    clock.advance(before)
    runtime.submit(_turn(cid, 2))

    restarted = runtime.restart()
    assert restarted.clock is not clock
    assert restarted.clock() == before
    assert TimerState.fold(log.read(cid)).pending["remind"].due_ms == after
    assert [e.type for e in log.read(cid)].count("timer_scheduled") == 1

    restarted.clock.advance(after - before)
    fired = restarted.submit(_turn(cid, 3))
    assert _types(fired)[:3] == ["timer_fired", "tool_called", "turn_received"]
    assert fired["tool_calls"][0]["args"] == {"k": "v"}
    assert fired["state"]["fired_timers"] == ["remind"]

    again = restarted.restart()
    again.clock.advance(random.randint(0, 10_000))
    quiet = again.submit(_turn(cid, 4))
    assert "timer_fired" not in _types(quiet)
    assert "timer_scheduled" not in _types(quiet)
    types = [e.type for e in log.read(cid)]
    assert types.count("timer_scheduled") == 1
    assert types.count("timer_fired") == 1
    assert quiet["state"]["fired_timers"] == ["remind"]


def test_restart_recovers_the_highest_processing_time_across_conversations():
    log = InMemoryEventLog()
    clock = ManualClock()
    runtime = _runtime([{"id": "t", "after_ms": 10_000, "tool": "notify"}], clock, log)
    high = 0
    for i in range(random.randint(2, 5)):
        clock.advance(random.randint(1, 1_000))
        high = clock()
        runtime.submit(_turn(f"c{i}", 1))
    clock.advance(random.randint(1, 1_000))  # never observed by a turn, so never recorded
    restarted = runtime.restart()
    assert restarted.clock() == high


def test_wall_clock_runtime_keeps_its_clock_across_restart():
    runtime = LocalRuntime()
    runtime.deploy(_doc([]))
    runtime.submit(_turn("c", 1))
    assert runtime.restart().clock is runtime.clock


def test_workflow_without_timers_records_no_processing_time():
    runtime = _runtime([])
    result = runtime.submit(_turn("c", 1))
    received = next(e for e in result["events"] if e["type"] == "turn_received")["payload"]
    assert "processing_time_ms" not in received
    assert "timer_scheduled" not in _types(result)
    assert "fired_timers" not in result["state"]


def test_unknown_timer_clock_is_rejected_at_deploy():
    with pytest.raises(ValidationError):
        _runtime([{"id": "t", "after_ms": 5, "clock": "lunar", "tool": "notify"}])

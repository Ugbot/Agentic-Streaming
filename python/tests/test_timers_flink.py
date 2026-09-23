"""Workflow timers on the ``flink-jvm`` binding: the adapter's ``ManualProcessingClock``
(``clock="manual"``), the event-time watermark folded from ``metadata.event_time_ms`` and
stop-with-savepoint restart, driven from Python (spec/v1/primitives.md section 8).

Deadlines and event times are randomized; every assertion is derived from the drawn values.
The streaming jobs run with parallelism 2 so every operator instance must read the one shared clock.
"""

from __future__ import annotations

import random

import pytest
from agentic_flink.conformance import TimeAdvancing
from agentic_flink.runtimes import FlinkRuntime
from agentic_flink.workflow import Event

pytestmark = pytest.mark.usefixtures("af")


def _workflow(timers):
    return {
        "spec_version": "agentic/v1",
        "backend": "local",
        "timers": timers,
        "agent": {
            "id": f"timers-{random.randrange(1 << 30)}",
            "router": {"kind": "keyword", "default": "main"},
            "paths": {"main": {"brain": "rule", "prompt": "You chat."}},
        },
        "tools": [
            {"id": "nudge", "kind": "constant", "value": "nudged"},
            {"id": "escalate", "kind": "constant", "value": "raised"},
        ],
    }


def _turn(cid: str, n: int, **metadata) -> Event:
    return Event.turn(cid, f"t{n}", f"text {n}", metadata={k: str(v) for k, v in metadata.items()})


def _types(result) -> list:
    return [e["type"] for e in result["events"]]


def _calls(result):
    return [{"tool": c["tool"], "index": c["index"], "args": c["args"]} for c in result["tool_calls"]]


@pytest.fixture
def manual():
    rt = FlinkRuntime(clock="manual", parallelism=2)
    yield rt
    rt.close()


def test_unknown_clock_is_rejected():
    with pytest.raises(ValueError):
        FlinkRuntime(clock="quartz")


def test_default_clock_is_wall_time_and_cannot_be_advanced():
    rt = FlinkRuntime()
    try:
        rt.deploy(_workflow([]))
        assert rt.clock_kind == "system"
        with pytest.raises(RuntimeError, match="clock='manual'"):
            rt.advance_time(random.randrange(1, 1000))
        with pytest.raises(RuntimeError, match="clock='manual'"):
            rt.now_ms()
    finally:
        rt.close()


def test_manual_clock_starts_at_zero_and_never_moves_backwards(manual: FlinkRuntime):
    manual.deploy(_workflow([]))
    assert isinstance(manual, TimeAdvancing)
    assert manual.now_ms() == 0
    steps = [random.randrange(0, 5000) for _ in range(random.randrange(2, 6))]
    total = 0
    for step in steps:
        total += step
        assert manual.advance_time(step) == total
    assert manual.now_ms() == total
    with pytest.raises(ValueError):
        manual.advance_time(-1)
    assert manual.now_ms() == total


def test_processing_timer_fires_once_before_the_turn_that_observed_its_deadline(manual: FlinkRuntime):
    after = random.randrange(500, 5000)
    payload = {"channel": random.choice(["email", "sms", "push"])}
    manual.deploy(_workflow([{"id": "followup", "after_ms": after, "tool": "nudge", "payload": payload}]))
    cid = f"c{random.randrange(1 << 20)}"

    first = manual.submit(_turn(cid, 1))
    assert first["tool_calls"] == []
    scheduled = [e for e in first["events"] if e["type"] == "timer_scheduled"]
    assert len(scheduled) == 1 and int(scheduled[0]["payload"]["due_ms"]) == after

    manual.advance_time(after - 1)
    early = manual.submit(_turn(cid, 2))
    assert early["tool_calls"] == []
    assert "timer_fired" not in _types(early)

    manual.advance_time(1)
    due = manual.submit(_turn(cid, 3))
    assert _calls(due) == [{"tool": "nudge", "index": 0, "args": payload}]
    assert due["state"]["fired_timers"] == ["followup"]
    types = _types(due)
    assert types.index("timer_fired") < types.index("tool_called") < types.index("turn_received")

    manual.advance_time(random.randrange(1, 100_000))
    later = manual.submit(_turn(cid, 4))
    assert later["tool_calls"] == []
    assert later["state"]["fired_timers"] == ["followup"]
    assert "timer_fired" not in _types(later)


def test_every_parallel_instance_reads_the_same_clock(manual: FlinkRuntime):
    after = random.randrange(500, 5000)
    manual.deploy(_workflow([{"id": "followup", "after_ms": after, "tool": "nudge", "payload": {"n": 1}}]))
    cids = [f"c{random.randrange(1 << 20)}-{i}" for i in range(random.randrange(4, 9))]
    for cid in cids:
        assert manual.submit(_turn(cid, 1))["tool_calls"] == []
    manual.advance_time(after + random.randrange(0, 100))
    for cid in cids:
        result = manual.submit(_turn(cid, 2))
        assert _calls(result) == [{"tool": "nudge", "index": 0, "args": {"n": 1}}], cid
        assert result["state"]["fired_timers"] == ["followup"]


def test_event_time_timer_reads_the_watermark_and_late_turns_do_not_move_it_back(manual: FlinkRuntime):
    after = random.randrange(1_000, 60_000)
    base = random.randrange(1_000_000, 2_000_000_000)
    manual.deploy(_workflow([{"id": "sla", "after_ms": after, "clock": "event", "tool": "escalate",
                              "payload": {"reason": "sla"}}]))
    cid = f"c{random.randrange(1 << 20)}"

    first = manual.submit(_turn(cid, 1, event_time_ms=base))
    scheduled = next(e for e in first["events"] if e["type"] == "timer_scheduled")["payload"]
    assert scheduled["clock"] == "event" and int(scheduled["due_ms"]) == base + after
    assert first["state"]["watermark_ms"] == base

    # A large processing-time jump must not fire an event-time timer.
    manual.advance_time(after * 10)
    not_yet = manual.submit(_turn(cid, 2, event_time_ms=base + after // 2))
    assert not_yet["tool_calls"] == [] and not_yet["state"]["watermark_ms"] == base + after // 2

    # A late turn (older event time) is processed in arrival order and leaves the watermark alone.
    late = manual.submit(_turn(cid, 3, event_time_ms=base - random.randrange(1, 1000)))
    assert late["status"] == "completed"
    assert late["state"]["watermark_ms"] == base + after // 2
    assert late["state"]["turn_count"] == 3
    assert late["tool_calls"] == []

    fired = manual.submit(_turn(cid, 4, event_time_ms=base + after))
    assert _calls(fired) == [{"tool": "escalate", "index": 0, "args": {"reason": "sla"}}]
    assert fired["state"]["fired_timers"] == ["sla"]
    assert fired["state"]["watermark_ms"] == base + after


def test_timer_scheduled_before_restart_fires_once_after_it(manual: FlinkRuntime):
    after = random.randrange(1_000, 20_000)
    manual.deploy(_workflow([{"id": "followup", "after_ms": after, "tool": "nudge", "payload": {"n": 1}}]))
    cid = f"c{random.randrange(1 << 20)}"
    manual.submit(_turn(cid, 1))
    before_restart = random.randrange(1, after)
    manual.advance_time(before_restart)
    second = manual.submit(_turn(cid, 2))
    assert "timer_fired" not in _types(second)

    manual.restart()
    assert manual.now_ms() == before_restart, "the manual clock keeps its reading across stop-with-savepoint"

    manual.advance_time(after - before_restart)
    fired = manual.submit(_turn(cid, 3))
    assert _calls(fired) == [{"tool": "nudge", "index": 0, "args": {"n": 1}}]
    assert fired["state"]["fired_timers"] == ["followup"]
    assert "timer_scheduled" not in _types(fired), "restart must not re-schedule"

    manual.restart()
    manual.advance_time(random.randrange(1, 10_000))
    again = manual.submit(_turn(cid, 4))
    assert again["tool_calls"] == []
    assert again["state"]["fired_timers"] == ["followup"]
    assert "timer_fired" not in _types(again)

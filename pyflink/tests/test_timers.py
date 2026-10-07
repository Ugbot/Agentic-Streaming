"""Workflow timers on the ``pyflink`` binding: the manual processing clock (``clock="manual"``),
the event-time watermark folded from ``metadata.event_time_ms`` and stop-with-savepoint restart,
driven from Python (spec/v1/primitives.md section 8).

Deadlines and event times are randomized; every assertion is derived from the drawn values.
Each streaming job here runs on the local MiniCluster, so a test takes a few seconds.
"""

from __future__ import annotations

import random

import pytest

from agentic_pyflink import FlinkConfig, FlinkRuntime, RuntimeStateError

STREAMING = FlinkConfig(mode="local", checkpoint_interval="200ms")


def _workflow(timers: list[dict]) -> dict:
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


def _turn(cid: str, n: int, **metadata) -> dict:
    event = {"conversation_id": cid, "turn_id": f"t{n}", "text": f"text {n}"}
    if metadata:
        event["metadata"] = {k: str(v) for k, v in metadata.items()}
    return event


def _types(result: dict) -> list[str]:
    return [e["type"] for e in result["events"]]


def _calls(result: dict) -> list[dict]:
    return [{"tool": c["tool"], "index": c["index"], "args": c["args"]} for c in result["tool_calls"]]


def test_unknown_clock_is_rejected() -> None:
    with pytest.raises(ValueError, match="clock"):
        FlinkRuntime(clock="quartz")


def test_system_clock_runtime_cannot_be_advanced(jars) -> None:
    with FlinkRuntime() as rt:
        assert rt.clock_kind == "system"
        with pytest.raises(RuntimeStateError, match="clock='manual'"):
            rt.advance_time(random.randrange(1, 1000))
        with pytest.raises(RuntimeStateError, match="clock='manual'"):
            rt.now_ms()


def test_manual_clock_starts_at_zero_and_never_moves_backwards(jars) -> None:
    with FlinkRuntime(clock="manual") as rt:
        assert rt.now_ms() == 0
        steps = [random.randrange(0, 5000) for _ in range(random.randrange(2, 6))]
        total = 0
        for step in steps:
            total += step
            assert rt.advance_time(step) == total
        assert rt.now_ms() == total
        with pytest.raises(ValueError):
            rt.advance_time(-1)
        assert rt.now_ms() == total


def test_processing_timer_fires_once_before_the_turn_that_observed_its_deadline(jars, rand_id) -> None:
    after = random.randrange(500, 5000)
    payload = {"channel": random.choice(["email", "sms", "push"])}
    cid = rand_id("c")
    with FlinkRuntime(STREAMING, clock="manual") as rt:
        rt.deploy(_workflow([{"id": "followup", "after_ms": after, "tool": "nudge", "payload": payload}]))

        first = rt.submit(_turn(cid, 1))
        assert first["tool_calls"] == []
        scheduled = [e for e in first["events"] if e["type"] == "timer_scheduled"]
        assert len(scheduled) == 1 and int(scheduled[0]["payload"]["due_ms"]) == after

        rt.advance_time(after - 1)
        early = rt.submit(_turn(cid, 2))
        assert early["tool_calls"] == []
        assert "timer_fired" not in _types(early)

        rt.advance_time(1)
        due = rt.submit(_turn(cid, 3))
        assert _calls(due) == [{"tool": "nudge", "index": 0, "args": payload}]
        assert due["state"]["fired_timers"] == ["followup"]
        types = _types(due)
        assert types.index("timer_fired") < types.index("tool_called") < types.index("turn_received")

        rt.advance_time(random.randrange(1, 100_000))
        later = rt.submit(_turn(cid, 4))
        assert later["tool_calls"] == []
        assert later["state"]["fired_timers"] == ["followup"]
        assert "timer_fired" not in _types(later)


def test_event_time_timer_reads_the_watermark_and_late_turns_do_not_move_it_back(jars, rand_id) -> None:
    after = random.randrange(1_000, 60_000)
    base = random.randrange(1_000_000, 2_000_000_000)
    cid = rand_id("c")
    with FlinkRuntime(STREAMING, clock="manual") as rt:
        rt.deploy(_workflow([{"id": "sla", "after_ms": after, "clock": "event", "tool": "escalate",
                              "payload": {"reason": "sla"}}]))

        first = rt.submit(_turn(cid, 1, event_time_ms=base))
        scheduled = next(e for e in first["events"] if e["type"] == "timer_scheduled")["payload"]
        assert scheduled["clock"] == "event" and int(scheduled["due_ms"]) == base + after
        assert first["state"]["watermark_ms"] == base

        rt.advance_time(after * 10)
        not_yet = rt.submit(_turn(cid, 2, event_time_ms=base + after // 2))
        assert not_yet["tool_calls"] == [] and not_yet["state"]["watermark_ms"] == base + after // 2

        late = rt.submit(_turn(cid, 3, event_time_ms=base - random.randrange(1, 1000)))
        assert late["status"] == "completed"
        assert late["state"]["watermark_ms"] == base + after // 2
        assert late["state"]["turn_count"] == 3
        assert late["tool_calls"] == []

        fired = rt.submit(_turn(cid, 4, event_time_ms=base + after))
        assert _calls(fired) == [{"tool": "escalate", "index": 0, "args": {"reason": "sla"}}]
        assert fired["state"]["fired_timers"] == ["sla"]
        assert fired["state"]["watermark_ms"] == base + after


def test_timer_scheduled_before_restart_fires_once_after_it(jars, rand_id) -> None:
    after = random.randrange(1_000, 20_000)
    cid = rand_id("c")
    with FlinkRuntime(STREAMING, clock="manual") as rt:
        rt.deploy(_workflow([{"id": "followup", "after_ms": after, "tool": "nudge", "payload": {"n": 1}}]))
        rt.submit(_turn(cid, 1))
        before_restart = random.randrange(1, after)
        rt.advance_time(before_restart)
        second = rt.submit(_turn(cid, 2))
        assert "timer_fired" not in _types(second)

        rt.restart()
        assert rt.now_ms() == before_restart, "the manual clock keeps its reading across stop-with-savepoint"

        rt.advance_time(after - before_restart)
        fired = rt.submit(_turn(cid, 3))
        assert _calls(fired) == [{"tool": "nudge", "index": 0, "args": {"n": 1}}]
        assert fired["state"]["fired_timers"] == ["followup"]
        assert "timer_scheduled" not in _types(fired), "restart must not re-schedule"

        rt.restart()
        rt.advance_time(random.randrange(1, 10_000))
        again = rt.submit(_turn(cid, 4))
        assert again["tool_calls"] == []
        assert again["state"]["fired_timers"] == ["followup"]
        assert "timer_fired" not in _types(again)

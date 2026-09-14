"""`context.compaction: window`: the fold retains the most recent `max_items` messages in log
order, `transcript_length` reports what is retained, the log and `turn_count` are untouched,
`none` retains everything, and conversations are windowed independently."""

from __future__ import annotations

import random
import uuid
from typing import Any, Dict, List

import pytest

from agentic import Turn, ValidationError
from agentic.events import ChatMessage, Event, InMemoryEventLog, reduce_state, retain_window, transcript, window_size
from agentic.runtime import LocalRuntime


def rid(prefix: str = "id") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def completed_turns(log: InMemoryEventLog, cid: str, turns: int) -> None:
    """Append `turns` completed turns: turn_received, memory_written (two messages), turn_completed."""
    for i in range(turns):
        tid = f"t{i}"
        log.append(cid, tid, "turn_received", {"turn_id": tid, "text": f"u{i}"}, 0)
        log.append(cid, tid, "memory_written", {"messages": [{"role": "user", "text": f"u{i}"},
                                                             {"role": "assistant", "text": f"a{i}"}]}, 0)
        log.append(cid, tid, "turn_completed", {"reply": f"a{i}"}, 0)


@pytest.mark.parametrize("seed", range(10))
def test_window_bounds_transcript_length_not_turn_count(seed: int) -> None:
    rng = random.Random(seed)
    max_items = rng.randint(1, 8)
    turns = rng.randint(1, 11)
    written = 2 * turns
    log = InMemoryEventLog()
    cid = rid("c")
    completed_turns(log, cid, turns)
    events = log.read(cid)
    window = {"max_items": max_items, "compaction": "window"}

    full = reduce_state(events)
    bounded = reduce_state(events, window)
    assert full["transcript_length"] == written
    assert bounded["transcript_length"] == min(written, max_items)
    assert full["turn_count"] == bounded["turn_count"] == turns
    assert len(events) == 3 * turns, "the log itself is never compacted"
    assert reduce_state(events, {"max_items": 1, "compaction": "none"}) == full
    assert reduce_state(events, {"max_tokens": 8, "compaction": "moscow"}) == full
    assert reduce_state(events, {}) == full

    kept = transcript(events, window)
    assert kept == transcript(events)[written - min(written, max_items):]
    assert len(kept) == bounded["transcript_length"]


@pytest.mark.parametrize("seed", range(10))
def test_retain_window_keeps_the_most_recent_messages_in_order(seed: int) -> None:
    rng = random.Random(seed)
    max_items = rng.randint(1, 6)
    n = rng.randint(max_items, max_items + 10)
    messages = [ChatMessage("user" if i % 2 == 0 else "assistant", f"m{i}") for i in range(n)]
    assert retain_window(messages, {"max_items": max_items, "compaction": "window"}) == messages[n - max_items:]
    assert retain_window(messages, None) == messages
    assert retain_window(messages, {"max_items": 1, "compaction": "none"}) == messages


def test_window_size_validates_the_context_block() -> None:
    assert window_size(None) is None
    assert window_size({}) is None
    assert window_size({"max_items": 3}) is None
    assert window_size({"max_items": 3, "compaction": "moscow"}) is None
    assert window_size({"max_items": 3, "compaction": "window"}) == 3
    for bad in ({"compaction": "window"}, {"compaction": "window", "max_items": 0},
                {"compaction": "window", "max_items": True}, {"compaction": "window", "max_items": "4"}):
        with pytest.raises(ValidationError):
            window_size(bad)


def windowed_workflow(max_items: int) -> Dict[str, Any]:
    return {
        "spec_version": "agentic/v1",
        "backend": "local",
        "context": {"max_items": max_items, "compaction": "window"},
        "agent": {"id": rid("chat"),
                  "router": {"kind": "keyword", "default": "main"},
                  "paths": {"main": {"brain": "rule", "prompt": "You chat."}}},
    }


def test_running_workflow_reports_the_retained_window() -> None:
    max_items = random.randint(1, 6)
    turns = random.randint(1, 7)
    rt = LocalRuntime()
    rt.deploy(windowed_workflow(max_items))
    assert rt.capabilities()["context_window"] == "supported"
    cid, other = rid("c"), rid("c")
    results: List[Dict[str, Any]] = [rt.submit(Turn(cid, f"t{i}", f"hello {i}")) for i in range(1, turns + 1)]
    assert [r["status"] for r in results] == ["completed"] * turns
    assert [r["state"]["turn_count"] for r in results] == list(range(1, turns + 1))
    assert [r["state"]["transcript_length"] for r in results] == [min(2 * i, max_items) for i in range(1, turns + 1)]

    events = rt.log.read(cid)
    assert reduce_state(events)["transcript_length"] == 2 * turns, "the log keeps every message"
    assert rt.state(cid) == {"turn_count": turns, "transcript_length": min(2 * turns, max_items)}
    assert rt.engine.transcript(cid) == transcript(events)[2 * turns - min(2 * turns, max_items):]

    isolated = rt.submit(Turn(other, "x1", "other conversation"))
    assert isolated["state"] == {"turn_count": 1, "transcript_length": min(2, max_items)}

    fresh = rt.restart()
    try:
        assert fresh.state(cid) == {"turn_count": turns, "transcript_length": min(2 * turns, max_items)}
        again = fresh.submit(Turn(cid, f"t{turns}", f"hello {turns}"))
        assert again["status"] == "duplicate"
        assert again["state"] == results[-1]["state"], "a recovered result reads the retained window too"
    finally:
        fresh.close()

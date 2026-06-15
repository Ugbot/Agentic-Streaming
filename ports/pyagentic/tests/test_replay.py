"""Phase 5: replay / time-travel. The event log is the source of truth; replaying it
through a fresh runtime re-materializes the same outcomes (determinism), and
``replay_until`` stops early to inspect state as-of a point in the log. Mirrors the Java
``replay`` package goldens.
"""

from __future__ import annotations

from pyagentic import (
    Event,
    InMemoryEventLog,
    InMemoryHotVectorIndex,
    LocalRuntime,
    SeedChannel,
    StreamRuntime,
    TwoTierRetriever,
    replay,
    replay_until,
)
from pyagentic.banking import build_banking_graph, default_tools, seed_kb


def _banking() -> LocalRuntime:
    """A fresh banking runtime — same idiom as test_stream's ``_banking``. Use a fresh
    one per replay since submit mutates store state."""
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retriever = TwoTierRetriever(hot, None, 4, 4)
    return LocalRuntime(build_banking_graph(), tools=default_tools(), retriever=retriever)


def _events() -> list[Event]:
    return [
        Event("c1", "what is my balance?", user_id="alice"),
        Event("c2", "tell me about crypto cash-back", user_id="bob"),
        Event("c1", "hello there", user_id="alice"),
    ]


def test_recording_via_stream_observer():
    log = InMemoryEventLog()
    events = _events()
    StreamRuntime(_banking()).observe(log.record).run(SeedChannel(events))

    assert log.events() == events
    assert len(log.events_for("c1")) == 2
    assert len(log.events_for("c2")) == 1
    assert log.events_for("c1") == [events[0], events[2]]


def test_replay_reproduces_paths():
    log = InMemoryEventLog()
    StreamRuntime(_banking()).observe(log.record).run(SeedChannel(_events()))

    results = replay(log.events(), _banking())
    assert [r.path for r in results] == ["payments", "cards", "general"]


def test_replay_until_is_time_travel():
    log = InMemoryEventLog()
    StreamRuntime(_banking()).observe(log.record).run(SeedChannel(_events()))

    results = replay_until(log.events(), 2, _banking())
    assert len(results) == 2
    assert [r.path for r in results] == ["payments", "cards"]


def test_replay_until_clamps_bounds():
    log = InMemoryEventLog()
    StreamRuntime(_banking()).observe(log.record).run(SeedChannel(_events()))
    events = log.events()

    assert replay_until(events, 0, _banking()) == []
    assert replay_until(events, -5, _banking()) == []
    assert len(replay_until(events, 99, _banking())) == len(events)

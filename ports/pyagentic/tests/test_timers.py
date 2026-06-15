"""Phase 2: portable timers — logical-time advance, cancel, durable restore, and firing
through the stream runtime as turns. Mirrors the Java ``TimerServiceTest`` goldens.
"""

from __future__ import annotations

from pyagentic import (
    DurableTimerService,
    Event,
    InMemoryHotVectorIndex,
    InMemoryKeyedStateStore,
    InMemoryTimerService,
    LocalRuntime,
    StreamRuntime,
    TwoTierRetriever,
)
from pyagentic.banking import build_banking_graph, default_tools, seed_kb


def _ev(id: str) -> Event:
    # Event(conversation_id, text, user_id=...) — text before user_id.
    return Event("c1", id, user_id="u")


def _banking() -> LocalRuntime:
    """A fresh banking runtime — same idiom as test_stream's ``_banking``."""
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retriever = TwoTierRetriever(hot, None, 4, 4)
    return LocalRuntime(build_banking_graph(), tools=default_tools(), retriever=retriever)


def test_advance_fires_due_timers_ascending_by_deadline():
    t = InMemoryTimerService()
    t.schedule("a", 100, _ev("a"))
    t.schedule("b", 50, _ev("b"))
    t.schedule("c", 200, _ev("c"))

    assert t.next_deadline() == 50
    due = t.advance_to(150)
    assert [timer.id for timer in due] == ["b", "a"], "ascending by fire_at"
    assert t.next_deadline() == 200, "only c remains"
    assert t.advance_to(150) == [], "nothing new due"


def test_equal_deadlines_keep_schedule_order():
    t = InMemoryTimerService()
    t.schedule("x", 100, _ev("x"))
    t.schedule("y", 100, _ev("y"))
    t.schedule("z", 100, _ev("z"))
    assert [timer.id for timer in t.advance_to(100)] == ["x", "y", "z"]


def test_cancel_removes_pending_timer():
    t = InMemoryTimerService()
    t.schedule("a", 100, _ev("a"))
    assert t.cancel("a") is True
    assert t.cancel("a") is False
    assert t.advance_to(1000) == []


def test_durable_timers_survive_restore():
    store = InMemoryKeyedStateStore()
    first = DurableTimerService(store)
    first.schedule(
        "escalate", 500, Event(conversation_id="c9", text="what is my balance?", user_id="alice")
    )

    # "restart": a brand-new service over the same store, then restore.
    recovered = DurableTimerService(store)
    recovered.restore()
    assert recovered.next_deadline() == 500
    due = recovered.advance_to(500)
    assert len(due) == 1
    assert due[0].payload.conversation_id == "c9"
    assert due[0].payload.text == "what is my balance?"


def test_timers_fire_through_the_stream_runtime_as_turns():
    stream = StreamRuntime(_banking())
    timers = InMemoryTimerService()
    timers.schedule(
        "followup", 1000, Event(conversation_id="c1", text="what is my balance?", user_id="alice")
    )

    assert stream.fire_due_timers(timers, 999) == [], "not due yet"
    fired = stream.fire_due_timers(timers, 1000)
    assert len(fired) == 1
    assert fired[0].path == "payments"
    assert "1234.56" in fired[0].reply


def test_reschedule_same_id_replaces_and_takes_new_order():
    # Re-scheduling an id replaces the timer and moves it to the new schedule position.
    t = InMemoryTimerService()
    t.schedule("a", 100, _ev("a"))
    t.schedule("b", 100, _ev("b"))
    t.schedule("a", 100, _ev("a"))  # a re-inserted last
    assert [timer.id for timer in t.advance_to(100)] == ["b", "a"]


def test_observers_see_fired_timer_payloads():
    seen: list[str] = []
    stream = StreamRuntime(_banking()).observe(lambda e: seen.append(e.conversation_id))
    timers = InMemoryTimerService()
    timers.schedule(
        "f", 10, Event(conversation_id="c1", text="what is my balance?", user_id="alice")
    )
    stream.fire_due_timers(timers, 10)
    assert seen == ["c1"]

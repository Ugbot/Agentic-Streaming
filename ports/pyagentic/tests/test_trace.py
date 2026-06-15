"""Phase 7: observability — the tracing SPI and StreamRuntime instrumentation.

Mirrors the Java tracing goldens: a RecordingTracer captures chained attrs/events,
the StreamRuntime traces a span per turn and per timer fire (carrying conversation,
path, ok, and one ``tool:<id>`` event per tool call), and the no-op default keeps
behavior identical when no tracer is set.
"""

from __future__ import annotations

from pyagentic import (
    Event,
    InMemoryHotVectorIndex,
    InMemoryTimerService,
    LocalRuntime,
    RecordingTracer,
    SeedChannel,
    StreamRuntime,
    TwoTierRetriever,
)
from pyagentic.banking import build_banking_graph, default_tools, seed_kb


def _banking() -> LocalRuntime:
    """A fresh banking runtime — same idiom as test_stream's ``_banking``."""
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retriever = TwoTierRetriever(hot, None, 4, 4)
    return LocalRuntime(build_banking_graph(), tools=default_tools(), retriever=retriever)


def test_recording_tracer_captures_chained_attrs_and_events():
    t = RecordingTracer()
    t.start("demo").attr("k", "v").event("hello").end()

    spans = t.spans()
    assert len(spans) == 1
    assert t.names() == ["demo"]
    span = spans[0]
    assert span.name == "demo"
    assert span.attrs["k"] == "v"
    assert span.events == ["hello"]


def test_stream_traces_turns():
    t = RecordingTracer()
    stream = StreamRuntime(_banking()).with_tracer(t)
    stream.run(SeedChannel([Event(conversation_id="c1", text="what is my balance?", user_id="alice")]))

    assert t.names() == ["turn"]
    span = t.spans()[0]
    assert span.attrs["conversation"] == "c1"
    assert span.attrs["path"] == "payments"
    assert span.attrs["ok"] == "true"
    assert "tool:get_balance" in span.events


def test_timer_fires_traced():
    t = RecordingTracer()
    stream = StreamRuntime(_banking()).with_tracer(t)
    timers = InMemoryTimerService()
    timers.schedule(
        "followup", 1000, Event(conversation_id="c1", text="what is my balance?", user_id="alice")
    )

    fired = stream.fire_due_timers(timers, 1000)
    assert len(fired) == 1
    assert t.names() == ["timer.fire"]
    span = t.spans()[0]
    assert span.attrs["path"] == "payments"
    assert span.attrs["conversation"] == "c1"


def test_noop_default_does_not_break_behavior():
    # No with_tracer: the no-op default must leave behavior identical.
    stream = StreamRuntime(_banking())
    results = stream.run(SeedChannel([Event(conversation_id="c1", text="hello there", user_id="alice")]))
    assert len(results) == 1
    assert results[0].path == "general"

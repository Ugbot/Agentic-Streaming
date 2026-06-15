"""Phase 1: the stream substrate. Driving a channel of events must be identical to
N* submit, and observers must see every event — the seam later phases (CEP/windows/
timers) build on. Mirrors the Java ``StreamRuntimeTest`` goldens.
"""

from __future__ import annotations

from pyagentic import (
    Event,
    InMemoryHotVectorIndex,
    LocalRuntime,
    QueueChannel,
    SeedChannel,
    StreamRuntime,
    TwoTierRetriever,
)
from pyagentic.banking import build_banking_graph, default_tools, seed_kb


def _banking() -> LocalRuntime:
    """A fresh banking runtime — same idiom as test_core's ``_runtime``."""
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retriever = TwoTierRetriever(hot, None, 4, 4)
    return LocalRuntime(build_banking_graph(), tools=default_tools(), retriever=retriever)


def _turns() -> list[Event]:
    # (conversation_id, user_id, text) — Event takes text positionally, user_id by name.
    return [
        Event("c1", "what is my balance?", user_id="alice"),
        Event("c2", "tell me about crypto cash-back", user_id="bob"),
        Event("c1", "hello there", user_id="alice"),
    ]


def test_streaming_equals_repeated_submit():
    # Reference: drive each event through submit individually.
    direct = _banking()
    via_submit = [f"{e.conversation_id}|{direct.submit(e).path}" for e in _turns()]

    # Streaming form: a SeedChannel through a StreamRuntime over a fresh runtime.
    stream = StreamRuntime(_banking())
    results = stream.run(SeedChannel(_turns()))
    via_stream = [
        f"{e.conversation_id}|{r.path}" for e, r in zip(_turns(), results)
    ]

    assert via_stream == via_submit, "streaming must match repeated submit, in order"
    assert via_stream == ["c1|payments", "c2|cards", "c1|general"]
    assert [r.path for r in results] == ["payments", "cards", "general"]


def test_observers_see_every_event_before_the_turn():
    seen: list[str] = []
    stream = StreamRuntime(_banking()).observe(lambda e: seen.append(e.conversation_id))
    stream.run(SeedChannel(_turns()))
    assert seen == ["c1", "c2", "c1"]


def test_queue_channel_drains_in_fifo_order():
    queue = (
        QueueChannel()
        .offer(Event("c1", "what is my balance?", user_id="alice"))
        .offer(Event("c1", "tell me about crypto cash-back", user_id="alice"))
    )
    results = StreamRuntime(_banking()).run(queue)
    assert len(results) == 2
    assert [r.path for r in results] == ["payments", "cards"]
    assert "1234.56" in results[0].reply


def test_seed_channel_of_constructor_and_then_empty():
    channel = SeedChannel.of(
        Event("c1", "what is my balance?", user_id="alice"),
    )
    assert channel.poll() is not None
    # Bounded: empty forever once drained.
    assert channel.poll() is None
    assert channel.poll() is None


def test_stream_runtime_stops_when_channel_empties_and_resumes():
    # An unbounded channel can be drained, refilled, and drained again — the stream
    # loop returns when poll() reports empty, then picks up newly-offered events.
    rt = _banking()
    stream = StreamRuntime(rt)
    queue: QueueChannel = QueueChannel()
    assert stream.run(queue) == []  # empty now -> no turns
    queue.offer(Event("c3", "card help", user_id="dave"))
    again = stream.run(queue)
    assert [r.path for r in again] == ["cards"]

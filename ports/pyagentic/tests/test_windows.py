"""Behavioral-parity tests for pyagentic.windows — mirrors the Java goldens."""

from __future__ import annotations

from pyagentic.windows import (
    Bucket,
    Session,
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
    WindowState,
)


def test_sliding_count_eviction_and_independence():
    w = SlidingWindow(60000)
    state = None
    for ts in (1000, 2000, 3000, 4000, 5000):
        state = w.add("h1", ts)
    assert state == WindowState(count=5, sum=5.0)

    # A far-future event evicts everything older than ts-window.
    assert w.add("h1", 200000).count == 1

    # Independent per key.
    assert w.add("h2", 1000).count == 1


def test_sliding_sum():
    w = SlidingWindow(10000)
    w.add("acct", 0, 100.0)
    state = w.add("acct", 5000, 250.0)
    assert state.count == 2
    assert state.sum == 350.0


def test_tumbling_emits_on_bucket_advance_and_close_flushes():
    w = TumblingWindow(1000)
    assert w.add("k", 100) is None
    assert w.add("k", 200) is None
    emitted = w.add("k", 1500)
    assert emitted == Bucket(key="k", start=0, count=2, sum=2.0)
    flushed = w.close("k")
    assert flushed == Bucket(key="k", start=1000, count=1, sum=1.0)


def test_session_gap_close_and_double_close():
    w = SessionWindow(5000)
    assert w.add("u", 0) is None
    assert w.add("u", 2000) is None
    emitted = w.add("u", 10000)
    assert emitted == Session(key="u", start=0, end=2000, count=2, sum=2.0)

    flushed = w.close("u")
    assert flushed is not None
    assert flushed.count == 1
    # No open session remains.
    assert w.close("u") is None

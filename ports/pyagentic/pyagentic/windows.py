"""Portable keyed windows — the Python counterpart of the Java ``org.jagentic.core.windows``.

Keyed, per-key time-window aggregates with no engine dependency. These mirror Flink's
windowing (and a Pekko/Temporal hand-rolled equivalent) at behavioural parity:

* :class:`SlidingWindow` — the portable ``VelocityDetector``: "how many events (and what
  summed value) for this key in the last ``window_millis``". Each :meth:`SlidingWindow.add`
  evicts events at or before ``ts - window_millis`` and returns the current
  :class:`WindowState`, so a caller fires when ``count >= threshold``.
* :class:`TumblingWindow` — fixed, non-overlapping buckets ``floor(ts / window_millis)``;
  when an event arrives in a later bucket the previous one closes and is emitted.
* :class:`SessionWindow` — groups events into sessions separated by an inactivity
  ``gap_millis``.

Each is independent per key. The Java uses ``synchronized``; here an :class:`RLock`
guards mutation for parity with the other thread-safe pyagentic modules.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from threading import RLock
from typing import Deque, Dict, Optional, Tuple


@dataclass(frozen=True)
class WindowState:
    """A numeric aggregate over a set of events: how many, and their summed value."""

    count: int
    sum: float


@dataclass(frozen=True)
class Bucket:
    """A closed tumbling-window bucket. ``start = bucket_index * window_millis``."""

    key: str
    start: int
    count: int
    sum: float


@dataclass(frozen=True)
class Session:
    """A closed session: ``start``/``end`` are the first/last event timestamps."""

    key: str
    start: int
    end: int
    count: int
    sum: float


class SlidingWindow:
    """Keyed sliding time-window aggregate (the ``VelocityDetector``)."""

    def __init__(self, window_millis: int) -> None:
        self._window_millis = window_millis
        self._by_key: Dict[str, Deque[Tuple[int, float]]] = {}
        self._lock = RLock()

    def add(self, key: str, ts: int, value: float = 1.0) -> WindowState:
        """Record an event for ``key`` at ``ts``; return count + sum within (ts-window, ts]."""
        with self._lock:
            q = self._by_key.get(key)
            if q is None:
                q = deque()
                self._by_key[key] = q
            q.append((ts, value))
            cutoff = ts - self._window_millis
            while q and q[0][0] <= cutoff:
                q.popleft()
            count = len(q)
            total = 0.0
            for _, v in q:
                total += v
            return WindowState(count, total)


class TumblingWindow:
    """Keyed tumbling (fixed, non-overlapping) time-window aggregate."""

    def __init__(self, window_millis: int) -> None:
        self._window_millis = window_millis
        # key -> [index, count, sum]
        self._by_key: Dict[str, list] = {}
        self._lock = RLock()

    def add(self, key: str, ts: int, value: float = 1.0) -> Optional[Bucket]:
        """Add an event; if it falls in a later bucket than the open one, emit (and start) — else None."""
        with self._lock:
            index = math.floor(ts / self._window_millis)
            open_ = self._by_key.get(key)
            emitted: Optional[Bucket] = None
            if open_ is None:
                open_ = [index, 0, 0.0]
                self._by_key[key] = open_
            elif index > open_[0]:
                emitted = Bucket(key, open_[0] * self._window_millis, open_[1], open_[2])
                open_[0] = index
                open_[1] = 0
                open_[2] = 0.0
            open_[1] += 1
            open_[2] += value
            return emitted

    def close(self, key: str) -> Optional[Bucket]:
        """Flush the currently-open bucket for a key (returns None if none open)."""
        with self._lock:
            open_ = self._by_key.pop(key, None)
            if open_ is None:
                return None
            return Bucket(key, open_[0] * self._window_millis, open_[1], open_[2])


class SessionWindow:
    """Keyed session window — groups events into sessions separated by ``gap_millis``."""

    def __init__(self, gap_millis: int) -> None:
        self._gap_millis = gap_millis
        # key -> [start, last, count, sum]
        self._by_key: Dict[str, list] = {}
        self._lock = RLock()

    def add(self, key: str, ts: int, value: float = 1.0) -> Optional[Session]:
        with self._lock:
            open_ = self._by_key.get(key)
            emitted: Optional[Session] = None
            if open_ is not None and ts - open_[1] > self._gap_millis:
                emitted = Session(key, open_[0], open_[1], open_[2], open_[3])
                open_ = None
            if open_ is None:
                open_ = [ts, ts, 0, 0.0]
                self._by_key[key] = open_
            open_[1] = ts
            open_[2] += 1
            open_[3] += value
            return emitted

    def close(self, key: str) -> Optional[Session]:
        with self._lock:
            open_ = self._by_key.pop(key, None)
            if open_ is None:
                return None
            return Session(key, open_[0], open_[1], open_[2], open_[3])

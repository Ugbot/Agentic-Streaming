"""Portable timers — the Python counterpart of the Java ``org.jagentic.core.timers``.

The portable analogue of Flink's ``TimerService`` / a Pekko scheduler / a Temporal
timer. Time is **logical**: callers advance the clock with :meth:`TimerService.advance_to`
and get back the due timers (so tests are deterministic); a real-time driver simply calls
``advance_to(now)`` on a tick. Powers SLAs, escalate-after-N, retries, scheduled
follow-ups, and CEP ``within`` expiry. Engine adapters may replace this entirely with a
native timer service.

``DurableTimerService`` persists its pending set through a :class:`KeyedStateStore`, so
timers survive a restart. The serialization is per-process self-consistent (it is never
read across languages), so each core port may encode it idiomatically — here JSON; the
behaviour ("a scheduled timer survives restore and fires") is what stays at parity.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from typing import Dict, List, Optional, Protocol, runtime_checkable

from .core import Event
from .memory import KeyedStateStore


@dataclass(frozen=True)
class Timer:
    """A scheduled event: when logical/processing time reaches ``fire_at``, ``payload``
    is fired back into the runtime as a turn. ``id`` is unique (re-scheduling the same
    id replaces)."""

    id: str
    fire_at: int
    payload: Event


@runtime_checkable
class TimerService(Protocol):
    """Portable timers. Mirrors ``org.jagentic.core.timers.TimerService``."""

    def schedule(self, id: str, fire_at: int, payload: Event) -> None:
        """Schedule (or replace, by id) a timer to fire ``payload`` at ``fire_at``."""
        ...

    def cancel(self, id: str) -> bool:
        """Cancel a pending timer; returns True if one was removed."""
        ...

    def advance_to(self, now: int) -> List[Timer]:
        """Remove and return all timers due at ``now`` (fire_at <= now), ascending by
        fire_at then schedule order."""
        ...

    def next_deadline(self) -> Optional[int]:
        """The earliest pending deadline, or None if no timers are pending."""
        ...


class InMemoryTimerService:
    """Process-local timers backed by an insertion-ordered dict; due timers come out
    ascending by fire_at, with schedule order as the stable tie-break."""

    def __init__(self) -> None:
        # Python dicts preserve insertion order; OrderedDict makes the intent explicit
        # and gives move_to_end semantics for the replace-keeps-new-order rule.
        self._timers: "OrderedDict[str, Timer]" = OrderedDict()
        self._lock = RLock()

    def schedule(self, id: str, fire_at: int, payload: Event) -> None:
        with self._lock:
            # Re-insert so a replaced timer takes the new schedule order.
            self._timers.pop(id, None)
            self._timers[id] = Timer(id, fire_at, payload)

    def cancel(self, id: str) -> bool:
        with self._lock:
            return self._timers.pop(id, None) is not None

    def advance_to(self, now: int) -> List[Timer]:
        with self._lock:
            due = [t for t in self._timers.values() if t.fire_at <= now]
            # Stable sort -> equal fire_at keeps schedule order (the dict iteration order).
            due.sort(key=lambda t: t.fire_at)
            for t in due:
                self._timers.pop(t.id, None)
            return due

    def next_deadline(self) -> Optional[int]:
        with self._lock:
            if not self._timers:
                return None
            return min(t.fire_at for t in self._timers.values())

    def pending(self) -> List[Timer]:
        """Snapshot of pending timers in schedule order (for durable persistence)."""
        with self._lock:
            return list(self._timers.values())

    def restore_all(self, restored: List[Timer]) -> None:
        with self._lock:
            for t in restored:
                self._timers[t.id] = t


class DurableTimerService:
    """A :class:`TimerService` that persists its pending set through a
    :class:`KeyedStateStore`, so timers survive a restart (with a durable store
    backing). Pending timers are written to one scalar slot; :meth:`restore` reloads
    them. Schedule/cancel/advance keep the slot current."""

    def __init__(
        self,
        store: KeyedStateStore,
        slot_key: str = "__timers__",
        slot_name: str = "pending",
    ) -> None:
        self._delegate = InMemoryTimerService()
        self._store = store
        self._slot_key = slot_key
        self._slot_name = slot_name
        self._lock = RLock()

    def schedule(self, id: str, fire_at: int, payload: Event) -> None:
        with self._lock:
            self._delegate.schedule(id, fire_at, payload)
            self._persist()

    def cancel(self, id: str) -> bool:
        with self._lock:
            removed = self._delegate.cancel(id)
            if removed:
                self._persist()
            return removed

    def advance_to(self, now: int) -> List[Timer]:
        with self._lock:
            due = self._delegate.advance_to(now)
            if due:
                self._persist()
            return due

    def next_deadline(self) -> Optional[int]:
        with self._lock:
            return self._delegate.next_deadline()

    def restore(self) -> None:
        """Reload pending timers from the store into this service (call after a restart)."""
        with self._lock:
            raw = self._store.get(self._slot_key, self._slot_name)
            if raw is None:
                return
            self._delegate.restore_all(_decode(str(raw)))

    def pending(self) -> List[Timer]:
        with self._lock:
            return self._delegate.pending()

    def _persist(self) -> None:
        self._store.put(self._slot_key, self._slot_name, _encode(self._delegate.pending()))


# ---- serialization: JSON list of {id, fire_at, conversation_id, user_id, text} ----


def _encode(timers: List[Timer]) -> str:
    return json.dumps(
        [
            {
                "id": t.id,
                "fire_at": t.fire_at,
                "conversation_id": t.payload.conversation_id,
                "user_id": t.payload.user_id,
                "text": t.payload.text,
            }
            for t in timers
        ]
    )


def _decode(s: str) -> List[Timer]:
    out: List[Timer] = []
    for rec in json.loads(s):
        event = Event(
            conversation_id=rec["conversation_id"],
            text=rec["text"],
            user_id=rec.get("user_id", "anonymous"),
        )
        out.append(Timer(rec["id"], int(rec["fire_at"]), event))
    return out

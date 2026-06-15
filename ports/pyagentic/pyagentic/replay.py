"""Replay / time-travel — the append-only event log + re-materialization API, the
portable mirror of the Java ``replay`` package (``EventLog`` + ``Replayer``).

An agent's state is a materialized view over an ordered log of inbound events. The
:class:`EventLog` is that log — the source of truth. Recording is free via the Phase-1
``StreamRuntime`` observer seam (``StreamRuntime(rt).observe(log.record).run(channel)``);
:func:`replay` re-materializes state by submitting the recorded events through a runtime.

Replaying through a *fresh* runtime over the same graph reproduces the outcomes
(determinism); replaying through a runtime built on a *new* graph version answers "what
would the new prompts/routing have done"; :func:`replay_until` stops early to inspect the
state as-of a point in the log — time-travel, the portable form of Datomic ``as-of`` / a
checkpoint restore.
"""

from __future__ import annotations

from threading import Lock
from typing import Dict, List, Protocol

from .core import Event, TurnResult
from .runtime import Runtime


class EventLog(Protocol):
    """The append-only log of inbound events. Recording is free via the
    ``StreamRuntime`` observer seam (``stream.observe(log.record)``)."""

    def record(self, event: Event) -> None: ...

    def events(self) -> List[Event]:
        """All recorded events, in arrival order."""
        ...

    def events_for(self, conversation_id: str) -> List[Event]:
        """Recorded events for one conversation, in arrival order."""
        ...


class InMemoryEventLog:
    """Process-local default. Thread-safe like the other in-memory stores."""

    def __init__(self) -> None:
        self._all: List[Event] = []
        self._by_key: Dict[str, List[Event]] = {}
        self._guard = Lock()

    def record(self, event: Event) -> None:
        with self._guard:
            self._all.append(event)
            self._by_key.setdefault(event.conversation_id, []).append(event)

    def events(self) -> List[Event]:
        with self._guard:
            return list(self._all)

    def events_for(self, conversation_id: str) -> List[Event]:
        with self._guard:
            return list(self._by_key.get(conversation_id, []))


def replay(events: List[Event], runtime: Runtime) -> List[TurnResult]:
    """Submit each event to ``runtime`` in order; return the turn results."""
    return [runtime.submit(event) for event in events]


def replay_until(events: List[Event], count: int, runtime: Runtime) -> List[TurnResult]:
    """Replay only the first ``count`` events (clamped to ``[0, len]``) — state
    as-of that point in the log."""
    n = max(0, min(count, len(events)))
    return replay(events[:n], runtime)

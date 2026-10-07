"""Workflow `timers` (spec/v1/primitives.md, section 8) and the two clocks they read.

Processing time is a logical clock the runtime owns: `wall_clock_ms` reads the system clock,
`ManualClock` moves only when told to (`advance`) and never backwards, which is how a
conformance fixture's `advance_time_ms` drives it. Event time is the conversation watermark,
the highest `event_time_ms` turn metadata seen, folded from the log.

Pending timers are never held in memory across turns: `TimerState.fold` derives them from
`timer_scheduled` minus `timer_fired`, so a restarted runtime recovers exactly the timers
still owed and can neither re-schedule nor double-fire one. Every `turn_received` of a
workflow with timers records the processing clock reading (`processing_time_ms`), so the
clock itself is recoverable from the log (`ManualClock.recovered_from`).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from .cep import event_time_ms
from .errors import ValidationError
from .events import Event, EventLog, Turn

PROCESSING = "processing"
EVENT = "event"


class ManualClock:
    """Logical processing time in milliseconds; callable like any `Clock`."""

    def __init__(self, start_ms: int = 0) -> None:
        if start_ms < 0:
            raise ValidationError(f"logical time cannot be negative: {start_ms}")
        self._now = int(start_ms)
        self._guard = threading.Lock()

    def __call__(self) -> int:
        with self._guard:
            return self._now

    def advance(self, ms: int) -> int:
        if ms < 0:
            raise ValidationError(f"logical time never moves backwards: advance by {ms}")
        with self._guard:
            self._now += int(ms)
            return self._now

    @classmethod
    def recovered_from(cls, log: EventLog) -> "ManualClock":
        """The clock a restarted runtime starts from: the highest processing time any
        conversation's `turn_received` recorded, or zero when none did."""
        recorded = 0
        for conversation_id in log.conversation_ids():
            state = TimerState.fold(log.read(conversation_id))
            if state.processing_time_ms is not None:
                recorded = max(recorded, state.processing_time_ms)
        return cls(recorded)


def event_time_of(turn: Turn) -> Optional[int]:
    """`metadata.event_time_ms` as an int, None when the turn has none: the same single read the CEP
    fold uses (:func:`agentic.cep.event_time_ms`), so timers and patterns agree on event time."""
    return event_time_ms(turn.metadata)


@dataclass(frozen=True)
class PendingTimer:
    timer_id: str
    clock: str
    due_ms: int


@dataclass
class TimerState:
    """What the log says about a conversation's clocks and timers."""

    watermark_ms: Optional[int] = None
    processing_time_ms: Optional[int] = None
    pending: Dict[str, PendingTimer] = field(default_factory=dict)
    fired: List[str] = field(default_factory=list)

    @classmethod
    def fold(cls, events: Iterable[Event]) -> "TimerState":
        state = cls()
        for event in events:
            payload = event.payload
            if event.type == "turn_received":
                if payload.get("event_time_ms") is not None:
                    t = int(payload["event_time_ms"])
                    state.watermark_ms = t if state.watermark_ms is None else max(state.watermark_ms, t)
                if payload.get("processing_time_ms") is not None:
                    state.processing_time_ms = int(payload["processing_time_ms"])
            elif event.type == "timer_scheduled":
                timer_id = str(payload["timer_id"])
                state.pending[timer_id] = PendingTimer(timer_id, str(payload.get("clock", PROCESSING)),
                                                      int(payload["due_ms"]))
            elif event.type == "timer_fired":
                timer_id = str(payload["timer_id"])
                if state.pending.pop(timer_id, None) is not None:
                    state.fired.append(timer_id)
        return state

    def watermark_after(self, event_time: Optional[int]) -> Optional[int]:
        """The watermark once a turn carrying `event_time` has arrived: never lower than before."""
        if event_time is None:
            return self.watermark_ms
        if self.watermark_ms is None:
            return event_time
        return max(self.watermark_ms, event_time)


def _reading(clock: str, processing_now: int, watermark: Optional[int]) -> int:
    if clock == EVENT:
        return watermark if watermark is not None else 0
    if clock == PROCESSING:
        return processing_now
    raise ValidationError(f"unknown timer clock {clock!r}; one of processing, event")


Invoke = Callable[[str, Mapping[str, Any]], Any]
Append = Callable[[str, Mapping[str, Any]], Any]


class WorkflowTimers:
    """The workflow's `timers` block, evaluated at the boundary of every turn.

    `fire_due` runs before a later turn's `turn_received` and fires every pending timer whose
    clock has reached its deadline, ordered by `(due_ms, timer_id)`, invoking the timer's tool
    with its declared payload. `schedule` runs after the first turn's `turn_received` and
    schedules every declared timer against the clock it names. Both are no-ops for a workflow
    without timers, which then also records no clock readings.
    """

    def __init__(self, specs: Sequence[Mapping[str, Any]], clock: Callable[[], int]) -> None:
        self.specs: List[Mapping[str, Any]] = list(specs or [])
        self.clock = clock
        for spec in self.specs:
            _reading(str(spec.get("clock", PROCESSING)), 0, 0)  # rejects an unknown clock at build time

    @property
    def active(self) -> bool:
        return bool(self.specs)

    def received_payload(self, turn: Turn, payload: Mapping[str, Any]) -> Dict[str, Any]:
        """`turn_received` payload with `event_time_ms` (when the turn carries one) and, for a
        workflow with timers, the processing clock reading."""
        out = dict(payload)
        event_time = event_time_of(turn)
        if event_time is not None:
            out["event_time_ms"] = event_time
        if self.active:
            out["processing_time_ms"] = self.clock()
        return out

    def fire_due(self, turn: Turn, prior: Sequence[Event], append: Append, invoke: Invoke) -> List[str]:
        """Fire the timers due at the head of this turn; returns their ids in firing order."""
        if not self.active or not prior:
            return []
        state = TimerState.fold(prior)
        now = self.clock()
        watermark = state.watermark_after(event_time_of(turn))
        due = sorted((t for t in state.pending.values() if _reading(t.clock, now, watermark) >= t.due_ms),
                     key=lambda t: (t.due_ms, t.timer_id))
        fired: List[str] = []
        for pending in due:
            append("timer_fired", {"timer_id": pending.timer_id, "due_ms": pending.due_ms})
            fired.append(pending.timer_id)
            spec = next((s for s in self.specs if s["id"] == pending.timer_id), None)
            if spec is not None and spec.get("tool"):
                invoke(str(spec["tool"]), dict(spec.get("payload") or {}))
        return fired

    def schedule(self, turn: Turn, prior: Sequence[Event], append: Append) -> List[PendingTimer]:
        """Schedule every declared timer on the conversation's first turn."""
        if not self.active or prior:
            return []
        state = TimerState.fold(prior)
        now = self.clock()
        watermark = state.watermark_after(event_time_of(turn))
        scheduled: List[PendingTimer] = []
        for spec in self.specs:
            clock = str(spec.get("clock", PROCESSING))
            timer = PendingTimer(str(spec["id"]), clock, _reading(clock, now, watermark) + int(spec["after_ms"]))
            append("timer_scheduled", {"timer_id": timer.timer_id, "clock": timer.clock, "due_ms": timer.due_ms})
            scheduled.append(timer)
        return scheduled

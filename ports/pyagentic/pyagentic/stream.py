"""The stream substrate — an agent as a materialized view over a stream of events.

The portable counterpart of a Flink source / Kafka consumer / Pekko Source. A
``Channel`` is a pull-based event source; the ``StreamRuntime`` drains it through a
backend ``Runtime`` as agent turns, so ``Runtime.submit(event)`` is just the one-shot
sugar and this is the streaming form. ``EventObserver`` callables see every event
before it becomes a turn — the seam CEP matchers, window aggregators, and tracers
plug into in later phases without the agent graph needing to know about them.

Per-conversation ordering stays the runtime's concern (the ``LocalRuntime`` per-key
lock, a Kafka partition, a Ray actor); the stream loop just preserves arrival order.
"""

from __future__ import annotations

from collections import deque
from threading import Lock
from typing import TYPE_CHECKING, Callable, Generic, Iterable, List, Optional, Protocol, TypeVar

from .core import Event, TurnResult
from .runtime import Runtime
from .trace import NoopTracer, Tracer

if TYPE_CHECKING:
    from .timers import TimerService

T = TypeVar("T")


class Channel(Protocol[T]):
    """A source of events. Pull-based: :meth:`poll` returns the next available event,
    or ``None`` when none is available right now. Never blocks. Bounded sources (a seed
    list) eventually return ``None`` forever; unbounded sources (a queue) may return
    ``None`` transiently and more later."""

    def poll(self) -> Optional[T]:
        """The next event if one is available, else ``None``. Never blocks."""
        ...


class SeedChannel(Generic[T]):
    """A bounded channel that replays a fixed list of events in order, then is empty
    (``None``) forever."""

    def __init__(self, events: Iterable[T]) -> None:
        self._it = iter(list(events))

    @classmethod
    def of(cls, *events: T) -> "SeedChannel[T]":
        return cls(events)

    def poll(self) -> Optional[T]:
        return next(self._it, None)


class QueueChannel(Generic[T]):
    """An unbounded in-memory channel: producers :meth:`offer` events; the runtime
    polls them in FIFO order. Thread-safe, so a producer thread and the stream loop can
    share it."""

    def __init__(self) -> None:
        self._queue: "deque[T]" = deque()
        self._guard = Lock()

    def offer(self, event: T) -> "QueueChannel[T]":
        with self._guard:
            self._queue.append(event)
        return self

    def poll(self) -> Optional[T]:
        with self._guard:
            return self._queue.popleft() if self._queue else None


# An observer sees every event the StreamRuntime drives, before it becomes a turn.
EventObserver = Callable[[Event], None]


class StreamRuntime:
    """Drives a :class:`Channel` of events through a backend :class:`Runtime` as agent
    turns — the realization of the thesis that an agent is a materialized view over a
    stream of events. ``Runtime.submit(event)`` is the one-shot sugar; this is the
    streaming form."""

    def __init__(self, runtime: Runtime) -> None:
        self._runtime = runtime
        self._observers: List[EventObserver] = []
        self._tracer: Tracer = NoopTracer()

    def observe(self, observer: EventObserver) -> "StreamRuntime":
        """Register an observer of the raw event stream (chainable)."""
        self._observers.append(observer)
        return self

    def with_tracer(self, tracer: Optional[Tracer]) -> "StreamRuntime":
        """Trace turns and timer fires through this tracer (chainable; default no-op)."""
        self._tracer = tracer if tracer is not None else NoopTracer()
        return self

    def _submit_traced(self, span_name: str, event: Event) -> TurnResult:
        """Notify observers, then submit ``event`` as a turn inside a trace span. The span
        carries the conversation id, chosen path, and ok flag (``"true"``/``"false"`` to
        mirror Java's ``Boolean.toString``), and one ``tool:<id>`` event per tool call."""
        for observer in self._observers:
            observer(event)
        span = self._tracer.start(span_name)
        span.attr("conversation", event.conversation_id)
        r = self._runtime.submit(event)
        span.attr("path", r.path).attr("ok", str(r.ok).lower())
        for tc in r.tool_calls:
            span.event("tool:" + tc)
        span.end()
        return r

    def run(self, channel: Channel[Event]) -> List[TurnResult]:
        """Drain every currently-available event from the channel as a turn, in arrival
        order, and return the results. Observers see each event first. Returns when the
        channel next reports empty (``poll() is None``)."""
        results: List[TurnResult] = []
        while True:
            event = channel.poll()
            if event is None:
                break
            results.append(self._submit_traced("turn", event))
        return results

    def fire_due_timers(self, timer_service: "TimerService", now: int) -> List[TurnResult]:
        """Advance ``timer_service`` to ``now`` and fire each due timer's payload through
        the backend runtime as a turn, in deadline order. Observers see each payload first,
        exactly as with a channel event — a fired timer is just another event entering the
        stream. Returns the results in deadline (then schedule) order."""
        results: List[TurnResult] = []
        for timer in timer_service.advance_to(now):
            results.append(self._submit_traced("timer.fire", timer.payload))
        return results

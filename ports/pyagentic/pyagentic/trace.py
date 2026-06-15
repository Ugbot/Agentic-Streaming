"""Phase 7: observability — a minimal, idiomatic tracing SPI.

A span per turn, timer fire, and CEP match. The :data:`NOOP` default costs nothing;
:class:`RecordingTracer` captures completed spans for tests and local inspection; an
OpenTelemetry exporter is an opt-in adapter (the heavy dependency stays out of the
core). Engine adapters can bridge to their native tracing behind the same SPI.

The portable counterpart of the Java ``org.jagentic.core.trace`` SPI — same shapes,
same semantics: ``attr``/``event`` are chainable, ``end()`` closes the span, and a
RecordingTracer records spans in ``end()`` order.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, List, Protocol


class Span(Protocol):
    """A single trace span — attach key/value attributes and named events, then
    :meth:`end`. ``attr`` and ``event`` return the span so they chain."""

    def attr(self, key: str, value: str) -> "Span":
        """Set an attribute; returns ``self`` for chaining."""
        ...

    def event(self, name: str) -> "Span":
        """Record a named event; returns ``self`` for chaining."""
        ...

    def end(self) -> None:
        """Close the span."""
        ...


class _NoopSpan:
    """A span that drops everything — the zero-cost default."""

    def attr(self, key: str, value: str) -> "Span":
        return self

    def event(self, name: str) -> "Span":
        return self

    def end(self) -> None:
        pass


class Tracer(Protocol):
    """Minimal tracing SPI: begin a span; attributes/events are added via the returned
    :class:`Span`, closed with ``end()``."""

    def start(self, name: str) -> Span:
        """Begin a span named ``name``."""
        ...


class NoopTracer:
    """A tracer whose spans are all no-ops — the default."""

    def start(self, name: str) -> Span:
        return _NOOP_SPAN


# Module-level singletons: the shared no-op span and tracer.
_NOOP_SPAN: Span = _NoopSpan()
NOOP: Tracer = NoopTracer()


@dataclass
class Recorded:
    """One completed span, as captured by :class:`RecordingTracer`."""

    name: str
    attrs: Dict[str, str] = field(default_factory=dict)
    events: List[str] = field(default_factory=list)


class _RecordingSpan:
    """A span that, on :meth:`end`, appends a :class:`Recorded` to its tracer."""

    def __init__(self, tracer: "RecordingTracer", name: str) -> None:
        self._tracer = tracer
        self._name = name
        self._attrs: Dict[str, str] = {}
        self._events: List[str] = []

    def attr(self, key: str, value: str) -> "Span":
        self._attrs[key] = value
        return self

    def event(self, name: str) -> "Span":
        self._events.append(name)
        return self

    def end(self) -> None:
        self._tracer._record(Recorded(self._name, self._attrs, self._events))


class RecordingTracer:
    """A tracer that records completed spans in ``end()`` order — for tests and local
    inspection. Thread-safe."""

    def __init__(self) -> None:
        self._spans: List[Recorded] = []
        self._guard = Lock()

    def start(self, name: str) -> Span:
        return _RecordingSpan(self, name)

    def _record(self, recorded: Recorded) -> None:
        with self._guard:
            self._spans.append(recorded)

    def spans(self) -> List[Recorded]:
        """Completed spans in ``end()`` order (a copy)."""
        with self._guard:
            return list(self._spans)

    def names(self) -> List[str]:
        """Span names in ``end()`` order."""
        return [r.name for r in self.spans()]

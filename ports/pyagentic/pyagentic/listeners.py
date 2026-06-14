"""Listeners SPI — the portable analogue of Flink's ``AgentEventListener``. Lifecycle
hooks the RoutedGraph fires per turn: start → routed → end. Defaults: a logging
listener and a counting metrics listener. Default off; opt in by passing listeners to
the graph.
"""

from __future__ import annotations

from collections import Counter
from typing import Protocol


class AgentListener(Protocol):
    """Lifecycle hooks. All are optional — a listener implements only what it needs
    (the graph fires them duck-typed), so older 3-hook listeners keep working."""

    def on_turn_start(self, event, ctx) -> None: ...

    def on_routed(self, path: str, ctx) -> None: ...

    def on_tool_call_start(self, tool_id: str, ctx) -> None: ...

    def on_tool_call_end(self, tool_id: str, result, ctx) -> None: ...

    def on_guardrail_block(self, reason: str, ctx) -> None: ...

    def on_error(self, stage: str, error, ctx) -> None: ...

    def on_turn_end(self, result, ctx) -> None: ...


class CompositeListener:
    """Fan out lifecycle events to several listeners."""

    def __init__(self, *listeners) -> None:
        self._listeners = list(listeners)

    def __getattr__(self, hook: str):
        def fan_out(*args):
            for ln in self._listeners:
                fn = getattr(ln, hook, None)
                if callable(fn):
                    fn(*args)
        return fan_out


class LoggingListener:
    """Prints each lifecycle event (stand-in for the reference LoggingAgentEventListener)."""

    def __init__(self, sink=print) -> None:
        self._sink = sink

    def on_turn_start(self, event, ctx) -> None:
        self._sink(f"[turn-start] conv={event.conversation_id} text={event.text!r}")

    def on_routed(self, path: str, ctx) -> None:
        self._sink(f"[routed] conv={ctx.conversation_id} path={path}")

    def on_tool_call_start(self, tool_id: str, ctx) -> None:
        self._sink(f"[tool-call] conv={ctx.conversation_id} tool={tool_id}")

    def on_tool_call_end(self, tool_id: str, result, ctx) -> None:
        self._sink(f"[tool-done] conv={ctx.conversation_id} tool={tool_id} result={result!r}")

    def on_guardrail_block(self, reason: str, ctx) -> None:
        self._sink(f"[guardrail-block] conv={ctx.conversation_id} reason={reason!r}")

    def on_error(self, stage: str, error, ctx) -> None:
        self._sink(f"[error] conv={ctx.conversation_id} stage={stage} error={error!r}")

    def on_turn_end(self, result, ctx) -> None:
        self._sink(f"[turn-end] conv={result.conversation_id} path={result.path} ok={result.ok} "
                   f"tools={result.tool_calls}")


class MetricsListener:
    """Counts turns, per-path dispatches, blocked turns, tool calls, and errors."""

    def __init__(self) -> None:
        self.turns = 0
        self.paths: Counter = Counter()
        self.blocked = 0
        self.tool_calls = 0
        self.errors = 0

    def on_turn_start(self, event, ctx) -> None:
        self.turns += 1

    def on_routed(self, path: str, ctx) -> None:
        self.paths[path] += 1

    def on_tool_call_end(self, tool_id: str, result, ctx) -> None:
        self.tool_calls += 1

    def on_guardrail_block(self, reason: str, ctx) -> None:
        self.blocked += 1

    def on_error(self, stage: str, error, ctx) -> None:
        self.errors += 1

    # `blocked` counts guardrail blocks (input/output) via on_guardrail_block; on_turn_end
    # is intentionally not used for it to avoid double-counting.

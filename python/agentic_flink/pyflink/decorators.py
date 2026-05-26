"""Decorators used to annotate methods on a :class:`Agent` subclass.

The decorators only attach marker attributes; the actual plan compilation
happens in :mod:`agentic_flink.pyflink.plan`. This keeps the decorators
side-effect-free so a decorated class can be imported without a JVM or
PyFlink installation.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, List, Optional, Sequence, Union

_AF_TOOL = "_af_pyflink_tool"
_AF_ACTION = "_af_pyflink_action"
_AF_LISTENER = "_af_pyflink_listener"
_AF_CHAT = "_af_pyflink_chat"


def tool(arg=None, *, name: Optional[str] = None, description: Optional[str] = None):
    """Mark a method as a Python tool exposed to the LLM.

    Usage::

        class MyAgent(Agent):
            @tool
            def add(self, a: int, b: int) -> int: ...

            @tool(name="weather", description="...")
            def get_weather(self, city: str) -> str: ...
    """

    def wrap(func: Callable) -> Callable:
        sig = inspect.signature(func)
        param_names = [
            p.name
            for p in sig.parameters.values()
            if p.name != "self"
            and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]
        setattr(
            func,
            _AF_TOOL,
            {
                "name": name or func.__name__,
                "description": description or (func.__doc__ or "").strip() or func.__name__,
                "param_names": param_names,
            },
        )
        return func

    if callable(arg):
        return wrap(arg)
    return wrap


def action(events: Union[str, Sequence[str]] = ()):
    """Mark a method as an event-keyed action.

    ``events`` is a string or list of event-type names; the operator routes events
    whose ``"type"`` field (or class simple name) matches one of these to this
    handler. An empty list / omitted value means "match anything".
    """

    if isinstance(events, str):
        ev: List[str] = [events]
    else:
        ev = list(events)

    def wrap(func: Callable) -> Callable:
        setattr(func, _AF_ACTION, {"name": func.__name__, "events": ev})
        return func

    return wrap


def listener(func: Callable) -> Callable:
    """Mark a method as a Python listener callback.

    The Java side calls listeners on standard agent lifecycle events
    (guardrail block, tool call, etc.). Phase 4 ships the marker; Phase 6
    documents the exact hook surface.
    """
    setattr(func, _AF_LISTENER, {"name": func.__name__})
    return func


def chat_model_connection(spec: Any) -> Callable[[Callable], Callable]:
    """Bind the chat model connection at class scope.

    Decorate a (placeholder) method whose return value is a :class:`ResourceRef`,
    or pass a :class:`ResourceRef` directly via ``spec``. Both forms collapse to
    the same plan entry.
    """

    def wrap(func: Callable) -> Callable:
        setattr(func, _AF_CHAT, {"spec": spec})
        return func

    return wrap


# Internal marker attribute names re-exported for the plan builder.
TOOL_ATTR = _AF_TOOL
ACTION_ATTR = _AF_ACTION
LISTENER_ATTR = _AF_LISTENER
CHAT_ATTR = _AF_CHAT

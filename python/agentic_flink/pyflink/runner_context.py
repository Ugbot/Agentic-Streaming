"""Light wrapper around the ``ctx`` dict an ``@action`` handler receives.

The Java side passes a plain :class:`dict` so PEMJA's marshalling stays trivial.
This wrapper offers attribute-style access plus a small helper surface; usage is
optional — actions can just index the dict directly.

Phase 4 ships the read-only fields the operator populates today (``agent_id``,
``event_type``, ``key``, ``processing_time``, ``chat_connection_fqn``). Phase 6
will document the full surface as it grows.
"""

from __future__ import annotations

from typing import Any, Dict


class RunnerContext:
    """Read-only view over the operator-supplied context dict."""

    __slots__ = ("_data",)

    def __init__(self, data: Dict[str, Any]):
        self._data = dict(data) if data else {}

    @property
    def agent_id(self) -> str:
        return self._data.get("agent_id", "")

    @property
    def event_type(self) -> str:
        return self._data.get("event_type", "")

    @property
    def key(self) -> Any:
        return self._data.get("key")

    @property
    def processing_time(self) -> int:
        return int(self._data.get("processing_time", 0))

    def get(self, k: str, default: Any = None) -> Any:
        return self._data.get(k, default)

    def __repr__(self) -> str:  # pragma: no cover
        return f"RunnerContext({self._data!r})"

"""Backend registry: given a built ``(graph, tools, retriever)`` and a backend name, return
a uniform runtime exposing ``submit(Event) -> TurnResult``.

``local`` (in-process, always available) is the only backend. The experimental adapters
(celery, nats, ray and the others) have moved out of this repository; the code as it stood
is at tag ``pre-cornerstone``. Any other name raises ``ValueError`` saying so.
"""

from __future__ import annotations

from typing import Any, Callable, Dict

from pyagentic.core import Event, TurnResult
from pyagentic.runtime import LocalRuntime


class _LocalBackend:
    name = "local"
    capability = "online"

    def __init__(self, graph, tools, retriever, store=None):
        self._rt = LocalRuntime(graph, store=store, tools=tools, retriever=retriever)

    def submit(self, event: Event) -> TurnResult:
        return self._rt.submit(event)

    @property
    def store(self):
        return self._rt.store

    def close(self):
        """In-process; holds no connection or worker to release."""


_BACKENDS: Dict[str, Callable[..., Any]] = {
    "local": _LocalBackend,
}


def make_backend(name: str, graph, tools, retriever, store=None):
    """Construct a backend by name from a built graph/tools/retriever. ``store`` is an
    optional hot-swapped ConversationStore (e.g. Redis). Any name that is not registered
    raises ``ValueError`` with the registered names and where the adapters went."""
    key = (name or "local").strip().lower()
    factory = _BACKENDS.get(key)
    if factory is None:
        raise ValueError(
            f"unknown backend {key!r}; supported backends: {', '.join(backend_names())}. "
            "The experimental adapters (celery, nats, ray and the others) have moved out of "
            "this repository; the old code is at tag pre-cornerstone")
    return factory(graph, tools, retriever, store=store)


def backend_names():
    return sorted(_BACKENDS)

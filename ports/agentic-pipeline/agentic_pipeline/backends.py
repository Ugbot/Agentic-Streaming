"""Backend registry — the shim that makes "choose a backend and the rest falls into
place" real. Given a built ``(graph, tools, retriever)`` and a backend name, return a
uniform runtime exposing ``submit(Event) -> TurnResult``. Every adapter is injectable
(Phase 2), so the SAME built graph runs on any backend.

Capability flags: ``online`` backends answer a turn synchronously (local/celery/nats);
``streamed``/``batch`` engines (faust/dask/airflow) host the graph but aren't a single
submit call — they're constructed the same way but driven by their engine.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Dict

# Make the sibling pure-Python core + engine adapters importable.
_PORTS = Path(__file__).resolve().parents[2]
for sub in ("pyagentic", "celery", "nats"):
    sys.path.insert(0, str(_PORTS / sub))

from pyagentic.core import Event, TurnResult  # noqa: E402
from pyagentic.runtime import LocalRuntime  # noqa: E402


class _LocalBackend:
    name = "local"
    capability = "online"

    def __init__(self, graph, tools, retriever):
        self._rt = LocalRuntime(graph, tools=tools, retriever=retriever)

    def submit(self, event: Event) -> TurnResult:
        return self._rt.submit(event)

    @property
    def store(self):
        return self._rt.store


class _CeleryBackend:
    name = "celery"
    capability = "online"

    def __init__(self, graph, tools, retriever):
        import agentic_celery as cl  # type: ignore

        if cl.Celery is None:
            raise RuntimeError("celery not installed: pip install celery")
        cl.configure(graph=graph, tools=tools, retriever=retriever)
        self._rt = cl.CeleryRuntime(eager=True)

    def submit(self, event: Event) -> TurnResult:
        return self._rt.submit(event)


class _NatsBackend:
    """Drives the async NatsRuntime on a dedicated event loop in a background thread
    (a NATS connection is bound to the loop it was created on)."""

    name = "nats"
    capability = "online"

    def __init__(self, graph, tools, retriever):
        import agentic_nats as na  # type: ignore

        if na.nats is None:
            raise RuntimeError("nats-py not installed: pip install nats-py")
        url = os.environ.get("AGENTIC_NATS_URL") or "nats://127.0.0.1:4222"
        self._rt = na.NatsRuntime(url=url, graph=graph, tools=tools, retriever=retriever)
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._run(self._rt.connect())

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout=30)

    def submit(self, event: Event) -> TurnResult:
        return self._run(self._rt.submit(event))

    def close(self):
        self._run(self._rt.close())
        self._loop.call_soon_threadsafe(self._loop.stop)


_BACKENDS: Dict[str, Callable[..., Any]] = {
    "local": _LocalBackend,
    "celery": _CeleryBackend,
    "nats": _NatsBackend,
}


def make_backend(name: str, graph, tools, retriever):
    """Construct a backend by name from a built graph/tools/retriever."""
    key = (name or "local").strip().lower()
    factory = _BACKENDS.get(key)
    if factory is None:
        raise ValueError(f"unknown backend {key!r}; choose one of {sorted(_BACKENDS)}")
    return factory(graph, tools, retriever)


def backend_names():
    return sorted(_BACKENDS)

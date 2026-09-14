"""Backend registry: given a built ``(graph, tools, retriever)`` and a backend name, return
a uniform runtime exposing ``submit(Event) -> TurnResult``. The same built graph runs on
any registered backend.

Registered backends are ``local`` (in-process, always available), ``celery`` and ``nats``.
The last two need their engine package (``agentic-pipeline[celery]`` / ``[nats]``) and the
adapter module from ``ports/celery`` / ``ports/nats`` importable; when either is missing
:class:`BackendUnavailableError` names the exact fix. An unregistered name raises
``ValueError`` listing the registered names.
"""

from __future__ import annotations

import asyncio
import os
import threading
from typing import Any, Callable, Dict

from pyagentic.core import Event, TurnResult
from pyagentic.runtime import LocalRuntime


class BackendUnavailableError(RuntimeError):
    """A registered backend cannot be constructed here; the message says what to install."""


def _import_adapter(module: str, ports_dir: str, extra: str):
    try:
        return __import__(module)
    except ImportError as exc:
        raise BackendUnavailableError(
            f"backend adapter module {module!r} is not importable ({exc}); it is the single-file module "
            f"ports/{ports_dir}/{module}.py, put that directory on PYTHONPATH and install the engine with "
            f"pip install 'agentic-pipeline[{extra}]'") from exc


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


class _CeleryBackend:
    name = "celery"
    capability = "online"

    def __init__(self, graph, tools, retriever, store=None):
        cl = _import_adapter("agentic_celery", "celery", "celery")
        if cl.Celery is None:
            raise BackendUnavailableError("celery is not installed: pip install 'agentic-pipeline[celery]'")
        cl.configure(graph=graph, tools=tools, retriever=retriever, store=store)
        self._rt = cl.CeleryRuntime(eager=True)

    def submit(self, event: Event) -> TurnResult:
        return self._rt.submit(event)


class _NatsBackend:
    """Drives the async NatsRuntime on a dedicated event loop in a background thread
    (a NATS connection is bound to the loop it was created on)."""

    name = "nats"
    capability = "online"

    def __init__(self, graph, tools, retriever, store=None):
        # NATS keeps durable per-conversation state in its own JetStream KV, so an
        # external `stores` selection doesn't apply here (the engine IS the store).
        na = _import_adapter("agentic_nats", "nats", "nats")
        if na.nats is None:
            raise BackendUnavailableError("nats-py is not installed: pip install 'agentic-pipeline[nats]'")
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


def make_backend(name: str, graph, tools, retriever, store=None):
    """Construct a backend by name from a built graph/tools/retriever. ``store`` is an
    optional hot-swapped ConversationStore (e.g. Redis); backends with their own durable
    store (nats) ignore it."""
    key = (name or "local").strip().lower()
    factory = _BACKENDS.get(key)
    if factory is None:
        raise ValueError(f"unknown backend {key!r}; supported backends: {', '.join(backend_names())}")
    return factory(graph, tools, retriever, store=store)


def backend_names():
    return sorted(_BACKENDS)

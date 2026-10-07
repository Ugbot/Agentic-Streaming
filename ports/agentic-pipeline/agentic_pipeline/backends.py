"""Backend registry: given a built ``(graph, tools, retriever)`` and a backend name, return
a uniform runtime exposing ``submit(Event) -> TurnResult``. The same built graph runs on
any registered backend.

Registered backends are ``local`` (in-process, always available), ``celery``, ``nats`` and
``ray``. Each of the last three is an experimental adapter package under
``ports/experimental`` (``agentic-celery``, ``agentic-nats``, ``agentic-ray``) that runs
``examples/pipelines/banking.yaml`` end to end with its engine running locally (Celery eager
tasks, a Ray local cluster) or in a Podman container (NATS JetStream); when the package or
its engine is missing :class:`BackendUnavailableError` names the exact fix.

The other Python adapters (``airflow``, ``dask``, ``faust``, ``gateway-fastapi``) are
demonstration-only: a batch or orchestration plane, an HTTP edge, or a worker that needs a
Kafka broker no test in this repository runs. Selecting one raises ``ValueError`` saying so
and naming the whole demonstration-only set; any other name raises ``ValueError`` listing the
registered names.

The adapters are experimental (not conformance tested); ``local`` is the only backend on
the acceptance path.
"""

from __future__ import annotations

import asyncio
import functools
import importlib
import os
import threading
from typing import Any, Callable, Dict

from pyagentic.core import Event, TurnResult
from pyagentic.runtime import LocalRuntime


class BackendUnavailableError(RuntimeError):
    """A registered backend cannot be constructed here; the message says what to install."""


EXPERIMENTAL_ADAPTERS_DIR = "ports/experimental"

# Adapters under ports/experimental that are not selectable as a ``backend:`` and why.
DEMONSTRATION_ONLY: Dict[str, str] = {
    "airflow": "an orchestration plane: one DAG run per turn with no live conversation runtime",
    "dask": "a batch data plane (ingestion, retrieval eval, transcript replay), not a turn runtime",
    "faust": "a Kafka worker; no Kafka-backed end to end test of banking.yaml runs in this repository",
    "gateway-fastapi": "an HTTP edge over the local, celery and nats backends, not a runtime itself",
}


def adapter_path(ports_dir: str) -> str:
    """Repository-relative path of an experimental adapter package directory."""
    return f"{EXPERIMENTAL_ADAPTERS_DIR}/{ports_dir}"


def _import_adapter(module: str, ports_dir: str, extra: str):
    """Import an adapter package (``agentic_<engine>``); it raises ImportError itself when
    its engine extra is missing, and the message here names both install steps."""
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise BackendUnavailableError(
            f"backend adapter package {module!r} is not importable ({exc}); install it from the checkout with "
            f"pip install -e '{adapter_path(ports_dir)}[{extra}]' (the engine itself is also "
            f"pip install 'agentic-pipeline[{extra}]')") from exc


class _LocalBackend:
    name = "local"
    capability = "online"

    def __init__(self, graph, tools, retriever, store=None, deps_factory=None):
        self._rt = LocalRuntime(graph, store=store, tools=tools, retriever=retriever)

    def submit(self, event: Event) -> TurnResult:
        return self._rt.submit(event)

    @property
    def store(self):
        return self._rt.store

    def close(self):
        """In-process; holds no connection or worker to release."""


class _CeleryBackend:
    name = "celery"
    capability = "online"

    def __init__(self, graph, tools, retriever, store=None, deps_factory=None):
        cl = _import_adapter("agentic_celery", "celery", "celery")
        cl.configure(graph=graph, tools=tools, retriever=retriever, store=store)
        self._rt = cl.CeleryRuntime(eager=True)

    def submit(self, event: Event) -> TurnResult:
        return self._rt.submit(event)

    def close(self):
        """Eager mode runs tasks in-process; there is no broker connection to release."""


class _NatsBackend:
    """Drives the async NatsRuntime on a dedicated event loop in a background thread
    (a NATS connection is bound to the loop it was created on)."""

    name = "nats"
    capability = "online"

    def __init__(self, graph, tools, retriever, store=None, deps_factory=None):
        # NATS keeps durable per-conversation state in its own JetStream KV, so an
        # external `stores` selection doesn't apply here (the engine IS the store).
        na = _import_adapter("agentic_nats", "nats", "nats")
        url = os.environ.get("AGENTIC_NATS_URL") or na.DEFAULT_URL
        self._rt = na.NatsRuntime(url=url, graph=graph, tools=tools, retriever=retriever)
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        try:
            self._run(self._rt.connect())
        except Exception as exc:
            self._loop.call_soon_threadsafe(self._loop.stop)
            raise BackendUnavailableError(
                f"nats backend: could not connect to {url} ({exc}); start a JetStream server, e.g. "
                "podman run -d --name nats-js -p 4222:4222 nats:latest -js") from exc

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout=30)

    def submit(self, event: Event) -> TurnResult:
        return self._run(self._rt.submit(event))

    def close(self):
        self._run(self._rt.close())
        self._loop.call_soon_threadsafe(self._loop.stop)


def _already_built(deps):
    return deps


class _RayBackend:
    """One Ray actor per conversation (``agentic_ray.RayRuntime``) on a local Ray cluster
    started in-process, or the cluster at ``AGENTIC_RAY_ADDRESS``.

    Each actor builds its own graph, tools and retriever by calling ``deps_factory`` (the
    loader passes one that recompiles the spec, which pickles as plain data); the already
    built triple only travels to the actors when no factory is given, and Ray then reports
    exactly what in it is not serializable (the in-memory stores hold locks)."""

    name = "ray"
    capability = "online"

    def __init__(self, graph, tools, retriever, store=None, deps_factory=None):
        ra = _import_adapter("agentic_ray", "ray", "ray")
        if deps_factory is None:
            deps_factory = functools.partial(_already_built, (graph, tools, retriever))
        self._rt = ra.RayRuntime(
            deps_factory=deps_factory,
            namespace=f"agentic-pipeline-{id(self)}",
            durable_store=store,
        )

    def submit(self, event: Event) -> TurnResult:
        return self._rt.submit(event)

    def close(self):
        self._rt.close()


_BACKENDS: Dict[str, Callable[..., Any]] = {
    "local": _LocalBackend,
    "celery": _CeleryBackend,
    "nats": _NatsBackend,
    "ray": _RayBackend,
}


def make_backend(name: str, graph, tools, retriever, store=None, deps_factory=None):
    """Construct a backend by name from a built graph/tools/retriever. ``store`` is an
    optional hot-swapped ConversationStore (e.g. Redis); backends with their own durable
    store (nats) ignore it. ``deps_factory`` is a picklable zero-argument callable that
    rebuilds ``(graph, tools, retriever)``; only backends whose workers run in other
    processes (ray) use it. A demonstration-only adapter name and an unknown name both
    raise ``ValueError`` with the registered names."""
    key = (name or "local").strip().lower()
    factory = _BACKENDS.get(key)
    if factory is None:
        supported = ", ".join(backend_names())
        if key in DEMONSTRATION_ONLY:
            raise ValueError(
                f"backend {key!r} is demonstration-only ({DEMONSTRATION_ONLY[key]}); "
                f"demonstration-only adapters under {EXPERIMENTAL_ADAPTERS_DIR}: "
                f"{', '.join(demonstration_only_names())}; supported backends: {supported}")
        raise ValueError(f"unknown backend {key!r}; supported backends: {supported}")
    return factory(graph, tools, retriever, store=store, deps_factory=deps_factory)


def backend_names():
    return sorted(_BACKENDS)


def demonstration_only_names():
    return sorted(DEMONSTRATION_ONLY)

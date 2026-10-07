"""Pluggable backend runtimes behind the FastAPI gateway.

Each backend exposes the same synchronous seam so the HTTP layer never has to know
which substrate ran the turn:

    submit(conversation_id, text, user_id) -> dict   # {conversation_id, reply, path, ok, tool_calls}
    history(conversation_id)               -> list[dict]  # [{role, content}, ...]
    name                                   -> str          # "local" | "celery" | "nats"

The portable router->path->verifier graph, tools, and retrieval all come from
``pyagentic``; only the runtime/transport seam differs per backend. The ``local``
backend is always available and has zero third-party dependencies; ``celery`` and
``nats`` import their adapter packages (``agentic-celery``, ``agentic-nats``, the
gateway's optional extras of the same names) when constructed, so this module imports
cleanly without them and raises a clear error only when such a backend is selected.
"""

from __future__ import annotations

import asyncio
import os
import threading
from typing import List, Optional, Protocol

from pyagentic.banking import build_banking_graph, default_tools, seed_kb
from pyagentic.core import Event, TurnResult
from pyagentic.memory import (
    ConversationStore,
    InMemoryConversationStore,
    InMemoryKeyedStateStore,
)
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever
from pyagentic.runtime import LocalRuntime

try:
    import agentic_celery
    _CELERY_IMPORT_ERROR: Optional[ImportError] = None
except ImportError as exc:
    agentic_celery = None
    _CELERY_IMPORT_ERROR = exc

try:
    import agentic_nats
    _NATS_IMPORT_ERROR: Optional[ImportError] = None
except ImportError as exc:
    agentic_nats = None
    _NATS_IMPORT_ERROR = exc


def _build_retriever() -> TwoTierRetriever:
    """Hot KB index seeded from the banking knowledge base (cold tier unused locally)."""
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    return TwoTierRetriever(hot, None, 4, 4)


def _result_to_dict(result: TurnResult) -> dict:
    return {
        "conversation_id": result.conversation_id,
        "reply": result.reply,
        "path": result.path,
        "ok": result.ok,
        "tool_calls": list(result.tool_calls),
    }


def _history_from_store(store: ConversationStore, conversation_id: str) -> List[dict]:
    return [{"role": m.role, "content": m.content} for m in store.history(conversation_id)]


class Backend(Protocol):
    """The synchronous seam every backend implements for the HTTP layer."""

    name: str

    def submit(self, conversation_id: str, text: str, user_id: str = "anonymous") -> dict: ...

    def history(self, conversation_id: str) -> List[dict]: ...


class LocalBackend:
    """In-process backend: ``LocalRuntime`` over the banking graph with a shared
    ``InMemoryConversationStore`` so transcripts are inspectable via :meth:`history`.

    Always available, zero third-party dependencies. This is the default."""

    name = "local"

    def __init__(self) -> None:
        self._store: ConversationStore = InMemoryConversationStore()
        self._runtime = LocalRuntime(
            handler=build_banking_graph(),
            store=self._store,
            state=InMemoryKeyedStateStore(),
            tools=default_tools(),
            retriever=_build_retriever(),
        )

    def submit(self, conversation_id: str, text: str, user_id: str = "anonymous") -> dict:
        event = Event(conversation_id=conversation_id, text=text, user_id=user_id)
        return _result_to_dict(self._runtime.submit(event))

    def history(self, conversation_id: str) -> List[dict]:
        return _history_from_store(self._store, conversation_id)


class CeleryBackend:
    """Backend over the Celery adapter (the ``agentic-celery`` package) in eager
    (in-process, no broker) mode. Uses a shared ``InMemoryConversationStore`` injected
    via the adapter's ``configure`` so transcripts stay inspectable for :meth:`history`.

    Raises a clear error at construction if the adapter or Celery is not installed;
    importing this module never requires either."""

    name = "celery"

    def __init__(self) -> None:
        if agentic_celery is None:
            raise RuntimeError(
                f"celery backend unavailable: {_CELERY_IMPORT_ERROR}. Install the gateway's celery extra: "
                "pip install 'agentic-gateway-fastapi[celery]'"
            ) from _CELERY_IMPORT_ERROR

        # Share a ConversationStore so /conversations works, and seed the retriever.
        self._store: ConversationStore = InMemoryConversationStore()
        agentic_celery.configure(
            graph=build_banking_graph(),
            tools=default_tools(),
            retriever=_build_retriever(),
            store=self._store,
            state=InMemoryKeyedStateStore(),
        )
        self._runtime = agentic_celery.CeleryRuntime(eager=True)

    def submit(self, conversation_id: str, text: str, user_id: str = "anonymous") -> dict:
        event = Event(conversation_id=conversation_id, text=text, user_id=user_id)
        return _result_to_dict(self._runtime.submit(event))

    def history(self, conversation_id: str) -> List[dict]:
        return _history_from_store(self._store, conversation_id)


class NatsBackend:
    """Backend over the NATS JetStream adapter (the ``agentic-nats`` package).

    ``NatsRuntime`` is asyncio-native and a NATS connection is bound to the event loop
    it was created on, so this backend owns ONE persistent loop in a background thread:
    ``connect`` and every ``submit`` are scheduled onto that single loop via
    ``run_coroutine_threadsafe``. Durable per-conversation state lives in JetStream KV,
    so :meth:`history` reads the transcript back from KV (not a local store).

    Requires a reachable JetStream server (``AGENTIC_NATS_URL``); construction raises a
    clear error if the adapter or ``nats-py`` is missing or the server is unreachable."""

    name = "nats"

    def __init__(self, url: Optional[str] = None) -> None:
        if agentic_nats is None:
            raise RuntimeError(
                f"nats backend unavailable: {_NATS_IMPORT_ERROR}. Install the gateway's nats extra: "
                "pip install 'agentic-gateway-fastapi[nats]'"
            ) from _NATS_IMPORT_ERROR

        resolved_url = url or os.environ.get("AGENTIC_NATS_URL", agentic_nats.DEFAULT_URL)
        self._runtime = agentic_nats.NatsRuntime(
            url=resolved_url,
            graph=build_banking_graph(),
            tools=default_tools(),
            retriever=_build_retriever(),
        )

        # One persistent loop on a background thread; the connection binds to it.
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="nats-loop", daemon=True)
        self._thread.start()
        try:
            self._run(self._runtime.connect(), timeout=10)
        except Exception as exc:
            self._shutdown_loop()
            raise RuntimeError(
                f"nats backend unavailable: could not connect to {resolved_url} ({exc}). "
                "Start a JetStream server, e.g. `podman run -p 4222:4222 nats:latest -js`."
            ) from exc

    def _run(self, coro, timeout: float = 30):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def _shutdown_loop(self) -> None:
        async def _cancel_pending() -> None:
            pending = [t for t in asyncio.all_tasks(self._loop) if t is not asyncio.current_task()]
            for t in pending:
                t.cancel()
            for t in pending:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        try:
            asyncio.run_coroutine_threadsafe(_cancel_pending(), self._loop).result(timeout=5)
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        if not self._loop.is_closed():
            self._loop.close()

    def submit(self, conversation_id: str, text: str, user_id: str = "anonymous") -> dict:
        event = Event(conversation_id=conversation_id, text=text, user_id=user_id)
        return _result_to_dict(self._run(self._runtime.submit(event)))

    def history(self, conversation_id: str) -> List[dict]:
        # Read the durable transcript back from JetStream KV via the adapter's loader.
        store, _owner, _rev = self._run(self._runtime._load(conversation_id))
        return _history_from_store(store, conversation_id)

    def close(self) -> None:
        try:
            self._run(self._runtime.close(), timeout=5)
        finally:
            self._shutdown_loop()


_BACKENDS = {
    "local": LocalBackend,
    "celery": CeleryBackend,
    "nats": NatsBackend,
}


def make_backend(name: Optional[str] = None) -> Backend:
    """Construct a backend by name. ``None`` reads ``AGENTIC_GATEWAY_BACKEND`` and
    defaults to ``"local"``. Raises ``ValueError`` for an unknown name."""
    resolved = (name or os.environ.get("AGENTIC_GATEWAY_BACKEND") or "local").strip().lower()
    factory = _BACKENDS.get(resolved)
    if factory is None:
        raise ValueError(
            f"unknown backend {resolved!r}; choose one of {sorted(_BACKENDS)}"
        )
    return factory()

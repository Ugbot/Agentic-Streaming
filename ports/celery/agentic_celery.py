"""Agentic-Flink on **Celery** — pure Python.

See ../../docs/portability/celery.md. Celery is a distributed *task queue*, not a
stream processor, so the essence maps a little differently than Faust/Ray:

  - **one turn = one Celery task** (``process_turn``). The task is stateless; all
    durable state lives in a shared ``pyagentic.ConversationStore`` (Redis-backed in
    production — Celery already runs on Redis/RabbitMQ), so C1 is *external*.
  - **single-writer-per-conversation (C2)** is not native. Celery has no keyed
    ordering, so we recover it the way the design doc prescribes: route every task
    for a conversation to the *same* queue (``conversation_queue(cid)``) consumed by a
    single worker, and take a per-conversation lock inside the worker. Turns for one
    conversation then run one-at-a-time, in submit order.
  - **fault tolerance (C3)** rides Celery's ``acks_late`` + ``retry`` + the result
    backend: a task that dies is redelivered; the ConversationStore is the source of
    truth so a retried turn is idempotent against the transcript.
  - **async I/O (C4)** is a Celery ``chord``/``chain`` (or an async task body) — the
    LLM/A2A call is its own task whose result feeds the verifier task.

The portable router->path->verifier graph + tools + retrieval are reused verbatim
from ``pyagentic``; only the task/runtime seam is Celery-specific.

Run live, no broker needed (eager mode, in-process — this is what the test uses):
    python agentic_celery.py
Run distributed (needs Redis + a worker):
    AGENTIC_CELERY_BROKER=redis://localhost:6379/0 \
    AGENTIC_CELERY_BACKEND=redis://localhost:6379/1 \
        celery -A agentic_celery:app worker -Q agentic.conv.0,agentic.conv.1 -l info
    # then submit turns from another process via CeleryRuntime(eager=False)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from threading import Lock
from typing import Dict

# Make the sibling pure-Python core importable (or `pip install -e ../pyagentic`).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pyagentic"))

from pyagentic.banking import build_banking_graph, default_tools, seed_kb  # noqa: E402
from pyagentic.core import AgentContext, Event, TurnResult  # noqa: E402
from pyagentic.memory import InMemoryConversationStore, InMemoryKeyedStateStore  # noqa: E402
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever  # noqa: E402

try:
    import celery as _celery_pkg
    from celery import Celery
    if not hasattr(_celery_pkg, "Celery"):  # guard against a namespace-package shadow
        Celery = None  # type: ignore
except ImportError:  # keep importable without the engine installed
    Celery = None  # type: ignore


# ---- per-worker, process-local agent dependencies -------------------------------
# Built once per worker process. In production the ConversationStore is a Redis/Fluss
# implementation (shared across workers); here it is in-memory and the eager runtime
# keeps everything in one process, so it is genuinely shared for the demo.

_NUM_CONVERSATION_QUEUES = int(os.environ.get("AGENTIC_CELERY_QUEUES", "4"))


def conversation_queue(conversation_id: str) -> str:
    """Stable queue name for a conversation — the C2 seam. Every turn for a given
    conversation is routed here, so a single worker processes the conversation in
    order (the Celery analogue of a Kafka partition / Flink keyBy)."""
    bucket = (hash(conversation_id) & 0x7FFFFFFF) % _NUM_CONVERSATION_QUEUES
    return f"agentic.conv.{bucket}"


class _Deps:
    """Lazily-built shared graph/tools/retriever/store + per-conversation locks.

    Every field defaults to the shared ``pyagentic`` banking essence, so adding a new
    tool/path to the core propagates here with no adapter change. The components are
    injectable (see :func:`configure`) for production — e.g. a Redis-backed
    ConversationStore shared across workers — and for tests that exercise an *extended*
    core graph through this same Celery task seam.
    """

    _instance: "_Deps | None" = None

    def __init__(self, graph=None, tools=None, retriever=None, store=None, state=None) -> None:
        self.graph = graph if graph is not None else build_banking_graph()
        self.tools = tools if tools is not None else default_tools()
        if retriever is not None:
            self.retriever = retriever
        else:
            hot = InMemoryHotVectorIndex()
            seed_kb(hot)
            self.retriever = TwoTierRetriever(hot, None, 4, 4)
        self.store = store if store is not None else InMemoryConversationStore()
        self.state = state if state is not None else InMemoryKeyedStateStore()
        self._locks: Dict[str, Lock] = {}
        self._locks_guard = Lock()

    @classmethod
    def get(cls) -> "_Deps":
        if cls._instance is None:
            cls._instance = _Deps()
        return cls._instance

    def lock_for(self, cid: str) -> Lock:
        with self._locks_guard:
            lk = self._locks.get(cid)
            if lk is None:
                lk = Lock()
                self._locks[cid] = lk
            return lk


def configure(graph=None, tools=None, retriever=None, store=None, state=None) -> None:
    """Replace the shared worker dependencies. Call once at worker startup to inject a
    production ConversationStore (Redis/Fluss) or an extended graph/tool set. The
    portable agent logic stays in the core; only what's injected here changes."""
    _Deps._instance = _Deps(graph=graph, tools=tools, retriever=retriever, store=store, state=state)


def _run_turn(conversation_id: str, text: str, user_id: str) -> dict:
    """The actual work — identical regardless of how Celery delivered it. Takes the
    per-conversation lock so concurrent worker threads can't interleave a single
    conversation (C2 within the worker), then runs the portable graph."""
    deps = _Deps.get()
    with deps.lock_for(conversation_id):
        ctx = AgentContext(
            conversation_id=conversation_id,
            user_id=user_id,
            store=deps.store,
            state=deps.state,
            tools=deps.tools,
            retriever=deps.retriever,
        )
        result: TurnResult = deps.graph.handle(Event(conversation_id, text, user_id), ctx)
        return {
            "conversation_id": result.conversation_id,
            "reply": result.reply,
            "path": result.path,
            "ok": result.ok,
            "tool_calls": result.tool_calls,
        }


# ---- the Celery app + task ------------------------------------------------------

if Celery is not None:
    app = Celery(
        "agentic-celery",
        broker=os.environ.get("AGENTIC_CELERY_BROKER", "memory://"),
        backend=os.environ.get("AGENTIC_CELERY_BACKEND", "cache+memory://"),
    )
    # C3 knobs: redeliver on worker loss; the ConversationStore makes a redelivered
    # turn idempotent. C2 knob: a worker process handles one conversation queue.
    app.conf.update(
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_default_queue="agentic.conv.0",
        worker_prefetch_multiplier=1,
        result_expires=3600,
    )

    @app.task(name="agentic.process_turn", bind=True, max_retries=2, acks_late=True)
    def process_turn(self, conversation_id: str, text: str, user_id: str = "anonymous") -> dict:
        """One conversational turn as a Celery task. Routed to the conversation's queue
        so it is single-writer; retried with backoff on transient failure (C3/C4)."""
        try:
            return _run_turn(conversation_id, text, user_id)
        except Exception as exc:  # transient backend/tool failure -> retry with backoff
            raise self.retry(exc=exc, countdown=min(2 ** self.request.retries, 8))
else:
    app = None  # importable for inspection/tests without celery installed
    process_turn = None  # type: ignore


class CeleryRuntime:
    """``pyagentic.Runtime`` over Celery: submits each turn as a task to the
    conversation's queue and awaits the result. ``eager=True`` runs the task body
    in-process with no broker (great for tests and this demo); ``eager=False`` sends
    it to a real worker over the configured broker."""

    def __init__(self, eager: bool = True) -> None:
        if Celery is None:
            raise RuntimeError("celery not installed: pip install celery")
        self.eager = eager
        app.conf.task_always_eager = eager
        app.conf.task_eager_propagates = eager

    def submit(self, event: Event) -> TurnResult:
        async_result = process_turn.apply_async(
            args=[event.conversation_id, event.text, event.user_id],
            queue=conversation_queue(event.conversation_id),
        )
        d = async_result.get(timeout=30)
        return TurnResult(
            d["conversation_id"], d["reply"], d["path"], d["ok"], d["tool_calls"]
        )


def _demo() -> None:
    rt = CeleryRuntime(eager=True)  # in-process, no broker
    turns = [
        ("c1", "what card types do you offer?"),
        ("c2", "what is my balance?"),
        ("c1", "tell me about crypto cash-back"),
        ("c3", "where is the nearest branch?"),
    ]
    for cid, text in turns:
        r = rt.submit(Event(cid, text, user_id="demo"))
        print(f"[{cid}] queue={conversation_queue(cid)} path={r.path} ok={r.ok} "
              f"reply={r.reply!r} tools={r.tool_calls}")


if __name__ == "__main__":  # pragma: no cover
    if Celery is None:
        print("celery not installed. `pip install celery`, then: python agentic_celery.py")
    else:
        _demo()

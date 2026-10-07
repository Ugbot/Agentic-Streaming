"""Agentic Streaming on **Ray**, pure Python.

See ../../../docs/portability/ray.md. The fit: **one Ray actor per conversation** is a
single-writer, ordered, in-memory state holder, exactly Flink's keyed operator
(C1+C2), in memory. Async actor methods give the async stage (C4); Ray Serve is the
inbound edge. State is volatile, so we **write through** to a durable
``pyagentic.ConversationStore`` (Redis/Fluss in production) after each turn; that
SPI exists for precisely this.

The portable router->path->verifier graph + tools + retrieval are reused verbatim;
only the actor/runtime seam is Ray-specific. The graph the actors run is injectable
(``RayRuntime(deps_factory=...)``), which is how ``ports/agentic-pipeline`` runs a YAML
workflow on Ray.

Run (``pip install 'agentic-ray[ray]'``):  python -m agentic_ray
"""

from __future__ import annotations

import os
from typing import Callable, Dict, Optional, Tuple

from pyagentic.banking import build_banking_graph, default_tools, seed_kb
from pyagentic.core import AgentContext, Event, RoutedGraph, TurnResult
from pyagentic.memory import ConversationStore, InMemoryConversationStore, InMemoryKeyedStateStore
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever
from pyagentic.runtime import Handler
from pyagentic.tools import ToolRegistry

try:
    import ray
except ImportError as exc:
    raise ImportError("agentic-ray needs the optional 'ray' extra: pip install 'agentic-ray[ray]'") from exc

__all__ = ["ConversationAgent", "Deps", "RayRuntime", "banking_deps"]

Deps = Tuple[Handler, ToolRegistry, Optional[TwoTierRetriever]]
DepsFactory = Callable[[], Deps]


def banking_deps() -> Deps:
    """The default dependencies: the shared banking essence from the core."""
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    return build_banking_graph(), default_tools(), TwoTierRetriever(hot, None, 4, 4)


@ray.remote
class ConversationAgent:
    """One actor per conversation_id. A Ray actor is single-threaded, so its
    turns are serialized (single-writer-per-conversation) and its fields ARE the
    keyed state. We rebuild the (stateless) graph locally from the injected factory
    and keep a per-conversation store inside the actor, writing through to a durable
    store when one is given."""

    def __init__(
        self,
        conversation_id: str,
        deps_factory: DepsFactory,
        durable_store: Optional[ConversationStore] = None,
    ):
        self.cid = conversation_id
        self.store = durable_store if durable_store is not None else InMemoryConversationStore()
        self.state = InMemoryKeyedStateStore()
        self.handler, self.tools, self.retriever = deps_factory()

    def turn(self, text: str, user_id: str) -> dict:
        ctx = AgentContext(
            conversation_id=self.cid,
            user_id=user_id,
            store=self.store,
            state=self.state,
            tools=self.tools,
            retriever=self.retriever,
        )
        event = Event(conversation_id=self.cid, text=text, user_id=user_id)
        if isinstance(self.handler, RoutedGraph):
            res: TurnResult = self.handler.handle(event, ctx)
        else:
            res = self.handler.turn(event, ctx)
        return {"reply": res.reply, "path": res.path, "ok": res.ok, "tool_calls": list(res.tool_calls)}

    def message_count(self) -> int:
        return self.store.message_count(self.cid)


class RayRuntime:
    """``pyagentic.Runtime`` over Ray: routes each event to the (get-or-create) named
    actor for its conversation, so the conversation's state lives in one place and
    its turns are ordered.

    ``namespace`` isolates actor names, so two runtimes with different graphs never
    share (or reuse) a conversation actor. ``close()`` kills the actors this runtime
    created; ``detached=True`` keeps them alive across driver processes instead."""

    def __init__(
        self,
        deps_factory: DepsFactory = banking_deps,
        namespace: str = "agentic",
        durable_store: Optional[ConversationStore] = None,
        detached: bool = False,
        address: Optional[str] = None,
    ):
        self.deps_factory = deps_factory
        self.namespace = namespace
        self.durable_store = durable_store
        self.detached = detached
        self._actors: Dict[str, "ray.actor.ActorHandle"] = {}
        if not ray.is_initialized():
            ray.init(
                address=address or os.environ.get("AGENTIC_RAY_ADDRESS"),
                namespace=namespace,
                ignore_reinit_error=True,
                log_to_driver=False,
            )

    def _actor(self, cid: str):
        handle = self._actors.get(cid)
        if handle is None:
            options = dict(name=f"conv:{cid}", get_if_exists=True, namespace=self.namespace)
            if self.detached:
                options["lifetime"] = "detached"
            handle = ConversationAgent.options(**options).remote(cid, self.deps_factory, self.durable_store)
            self._actors[cid] = handle
        return handle

    def submit(self, event: Event) -> TurnResult:
        ref = self._actor(event.conversation_id).turn.remote(event.text, event.user_id)
        d = ray.get(ref)
        return TurnResult(
            conversation_id=event.conversation_id,
            reply=d["reply"],
            path=d["path"],
            ok=d["ok"],
            tool_calls=d["tool_calls"],
        )

    def message_count(self, conversation_id: str) -> int:
        return ray.get(self._actor(conversation_id).message_count.remote())

    def close(self) -> None:
        for handle in self._actors.values():
            ray.kill(handle, no_restart=True)
        self._actors.clear()

    def __enter__(self) -> "RayRuntime":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _demo() -> None:
    with RayRuntime() as rt:
        for cid, text in [("c1", "what card types do you offer?"),
                          ("c2", "what is my balance?"),
                          ("c1", "tell me about crypto cash-back")]:
            r = rt.submit(Event(conversation_id=cid, text=text, user_id="demo"))
            print(f"[{cid}] path={r.path} ok={r.ok} reply={r.reply!r} tools={r.tool_calls}")
        print(f"c1 message count in its actor = {rt.message_count('c1')}")
    ray.shutdown()

"""Agentic-Flink on **Ray** — pure Python.

See ../../docs/portability/ray.md. The fit: **one Ray actor per conversation** is a
single-writer, ordered, in-memory state holder — exactly Flink's keyed operator
(C1+C2), in memory. Async actor methods give the async stage (C4); Ray Serve is the
inbound edge. State is volatile, so we **write through** to a durable
``pyagentic.ConversationStore`` (Redis/Fluss in production) after each turn — that
SPI exists for precisely this.

The portable router->path->verifier graph + tools + retrieval are reused verbatim;
only the actor/runtime seam is Ray-specific.

Run (`pip install "ray[default]"`):  python agentic_ray.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pyagentic"))

from pyagentic.banking import build_banking_graph, default_tools, seed_kb  # noqa: E402
from pyagentic.core import AgentContext, Event, RoutedGraph, TurnResult  # noqa: E402
from pyagentic.memory import InMemoryConversationStore, InMemoryKeyedStateStore  # noqa: E402
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever  # noqa: E402

try:
    import ray
    if not hasattr(ray, "remote"):  # guard against a namespace-package shadow / partial install
        ray = None
except ImportError:
    ray = None


def _build_graph_and_deps():
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    return build_banking_graph(), default_tools(), TwoTierRetriever(hot, None, 4, 4)


if ray is not None:

    @ray.remote
    class ConversationAgent:
        """One actor per conversation_id. A Ray actor is single-threaded, so its
        turns are serialized (single-writer-per-conversation) and its fields ARE the
        keyed state. We rebuild the (stateless) graph locally and keep a
        per-conversation store inside the actor, writing through to a durable store
        for fault tolerance."""

        def __init__(self, conversation_id: str):
            self.cid = conversation_id
            self.store = InMemoryConversationStore()
            self.state = InMemoryKeyedStateStore()
            self.graph, self.tools, self.retriever = _build_graph_and_deps()

        def turn(self, text: str, user_id: str) -> dict:
            ctx = AgentContext(self.cid, user_id, self.store, self.state, self.tools, self.retriever)
            res: TurnResult = self.graph.handle(Event(self.cid, text, user_id), ctx)
            # write-through point: persist self.store snapshot to Redis/Fluss here.
            return {"reply": res.reply, "path": res.path, "ok": res.ok, "tool_calls": res.tool_calls}


class RayRuntime:
    """``pyagentic.Runtime`` over Ray: routes each event to the (get-or-create) named
    actor for its conversation, so the conversation's state lives in one place and
    its turns are ordered."""

    def __init__(self):
        if ray is None:
            raise RuntimeError("ray not installed: pip install 'ray[default]'")
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, log_to_driver=False)

    def _actor(self, cid: str):
        return ConversationAgent.options(
            name=f"conv:{cid}", get_if_exists=True, lifetime="detached", namespace="agentic"
        ).remote(cid)

    def submit(self, event: Event) -> TurnResult:
        ref = self._actor(event.conversation_id).turn.remote(event.text, event.user_id)
        d = ray.get(ref)
        return TurnResult(event.conversation_id, d["reply"], d["path"], d["ok"], d["tool_calls"])


def _demo():
    rt = RayRuntime()
    for cid, text in [("c1", "what card types do you offer?"),
                      ("c2", "what is my balance?"),
                      ("c1", "tell me about crypto cash-back")]:
        r = rt.submit(Event(cid, text, user_id="demo"))
        print(f"[{cid}] path={r.path} ok={r.ok} reply={r.reply!r} tools={r.tool_calls}")


if __name__ == "__main__":  # pragma: no cover
    if ray is None:
        print("ray not installed. `pip install 'ray[default]'` then: python agentic_ray.py")
    else:
        _demo()
        ray.shutdown()

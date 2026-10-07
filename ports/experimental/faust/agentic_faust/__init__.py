"""Agentic Streaming on **Faust** (faust-streaming), pure Python.

See ../../../docs/portability/faust.md. The essence maps almost 1:1:
  - a Faust ``@app.agent`` (a keyed Kafka stream processor) hosts our RoutedGraph;
  - a Faust ``Table`` (RocksDB + changelog) is the durable per-conversation state,
    wrapped as a ``pyagentic.ConversationStore``;
  - partitioning by ``conversation_id`` gives single-writer-per-conversation (C2);
  - native ``asyncio`` gives the async stage (C4).

The portable agent core (router->path->verifier, tools, retrieval) is reused
verbatim from ``pyagentic``; only the runtime seam is Faust-specific.

This module is the engine-free half: the Table-backed ``ConversationStore``, the
injectable dependencies and :func:`handle_turn`, the load -> handle -> save bracket
the Faust agent runs per record. It imports without faust so the store and the turn
logic are unit-testable over plain dicts. ``agentic_faust.app`` holds the Faust
``App`` and needs the optional ``faust`` extra plus a Kafka broker:

    pip install 'agentic-faust[faust]'
    AGENTIC_FAUST_BROKER=kafka://localhost:9092 faust -A agentic_faust.app worker -l info
    # then produce to the 'agentic.requests' topic a JSON {conversation_id,user_id,text}

``python -m agentic_faust`` runs the banking turns through :func:`handle_turn` over
in-memory tables (no broker).
"""

from __future__ import annotations

from typing import Dict, List, MutableMapping, Optional

from pyagentic.banking import build_banking_graph, default_tools, seed_kb
from pyagentic.core import AgentContext, Event, TurnResult
from pyagentic.memory import ChatMessage, ConversationStore, InMemoryKeyedStateStore
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever

__all__ = [
    "FaustTableConversationStore",
    "configure",
    "handle_turn",
    "result_to_dict",
]


class FaustTableConversationStore(ConversationStore):
    """A ``ConversationStore`` backed by Faust Tables: durable keyed state with a
    changelog topic, recovered on rebalance. One row per conversation id holds a
    serialisable envelope (messages + attributes + owner). Any two mutable mappings
    work (a Faust ``Table`` in the worker, plain dicts in tests)."""

    def __init__(
        self,
        transcript_table: MutableMapping[str, List[dict]],
        attr_table: MutableMapping[str, Dict[str, str]],
        max_messages: int = 200,
    ):
        self._t = transcript_table  # cid -> list[dict]
        self._a = attr_table        # cid -> dict[str,str] (also "__owner__")
        self._max = max_messages

    def append(self, conversation_id: str, message: ChatMessage) -> None:
        msgs = list(self._t.get(conversation_id) or [])
        msgs.append({"role": message.role, "content": message.content,
                     "tool_name": message.tool_name, "tool_call_id": message.tool_call_id})
        self._t[conversation_id] = msgs[-self._max:]

    def history(self, conversation_id: str) -> List[ChatMessage]:
        return [
            ChatMessage(
                role=m["role"],
                content=m["content"],
                tool_name=m.get("tool_name"),
                tool_call_id=m.get("tool_call_id"),
            )
            for m in (self._t.get(conversation_id) or [])
        ]

    def message_count(self, conversation_id: str) -> int:
        return len(self._t.get(conversation_id) or [])

    def put_attribute(self, conversation_id: str, key: str, value: str) -> None:
        a = dict(self._a.get(conversation_id) or {})
        a[key] = value
        self._a[conversation_id] = a

    def get_attribute(self, conversation_id: str, key: str) -> Optional[str]:
        return (self._a.get(conversation_id) or {}).get(key)

    def attributes(self, conversation_id: str) -> Dict[str, str]:
        return {k: v for k, v in (self._a.get(conversation_id) or {}).items() if not k.startswith("__")}

    def associate_user(self, conversation_id: str, user_id: str) -> None:
        a = dict(self._a.get(conversation_id) or {})
        a["__owner__"] = user_id
        self._a[conversation_id] = a

    def conversations_for_user(self, user_id: str) -> List[str]:
        # Demo-scope scan; production keeps a reverse-index Table keyed by user.
        return [cid for cid in list(self._a.keys()) if (self._a.get(cid) or {}).get("__owner__") == user_id]

    def clear(self, conversation_id: str) -> None:
        self._t.pop(conversation_id, None)
        self._a.pop(conversation_id, None)


# ---- injectable deps (default to the shared banking essence) ----
# A YAML loader / custom workflow calls configure(...) before the worker starts to run
# an arbitrary graph through the Faust agent with no code change.
_graph = build_banking_graph()
_tools = default_tools()
_hot = InMemoryHotVectorIndex()
seed_kb(_hot)
_retriever = TwoTierRetriever(_hot, None, 4, 4)
_state = InMemoryKeyedStateStore()


def configure(graph=None, tools=None, retriever=None, state=None) -> None:
    global _graph, _tools, _retriever, _state
    if graph is not None:
        _graph = graph
    if tools is not None:
        _tools = tools
    if retriever is not None:
        _retriever = retriever
    if state is not None:
        _state = state


def handle_turn(conversation_id: str, user_id: str, text: str, store: ConversationStore) -> TurnResult:
    """One conversational turn over the given store: the body the Faust agent runs for
    every record of a conversation's partition (single writer per key)."""
    ctx = AgentContext(
        conversation_id=conversation_id,
        user_id=user_id,
        store=store,
        state=_state,
        tools=_tools,
        retriever=_retriever,
    )
    return _graph.handle(Event(conversation_id=conversation_id, text=text, user_id=user_id), ctx)


def result_to_dict(result: TurnResult) -> dict:
    """The reply record published to ``agentic.replies``."""
    return {
        "conversation_id": result.conversation_id,
        "reply": result.reply,
        "path": result.path,
        "ok": result.ok,
        "tool_calls": list(result.tool_calls),
    }


def _demo() -> None:
    store = FaustTableConversationStore({}, {})
    turns = [
        ("c1", "what card types do you offer?"),
        ("c2", "what is my balance?"),
        ("c1", "tell me about crypto cash-back"),
        ("c3", "where is the nearest branch?"),
    ]
    for cid, text in turns:
        r = handle_turn(cid, "demo", text, store)
        print(f"[{cid}] path={r.path} ok={r.ok} reply={r.reply!r} tools={r.tool_calls}")
    print(f"c1 persisted message count = {store.message_count('c1')}")

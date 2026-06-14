"""Agentic-Flink on **Faust** (faust-streaming) — pure Python.

See ../../docs/portability/faust.md. The essence maps almost 1:1:
  - a Faust ``@app.agent`` (a keyed Kafka stream processor) hosts our RoutedGraph;
  - a Faust ``Table`` (RocksDB + changelog) is the durable per-conversation state,
    wrapped as a ``pyagentic.ConversationStore``;
  - partitioning by ``conversation_id`` gives single-writer-per-conversation (C2);
  - native ``asyncio`` gives the async stage (C4).

The portable agent core (router->path->verifier, tools, retrieval) is reused
verbatim from ``pyagentic``; only the runtime seam is Faust-specific.

Run (needs Kafka on localhost:9092 + `pip install faust-streaming`):
    faust -A agentic_faust:app worker -l info
    # then produce to the 'agentic.requests' topic a JSON {conversation_id,user_id,text}
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional

# Make the sibling pure-Python core importable (or `pip install -e ../pyagentic`).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pyagentic"))

from pyagentic.banking import build_banking_graph, default_tools, seed_kb  # noqa: E402
from pyagentic.core import Event  # noqa: E402
from pyagentic.memory import ChatMessage, ConversationStore  # noqa: E402
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever  # noqa: E402

try:
    import faust
    if not hasattr(faust, "App"):  # guard against a namespace-package shadow / partial install
        faust = None
except ImportError:  # keep importable without the engine installed
    faust = None


class FaustTableConversationStore(ConversationStore):
    """A ``ConversationStore`` backed by Faust Tables — durable keyed state with a
    changelog topic, recovered on rebalance. One row per conversation id holds a
    serialisable envelope (messages + attributes + owner)."""

    def __init__(self, transcript_table, attr_table, max_messages: int = 200):
        self._t = transcript_table  # cid -> list[dict]
        self._a = attr_table        # cid -> dict[str,str] (also "__owner__", "__user:<u>__")
        self._max = max_messages

    def append(self, conversation_id: str, message: ChatMessage) -> None:
        msgs = list(self._t.get(conversation_id) or [])
        msgs.append({"role": message.role, "content": message.content,
                     "tool_name": message.tool_name, "tool_call_id": message.tool_call_id})
        self._t[conversation_id] = msgs[-self._max:]

    def history(self, conversation_id: str) -> List[ChatMessage]:
        return [ChatMessage(m["role"], m["content"], m.get("tool_name"), m.get("tool_call_id"))
                for m in (self._t.get(conversation_id) or [])]

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
        return [cid for cid in self._a.keys() if (self._a.get(cid) or {}).get("__owner__") == user_id]

    def clear(self, conversation_id: str) -> None:
        self._t.pop(conversation_id, None)
        self._a.pop(conversation_id, None)


# ---- the Faust app: one agent per conversation key ----

if faust is not None:
    app = faust.App("agentic-faust", broker="kafka://localhost:9092", store="rocksdb://")

    class RequestRecord(faust.Record, serializer="json"):
        conversation_id: str
        user_id: str = "anonymous"
        text: str = ""

    requests_topic = app.topic("agentic.requests", value_type=RequestRecord)
    replies_topic = app.topic("agentic.replies", value_type=bytes)

    transcripts = app.Table("agentic.transcripts", default=list, partitions=8)
    attrs = app.Table("agentic.attrs", default=dict, partitions=8)

    _store = FaustTableConversationStore(transcripts, attrs)
    _hot = InMemoryHotVectorIndex()
    seed_kb(_hot)
    _retriever = TwoTierRetriever(_hot, None, 4, 4)
    _graph = build_banking_graph()
    _tools = default_tools()

    @app.agent(requests_topic)
    async def banking_agent(stream):
        """A Faust agent == our keyed agent. Faust routes each conversation to the
        same partition/worker, so state access is single-writer; we run the
        router->path->verifier graph and publish the reply."""
        from pyagentic.core import AgentContext

        async for req in stream.group_by(RequestRecord.conversation_id):
            ctx = AgentContext(
                conversation_id=req.conversation_id, user_id=req.user_id,
                store=_store, state=None, tools=_tools, retriever=_retriever)
            result = _graph.handle(Event(req.conversation_id, req.text, req.user_id), ctx)
            await replies_topic.send(value=result.reply.encode("utf-8"))
else:
    app = None  # importable for inspection/tests without faust installed


if __name__ == "__main__":  # pragma: no cover
    if faust is None:
        print("faust-streaming not installed. `pip install faust-streaming` and run a Kafka broker,")
        print("then: faust -A agentic_faust:app worker -l info")
    else:
        app.main()

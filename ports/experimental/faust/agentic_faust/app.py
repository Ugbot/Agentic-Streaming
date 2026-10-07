"""The Faust ``App`` for the banking agent: ``faust -A agentic_faust.app worker -l info``.

Needs the optional ``faust`` extra and a Kafka broker (``AGENTIC_FAUST_BROKER``, default
``kafka://localhost:9092``). Table storage is ``AGENTIC_FAUST_STORE`` (default ``memory://``,
recovered from the changelog topic; ``rocksdb://`` with ``faust-streaming[rocksdb]``).
"""

from __future__ import annotations

import json
import os

from agentic_faust import FaustTableConversationStore, handle_turn, result_to_dict

try:
    import faust
except ImportError as exc:
    raise ImportError(
        "agentic-faust needs the optional 'faust' extra: pip install 'agentic-faust[faust]'"
    ) from exc

__all__ = ["RequestRecord", "app", "banking_agent", "replies_topic", "requests_topic", "store"]

app = faust.App(
    "agentic-faust",
    broker=os.environ.get("AGENTIC_FAUST_BROKER", "kafka://localhost:9092"),
    store=os.environ.get("AGENTIC_FAUST_STORE", "memory://"),
)


class RequestRecord(faust.Record, serializer="json"):
    conversation_id: str
    user_id: str = "anonymous"
    text: str = ""


requests_topic = app.topic("agentic.requests", key_type=str, value_type=RequestRecord)
replies_topic = app.topic("agentic.replies", key_type=str, value_type=bytes)

transcripts = app.Table("agentic.transcripts", default=list, partitions=8)
attrs = app.Table("agentic.attrs", default=dict, partitions=8)

# Durable per-conversation store over the two Tables; graph/tools/retriever come from
# the injectable deps in agentic_faust (override via agentic_faust.configure()).
store = FaustTableConversationStore(transcripts, attrs)


@app.agent(requests_topic)
async def banking_agent(stream):
    """A Faust agent is our keyed agent. Faust routes each conversation to the same
    partition/worker, so state access is single-writer; we run the
    router->path->verifier graph and publish the reply keyed by conversation."""
    async for req in stream.group_by(RequestRecord.conversation_id):
        result = handle_turn(req.conversation_id, req.user_id, req.text, store)
        payload = json.dumps(result_to_dict(result)).encode("utf-8")
        await replies_topic.send(key=req.conversation_id, value=payload)

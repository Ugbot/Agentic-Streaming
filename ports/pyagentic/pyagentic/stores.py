"""Pluggable external stores — hot-swap the durable backing of a ConversationStore
behind the same SPI. The agent logic never changes; only the YAML ``stores:`` section
(or env) picks an implementation + its connection link, and Docker/Podman Compose
brings the service up.

``InMemoryConversationStore`` is the default (process-local). ``RedisConversationStore``
(works against Redis or Valkey) is the canonical external example: same interface,
durable + shared across workers/processes. Add Postgres/Fluss/etc. the same way — a new
class implementing ``ConversationStore`` + a registry entry.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from .memory import ChatMessage, ConversationStore, InMemoryConversationStore

try:
    import redis as _redis  # redis-py also speaks to Valkey
except ImportError:  # keep importable without the client
    _redis = None


class RedisConversationStore(ConversationStore):
    """A ConversationStore backed by Redis/Valkey: transcript as a bounded list, scalar
    attributes as a hash, a per-user set for the reverse index. Same SPI as the
    in-memory store, so it's a drop-in hot-swap."""

    def __init__(self, url: str = "redis://localhost:6379/0", max_messages: int = 200, prefix: str = "agentic"):
        if _redis is None:
            raise RuntimeError("redis-py not installed: pip install redis")
        self._r = _redis.from_url(url, decode_responses=True)
        self._max = max(1, max_messages)
        self._p = prefix

    def _msgs(self, cid: str) -> str:
        return f"{self._p}:conv:{cid}:msgs"

    def _attrs(self, cid: str) -> str:
        return f"{self._p}:conv:{cid}:attrs"

    def _user(self, uid: str) -> str:
        return f"{self._p}:user:{uid}:convs"

    def append(self, conversation_id: str, message: ChatMessage) -> None:
        key = self._msgs(conversation_id)
        self._r.rpush(key, json.dumps(
            {"role": message.role, "content": message.content,
             "tool_name": message.tool_name, "tool_call_id": message.tool_call_id}))
        self._r.ltrim(key, -self._max, -1)

    def history(self, conversation_id: str) -> List[ChatMessage]:
        out = []
        for raw in self._r.lrange(self._msgs(conversation_id), 0, -1):
            d = json.loads(raw)
            out.append(ChatMessage(d["role"], d["content"], d.get("tool_name"), d.get("tool_call_id")))
        return out

    def message_count(self, conversation_id: str) -> int:
        return int(self._r.llen(self._msgs(conversation_id)))

    def put_attribute(self, conversation_id: str, key: str, value: str) -> None:
        self._r.hset(self._attrs(conversation_id), key, value)

    def get_attribute(self, conversation_id: str, key: str) -> Optional[str]:
        return self._r.hget(self._attrs(conversation_id), key)

    def attributes(self, conversation_id: str) -> Dict[str, str]:
        return {k: v for k, v in self._r.hgetall(self._attrs(conversation_id)).items() if not k.startswith("__")}

    def associate_user(self, conversation_id: str, user_id: str) -> None:
        prior = self._r.hget(self._attrs(conversation_id), "__owner__")
        if prior and prior != user_id:
            self._r.srem(self._user(prior), conversation_id)
        self._r.hset(self._attrs(conversation_id), "__owner__", user_id)
        self._r.sadd(self._user(user_id), conversation_id)

    def conversations_for_user(self, user_id: str) -> List[str]:
        return sorted(self._r.smembers(self._user(user_id)))

    def clear(self, conversation_id: str) -> None:
        owner = self._r.hget(self._attrs(conversation_id), "__owner__")
        if owner:
            self._r.srem(self._user(owner), conversation_id)
        self._r.delete(self._msgs(conversation_id), self._attrs(conversation_id))


def make_conversation_store(spec: Optional[Dict[str, Any]]) -> ConversationStore:
    """Build a ConversationStore from a ``{kind, url, ...}`` spec (the YAML ``stores.
    conversation`` section). ``kind: memory`` (default) | ``redis`` (Redis/Valkey).
    The ``url`` may be ``${ENV_VAR}`` to read a connection link from the environment."""
    spec = spec or {}
    kind = (spec.get("kind") or "memory").lower()
    if kind == "memory":
        return InMemoryConversationStore(max_messages=int(spec.get("max_messages", 200)))
    if kind in ("redis", "valkey"):
        url = _resolve(spec.get("url") or os.environ.get("AGENTIC_REDIS_URL") or "redis://localhost:6379/0")
        return RedisConversationStore(url=url, max_messages=int(spec.get("max_messages", 200)),
                                      prefix=spec.get("prefix", "agentic"))
    raise ValueError(f"unknown conversation store kind {kind!r}; choose memory|redis")


def _resolve(value: str) -> str:
    """Expand a ``${ENV}`` connection link."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.environ.get(value[2:-1], "")
    return value

"""Per-conversation memory — the portable analogue of the Java ``ConversationStore``.

This is the single most important abstraction in the port: durable, per-key state
shared across operators and turns, keyed by ``conversation_id`` and indexable by
``user_id``. In Flink this rode on keyed state; here it's a plain SPI with swappable
backends so every engine adapter (Faust Table, Ray actor field, Redis, …) can plug in.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import RLock
from typing import Deque, Dict, List, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class ChatMessage:
    """A single transcript entry. ``role`` is one of system|user|assistant|tool."""

    role: str
    content: str
    tool_name: Optional[str] = None
    tool_call_id: Optional[str] = None

    @staticmethod
    def user(text: str) -> "ChatMessage":
        return ChatMessage("user", text)

    @staticmethod
    def assistant(text: str) -> "ChatMessage":
        return ChatMessage("assistant", text)

    @staticmethod
    def system(text: str) -> "ChatMessage":
        return ChatMessage("system", text)

    @staticmethod
    def tool(call_id: str, name: str, content: str) -> "ChatMessage":
        return ChatMessage("tool", content, tool_name=name, tool_call_id=call_id)


@runtime_checkable
class ConversationStore(Protocol):
    """Multi-turn transcript + scalar workflow attributes, keyed by conversation id.

    Mirrors ``org.agentic.flink.memory.conversation.ConversationStore``. Attributes
    carry small scalars across operators/turns (routing phase, remote contextId);
    the transcript is the bounded message history. ``user_id`` indexing supports
    "all conversations for a user".
    """

    def append(self, conversation_id: str, message: ChatMessage) -> None: ...

    def history(self, conversation_id: str) -> List[ChatMessage]: ...

    def message_count(self, conversation_id: str) -> int: ...

    def put_attribute(self, conversation_id: str, key: str, value: str) -> None: ...

    def get_attribute(self, conversation_id: str, key: str) -> Optional[str]: ...

    def attributes(self, conversation_id: str) -> Dict[str, str]: ...

    def associate_user(self, conversation_id: str, user_id: str) -> None: ...

    def conversations_for_user(self, user_id: str) -> List[str]: ...

    def clear(self, conversation_id: str) -> None: ...


@dataclass
class _Convo:
    messages: Deque[ChatMessage]
    attributes: Dict[str, str] = field(default_factory=dict)
    owner: Optional[str] = None


class InMemoryConversationStore:
    """Process-local, thread-safe ``ConversationStore`` — the embedded default.

    Bounded transcript per conversation (LRU on age). For a distributed deployment
    swap in a Redis/Fluss-backed implementation behind the same Protocol; the agent
    logic doesn't change.
    """

    def __init__(self, max_messages: int = 200) -> None:
        self._max = max(1, max_messages)
        self._convos: Dict[str, _Convo] = {}
        self._user_index: Dict[str, List[str]] = {}
        self._lock = RLock()

    def _convo(self, cid: str) -> _Convo:
        c = self._convos.get(cid)
        if c is None:
            c = _Convo(messages=deque(maxlen=self._max))
            self._convos[cid] = c
        return c

    def append(self, conversation_id: str, message: ChatMessage) -> None:
        with self._lock:
            self._convo(conversation_id).messages.append(message)

    def history(self, conversation_id: str) -> List[ChatMessage]:
        with self._lock:
            c = self._convos.get(conversation_id)
            return list(c.messages) if c else []

    def message_count(self, conversation_id: str) -> int:
        with self._lock:
            c = self._convos.get(conversation_id)
            return len(c.messages) if c else 0

    def put_attribute(self, conversation_id: str, key: str, value: str) -> None:
        with self._lock:
            self._convo(conversation_id).attributes[key] = value

    def get_attribute(self, conversation_id: str, key: str) -> Optional[str]:
        with self._lock:
            c = self._convos.get(conversation_id)
            return c.attributes.get(key) if c else None

    def attributes(self, conversation_id: str) -> Dict[str, str]:
        with self._lock:
            c = self._convos.get(conversation_id)
            return dict(c.attributes) if c else {}

    def associate_user(self, conversation_id: str, user_id: str) -> None:
        with self._lock:
            prior = self._convo(conversation_id).owner
            if prior and prior != user_id:
                self._user_index.get(prior, []).remove(conversation_id) if conversation_id in self._user_index.get(
                    prior, []
                ) else None
            self._convo(conversation_id).owner = user_id
            ids = self._user_index.setdefault(user_id, [])
            if conversation_id not in ids:
                ids.append(conversation_id)

    def conversations_for_user(self, user_id: str) -> List[str]:
        with self._lock:
            return list(self._user_index.get(user_id, []))

    def clear(self, conversation_id: str) -> None:
        with self._lock:
            c = self._convos.pop(conversation_id, None)
            if c and c.owner:
                ids = self._user_index.get(c.owner, [])
                if conversation_id in ids:
                    ids.remove(conversation_id)


@runtime_checkable
class KeyedStateStore(Protocol):
    """The portable form of Flink keyed ``ValueState`` — per-(key,name) scalar slot.

    Engine adapters back this with their native keyed state (Kafka Streams
    ``KeyValueStore``, Faust ``Table``, a Ray actor field) or Redis. The
    ``ConversationStore`` is the richer, transcript-aware sibling; this is the
    minimal get/put used for things like a per-conversation routing budget.
    """

    def get(self, key: str, name: str) -> Optional[object]: ...

    def put(self, key: str, name: str, value: object) -> None: ...

    def clear(self, key: str) -> None: ...


class InMemoryKeyedStateStore:
    """Process-local ``KeyedStateStore``."""

    def __init__(self) -> None:
        self._d: Dict[str, Dict[str, object]] = {}
        self._lock = RLock()

    def get(self, key: str, name: str) -> Optional[object]:
        with self._lock:
            return self._d.get(key, {}).get(name)

    def put(self, key: str, name: str, value: object) -> None:
        with self._lock:
            self._d.setdefault(key, {})[name] = value

    def clear(self, key: str) -> None:
        with self._lock:
            self._d.pop(key, None)

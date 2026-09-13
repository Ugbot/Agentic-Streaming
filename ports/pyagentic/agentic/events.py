"""The event log: the source of truth for a conversation. State is a fold over it."""

from __future__ import annotations

import json
import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence

from .errors import ValidationError

EVENT_TYPES: FrozenSet[str] = frozenset({
    "turn_received", "guardrail_rejected", "routed", "brain_started", "tool_called",
    "tool_failed", "retrieved", "reply_drafted", "verification_failed", "memory_written",
    "turn_completed", "turn_failed", "turn_suspended", "turn_resumed", "timer_scheduled",
    "timer_fired", "compensation_started", "compensation_step", "compensation_completed",
    "delegated",
})

TERMINAL_STATUSES: FrozenSet[str] = frozenset({
    "completed", "rejected", "unverified", "failed", "suspended", "duplicate",
})


@dataclass(frozen=True)
class Event:
    """One immutable record of a conversation log (`spec/v1/primitives.md`, Event)."""

    conversation_id: str
    turn_id: str
    sequence: int
    type: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    timestamp: int = 0
    metadata: Mapping[str, str] = field(default_factory=dict)

    def normalized(self) -> Dict[str, Any]:
        """The shape `spec/v1/result.schema.json` uses for `events[]`."""
        return {"type": self.type, "sequence": self.sequence, "payload": dict(self.payload)}


@dataclass(frozen=True)
class Turn:
    """One inbound request. `turn_id` is the idempotency key and is mandatory.

    A `signal` resumes the suspended turn with the same `turn_id` instead of starting one.
    """

    conversation_id: str
    turn_id: str
    text: str = ""
    user_id: str = "anonymous"
    signal: Optional[Mapping[str, Any]] = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.conversation_id:
            raise ValidationError("a turn needs a non-empty conversation_id")
        if not self.turn_id:
            raise ValidationError("a turn needs a non-empty turn_id; it is the idempotency key")


@dataclass(frozen=True)
class ChatMessage:
    role: str
    text: str


def reduce_state(log: Iterable[Event]) -> Dict[str, Any]:
    """The only definition of conversation state. Pure, total, and ignores unknown types."""
    state: Dict[str, Any] = {"turn_count": 0, "transcript_length": 0}
    for event in log:
        if event.type == "turn_received":
            state["turn_count"] += 1
        elif event.type == "memory_written":
            state["transcript_length"] += len(event.payload.get("messages", ()))
        elif event.type == "retrieved":
            state["last_retrieved_ids"] = list(event.payload.get("ids", ()))
    return state


def transcript(log: Iterable[Event]) -> List[ChatMessage]:
    """The conversation memory as a fold over `memory_written` events."""
    messages: List[ChatMessage] = []
    for event in log:
        if event.type == "memory_written":
            for message in event.payload.get("messages", ()):
                messages.append(ChatMessage(role=str(message["role"]), text=str(message["text"])))
    return messages


class EventLog(ABC):
    """Append-only, per-conversation ordered log. `append` assigns the dense sequence."""

    @abstractmethod
    def append(self, conversation_id: str, turn_id: str, type: str,
               payload: Mapping[str, Any], timestamp: int,
               metadata: Optional[Mapping[str, str]] = None) -> Event: ...

    @abstractmethod
    def read(self, conversation_id: str) -> Sequence[Event]: ...

    @abstractmethod
    def conversation_ids(self) -> Sequence[str]: ...


class InMemoryEventLog(EventLog):
    """Process-lifetime log. It outlives a runtime restart in the same process, which is
    what `replay` and `suspend_resume` need, but it is not durability across a crash."""

    def __init__(self) -> None:
        self._logs: Dict[str, List[Event]] = {}
        self._guard = threading.Lock()

    def append(self, conversation_id: str, turn_id: str, type: str,
               payload: Mapping[str, Any], timestamp: int,
               metadata: Optional[Mapping[str, str]] = None) -> Event:
        if type not in EVENT_TYPES:
            raise ValidationError(f"unknown event type {type!r}; v1 event types are a closed set")
        with self._guard:
            log = self._logs.setdefault(conversation_id, [])
            event = Event(conversation_id, turn_id, len(log), type, dict(payload),
                          timestamp, dict(metadata or {}))
            log.append(event)
            return event

    def read(self, conversation_id: str) -> Sequence[Event]:
        with self._guard:
            return tuple(self._logs.get(conversation_id, ()))

    def conversation_ids(self) -> Sequence[str]:
        with self._guard:
            return tuple(self._logs)


class FileEventLog(EventLog):
    """One JSON-lines file per conversation under `directory`. Survives the process.

    Reads happen once per conversation and are then served from memory; appends write
    the line and `fsync` before returning, so an acknowledged event is on disk. Unknown
    event types found on disk are preserved (they stay in the file and the in-memory
    sequence) but rejected on append, because the v1 emitted set is closed.
    """

    def __init__(self, directory: str) -> None:
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self._cache: Dict[str, List[Event]] = {}
        self._guard = threading.Lock()

    def _path(self, conversation_id: str) -> str:
        safe = "".join(c if c.isalnum() or c in "-_." else f"%{ord(c):02x}" for c in conversation_id)
        return os.path.join(self.directory, safe + ".jsonl")

    def _load(self, conversation_id: str) -> List[Event]:
        log = self._cache.get(conversation_id)
        if log is None:
            log = []
            path = self._path(conversation_id)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        if line.strip():
                            rec = json.loads(line)
                            log.append(Event(conversation_id, rec["turn_id"], rec["sequence"], rec["type"],
                                             rec.get("payload") or {}, rec.get("timestamp", 0),
                                             rec.get("metadata") or {}))
            self._cache[conversation_id] = log
        return log

    def append(self, conversation_id: str, turn_id: str, type: str,
               payload: Mapping[str, Any], timestamp: int,
               metadata: Optional[Mapping[str, str]] = None) -> Event:
        if type not in EVENT_TYPES:
            raise ValidationError(f"unknown event type {type!r}; v1 event types are a closed set")
        with self._guard:
            log = self._load(conversation_id)
            event = Event(conversation_id, turn_id, len(log), type, dict(payload),
                          timestamp, dict(metadata or {}))
            record = {"turn_id": turn_id, "sequence": event.sequence, "type": type,
                      "payload": event.payload, "timestamp": timestamp, "metadata": event.metadata}
            with open(self._path(conversation_id), "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, sort_keys=True) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            log.append(event)
            return event

    def read(self, conversation_id: str) -> Sequence[Event]:
        with self._guard:
            return tuple(self._load(conversation_id))

    def conversation_ids(self) -> Sequence[str]:
        with self._guard:
            ids = set(self._cache)
            for name in os.listdir(self.directory):
                if name.endswith(".jsonl"):
                    ids.add(_unescape(name[:-6]))
            return tuple(sorted(ids))


def _unescape(name: str) -> str:
    out = []
    i = 0
    while i < len(name):
        if name[i] == "%" and len(name) >= i + 3:
            out.append(chr(int(name[i + 1:i + 3], 16)))
            i += 3
        else:
            out.append(name[i])
            i += 1
    return "".join(out)

"""Backend registry: `local`, `CoreBackend` on the `agentic` local runtime, is the only backend. The
experimental adapters (celery, nats, ray and the others) have moved out of this repository; the code as
it stood is at tag ``pre-cornerstone``. Any other name raises ``ValueError`` saying so.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

from agentic.events import Turn
from agentic.runtime import LocalRuntime


@dataclass
class Event:
    """One inbound message. A redelivery carries the same `turn_id`; an event without one is a new
    turn and gets its own, since the caller has said it will never redeliver it."""

    conversation_id: str
    text: str
    user_id: str = "anonymous"
    metadata: Dict[str, str] = field(default_factory=dict)
    turn_id: Optional[str] = None
    signal: Optional[Mapping[str, Any]] = None


@dataclass
class TurnResult:
    """A normalized result (`spec/v1/result.schema.json`) with the fields the CLI prints. `result`
    is the whole of it; `derived_turns` are the turns a `cep` pattern injected after it."""

    conversation_id: str
    reply: Optional[str]
    path: Optional[str] = None
    ok: bool = True
    tool_calls: List[str] = field(default_factory=list)
    status: str = "completed"
    error: Optional[Mapping[str, Any]] = None
    result: Mapping[str, Any] = field(default_factory=dict)
    derived_turns: List[Mapping[str, Any]] = field(default_factory=list)


class CoreBackend:
    """The `agentic` local runtime: the conformance semantics, on the event log and the journal."""

    name = "local"
    capability = "online"

    def __init__(self, document: Mapping[str, Any], store_dir: Optional[str] = None) -> None:
        self.runtime = LocalRuntime(store_dir=store_dir)
        self.runtime.deploy(document)

    def submit(self, event: Any) -> TurnResult:
        turn = Turn(event.conversation_id, getattr(event, "turn_id", None) or uuid.uuid4().hex, event.text,
                    getattr(event, "user_id", "anonymous"), getattr(event, "signal", None),
                    dict(getattr(event, "metadata", None) or {}))
        result = self.runtime.submit(turn)
        error = result.get("error")
        derived = list((result.get("runtime_detail") or {}).get("derived_turns") or [])
        return TurnResult(result["conversation_id"], result.get("reply"), result.get("path"),
                          result["status"] in ("completed", "duplicate") and error is None,
                          [call["tool"] for call in result.get("tool_calls", []) if "error" not in call],
                          result["status"], error, result, derived)

    @property
    def tools(self) -> Any:
        return self.runtime.tools

    def close(self) -> None:
        self.runtime.close()


_BACKENDS: Dict[str, Callable[..., CoreBackend]] = {
    "local": CoreBackend,
}


def resolve(name: Optional[str]) -> str:
    """The registered backend `name` means. Any other name raises ``ValueError`` with the registered
    names and where the adapters went."""
    key = (name or "local").strip().lower()
    if key not in _BACKENDS:
        raise ValueError(
            f"unknown backend {key!r}; supported backends: {', '.join(backend_names())}. "
            "The experimental adapters (celery, nats, ray and the others) have moved out of "
            "this repository; the old code is at tag pre-cornerstone")
    return key


def backend_names():
    return sorted(_BACKENDS)

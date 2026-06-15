"""Human-in-the-loop suspend/resume — the portable Phase-6 pattern.

A :class:`HumanGate` wraps a :class:`~pyagentic.runtime.Runtime`. A turn that
``needs_approval`` suspends instead of completing; a later :meth:`HumanGate.resume`
(CQRS: resume is just another command) replays the held turn (approved) or denies
it. :meth:`HumanGate.check_timeouts` escalates suspensions that age past the
timeout — the portable "approve within N minutes or escalate", composing with the
Phase-2 timer service.

Mirrors the Java ``org.jagentic.core.suspend`` package at behavioral parity.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Callable, Dict, List, Optional, Protocol

from .core import Event, TurnResult
from .runtime import Runtime


@dataclass(frozen=True)
class Suspension:
    """A turn held awaiting external input (human approval, an async result).

    ``pending_text`` is the held turn's text, replayed on resume; ``since`` is when
    it suspended (for timeouts).
    """

    conversation_id: str
    reason: str
    pending_text: str
    since: int


class SuspensionService(Protocol):
    """Tracks which conversations are suspended awaiting input. The in-memory default
    is process-local; a durable impl can persist via a keyed-state SPI so a suspended
    turn survives restart."""

    def suspend(self, conversation_id: str, reason: str, pending_text: str, now: int) -> None: ...

    def is_suspended(self, conversation_id: str) -> bool: ...

    def peek(self, conversation_id: str) -> Optional[Suspension]: ...

    def clear(self, conversation_id: str) -> Optional[Suspension]: ...

    def all_pending(self) -> List[Suspension]: ...


class InMemorySuspensionService:
    """Process-local, thread-safe :class:`SuspensionService`."""

    def __init__(self) -> None:
        self._by_key: Dict[str, Suspension] = {}
        self._lock = Lock()

    def suspend(self, conversation_id: str, reason: str, pending_text: str, now: int) -> None:
        with self._lock:
            self._by_key[conversation_id] = Suspension(conversation_id, reason, pending_text, now)

    def is_suspended(self, conversation_id: str) -> bool:
        with self._lock:
            return conversation_id in self._by_key

    def peek(self, conversation_id: str) -> Optional[Suspension]:
        with self._lock:
            return self._by_key.get(conversation_id)

    def clear(self, conversation_id: str) -> Optional[Suspension]:
        """Remove and return the pending suspension (the resume command)."""
        with self._lock:
            return self._by_key.pop(conversation_id, None)

    def all_pending(self) -> List[Suspension]:
        """All currently-suspended conversations (for timeout sweeps)."""
        with self._lock:
            return list(self._by_key.values())


class HumanGate:
    """A human-in-the-loop gate around a :class:`~pyagentic.runtime.Runtime`."""

    def __init__(
        self,
        runtime: Runtime,
        suspensions: SuspensionService,
        needs_approval: Callable[[Event], bool],
        timeout_millis: int = 0,
    ) -> None:
        self.runtime = runtime
        self.suspensions = suspensions
        self.needs_approval = needs_approval
        self.timeout_millis = timeout_millis

    def submit(self, event: Event, now: int) -> TurnResult:
        """Submit through the gate. Suspends (awaiting approval) when required;
        otherwise runs a normal turn."""
        cid = event.conversation_id
        if self.suspensions.is_suspended(cid):
            return TurnResult(
                cid,
                "[awaiting-approval] a turn is already pending approval",
                path="awaiting-approval",
                ok=False,
            )
        if self.needs_approval(event):
            self.suspensions.suspend(cid, "approval required: " + event.text, event.text, now)
            return TurnResult(
                cid,
                "[awaiting-approval] " + event.text,
                path="awaiting-approval",
                ok=False,
            )
        return self.runtime.submit(event)

    def resume(self, conversation_id: str, approved: bool, now: int) -> TurnResult:
        """Resume a suspended conversation: approved → replay the held turn;
        denied → report."""
        pending = self.suspensions.clear(conversation_id)
        if pending is None:
            return TurnResult(conversation_id, "[resume] nothing pending", path="resume", ok=False)
        if not approved:
            return TurnResult(conversation_id, "[denied] " + pending.reason, path="denied", ok=False)
        # Replay the held turn as a fresh (metadata-free, so it won't re-trigger the gate) event.
        return self.runtime.submit(
            Event(conversation_id=conversation_id, text=pending.pending_text, user_id="system")
        )

    def check_timeouts(self, now: int) -> List[TurnResult]:
        """Escalate (and clear) suspensions older than the timeout; one result per
        escalated conversation."""
        out: List[TurnResult] = []
        if self.timeout_millis <= 0:
            return out
        for s in self.suspensions.all_pending():
            if now - s.since > self.timeout_millis:
                self.suspensions.clear(s.conversation_id)
                out.append(
                    TurnResult(
                        s.conversation_id,
                        "[escalated] approval timed out: " + s.reason,
                        path="escalated",
                        ok=False,
                    )
                )
        return out

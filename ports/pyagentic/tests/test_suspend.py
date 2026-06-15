"""Phase 6: human-in-the-loop suspend/resume. A turn that needs approval suspends
instead of completing; resume replays the held turn (approved) or denies it; an aged
suspension escalates on a timeout sweep. Mirrors the Java ``suspend`` package goldens.
"""

from __future__ import annotations

from pyagentic import (
    Event,
    HumanGate,
    InMemoryHotVectorIndex,
    InMemorySuspensionService,
    LocalRuntime,
    TwoTierRetriever,
)
from pyagentic.banking import build_banking_graph, default_tools, seed_kb


def _banking() -> LocalRuntime:
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retriever = TwoTierRetriever(hot, None, 4, 4)
    return LocalRuntime(build_banking_graph(), tools=default_tools(), retriever=retriever)


def _needs_approval(event: Event) -> bool:
    return "needs_approval" in event.metadata


def _approval_event() -> Event:
    return Event(
        conversation_id="c1",
        text="what is my balance?",
        user_id="alice",
        metadata={"needs_approval": "true"},
    )


def test_approval_required_submit_suspends() -> None:
    suspensions = InMemorySuspensionService()
    gate = HumanGate(_banking(), suspensions, _needs_approval)

    result = gate.submit(_approval_event(), 0)

    assert result.path == "awaiting-approval"
    assert result.ok is False
    assert suspensions.is_suspended("c1") is True


def test_resume_approved_replays_held_turn() -> None:
    suspensions = InMemorySuspensionService()
    gate = HumanGate(_banking(), suspensions, _needs_approval)
    gate.submit(_approval_event(), 0)

    result = gate.resume("c1", True, 10)

    assert result.path == "payments"
    assert "1234.56" in result.reply
    assert suspensions.is_suspended("c1") is False


def test_resume_denied() -> None:
    suspensions = InMemorySuspensionService()
    gate = HumanGate(_banking(), suspensions, _needs_approval)
    gate.submit(_approval_event(), 0)

    result = gate.resume("c1", False, 10)

    assert result.path == "denied"
    assert suspensions.is_suspended("c1") is False


def test_normal_submit_passes_through() -> None:
    suspensions = InMemorySuspensionService()
    gate = HumanGate(_banking(), suspensions, _needs_approval)

    result = gate.submit(Event("c1", "what is my balance?", "alice"), 0)

    assert result.path == "payments"
    assert result.ok is True
    assert suspensions.is_suspended("c1") is False


def test_timeout_escalates() -> None:
    suspensions = InMemorySuspensionService()
    gate = HumanGate(_banking(), suspensions, _needs_approval, timeout_millis=1000)
    gate.submit(_approval_event(), 0)

    assert gate.check_timeouts(500) == []

    escalated = gate.check_timeouts(2000)
    assert len(escalated) == 1
    assert escalated[0].path == "escalated"
    assert suspensions.is_suspended("c1") is False


def test_resume_nothing_pending() -> None:
    suspensions = InMemorySuspensionService()
    gate = HumanGate(_banking(), suspensions, _needs_approval)

    result = gate.resume("c1", True, 10)

    assert result.path == "resume"
    assert result.ok is False

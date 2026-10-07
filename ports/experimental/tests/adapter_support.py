"""Shared helpers for the Python adapter tests under ports/experimental.

The adapters are installed packages (``pip install -e ports/experimental/<engine>``), so
nothing here touches ``sys.path``. Engine-backed tests carry the engine's marker (see
../pytest.ini) and skip with a printed reason only when the adapter package or its engine
is not importable, or when a server it needs (NATS JetStream) is not reachable.
"""

from __future__ import annotations

import importlib
import random
import string

import pytest
from pyagentic.banking import build_banking_graph, default_tools
from pyagentic.core import Agent, AgentContext, Event, RoutedGraph
from pyagentic.tools import ToolRegistry


def import_adapter(package: str):
    """Import an adapter package or skip the test with the adapter's own install hint."""
    try:
        return importlib.import_module(package)
    except ImportError as exc:
        pytest.skip(f"{package} not importable: {exc}")


def random_conversation_id(prefix: str) -> str:
    return prefix + "-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


# Representative texts per banking path; tests draw from these so no run depends on one phrasing.
CARD_TEXTS = [
    "what card types do you offer?",
    "tell me about crypto cash-back",
    "can I get a platinum card?",
    "how does cashback work on my card?",
]
BALANCE_TEXTS = [
    "what is my balance?",
    "show me my account balance please",
    "I want to check my balance",
]
GENERAL_TEXTS = [
    "hello there",
    "where is the nearest branch?",
    "what are your opening hours?",
]
FRAUD_TEXTS = [
    "my card was stolen, please freeze it",
    "I think there is fraud on my account",
    "freeze my card right now",
]


def pick(texts):
    return random.choice(texts)


class FraudBrain:
    """A brain added purely from the public Brain protocol; it calls a new tool."""

    def turn(self, user_text: str, ctx: AgentContext) -> str:
        ref = ctx.call_tool("freeze_card", {"user": ctx.user_id})
        return f"[fraud] Your card is frozen (ref {ref}). A specialist will call you."


def extended_tools() -> ToolRegistry:
    """The core's default tools plus a new one, registered through the public API."""
    reg = default_tools()
    reg.register("freeze_card", "Freeze the user's card", lambda p: f"FRZ-{p['user']}")
    return reg


def extended_graph() -> RoutedGraph:
    """The banking graph plus a new 'fraud' path whose router takes precedence."""
    base = build_banking_graph()
    paths = dict(base.paths)
    paths["fraud"] = Agent("fraud", "You handle fraud and stolen cards.", FraudBrain())

    def router(event: Event, ctx: AgentContext) -> str:
        low = event.text.lower()
        if "stolen" in low or "fraud" in low or "freeze" in low:
            return "fraud"
        return base.router(event, ctx)

    return RoutedGraph(router=router, paths=paths, verifier=base.verifier)

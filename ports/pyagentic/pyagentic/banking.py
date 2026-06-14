"""The banking ``router -> path -> verifier`` worked example, engine-agnostic.

A faithful, model-free subset of the Java ``BankingAgentGraph``: a rule router
classifies the request into a path (cards / payments / general), each path is an
agent whose deterministic brain may call a tool and/or retrieve from the hot+cold
KB, and a verifier sanity-checks the reply. Every engine adapter reuses this to
demonstrate the same workflow on its runtime.
"""

from __future__ import annotations

from typing import Tuple

from .core import Agent, AgentContext, Event, RoutedGraph
from .retrieval import hashing_embedder
from .tools import ToolRegistry

_EMBED = hashing_embedder(64)

# A tiny "knowledge base" the cards path retrieves from (hot+cold via the runtime's retriever).
KB = {
    "kb_cards_types": "We offer three card types: classic, gold, and platinum, each with different fees.",
    "kb_cards_crypto": "Crypto cash-back can be redeemed to a linked wallet or a manual address.",
    "kb_payments_limits": "Daily transfer limits are 10,000 by default; raise them in settings.",
    "kb_payments_dispute": "To dispute a charge, open the transaction and tap Dispute within 60 days.",
}


def seed_kb(tools_or_index) -> None:
    """Populate a HotVectorIndex (the cold-equivalent here) with the KB."""
    for doc_id, text in KB.items():
        tools_or_index.upsert(doc_id, _EMBED(text), text)


class RuleBrain:
    """Deterministic brain: keyword rules that optionally call a tool / retrieve.
    Stands in for an LLM ReAct loop so the port runs and is testable with no model.
    """

    def __init__(self, name: str, keywords: tuple[str, ...] = ()) -> None:
        self.name = name
        self.keywords = keywords

    def turn(self, user_text: str, ctx: AgentContext) -> str:
        low = user_text.lower()
        # Tool example: balance lookup.
        if "balance" in low:
            bal = ctx.call_tool("get_balance", {"user": ctx.user_id})
            return f"[{self.name}] Your balance is {bal}."
        # Retrieval example (cards/payments knowledge).
        if ctx.retriever is not None:
            hits = ctx.retriever.retrieve(_EMBED(user_text), k=1)
            if hits and hits[0].score > 0.15:
                return f"[{self.name}] {hits[0].text}"
        return f"[{self.name}] I can help with {self.name} questions. You said: {user_text!r}"


def banking_router(event: Event, ctx: AgentContext) -> str:
    low = event.text.lower()
    if any(w in low for w in ("card", "crypto", "cash-back", "cashback")):
        return "cards"
    if any(w in low for w in ("transfer", "payment", "dispute", "charge", "limit", "balance")):
        return "payments"
    return "general"


def banking_verifier(reply: str, ctx: AgentContext) -> Tuple[bool, str]:
    # Trivial sanity check: a non-empty reply that names its path is "ok".
    ok = bool(reply) and reply.startswith("[")
    return ok, reply if ok else reply + " (unverified)"


def build_banking_graph() -> RoutedGraph:
    return RoutedGraph(
        router=banking_router,
        paths={
            "cards": Agent("cards", "You answer card questions.", RuleBrain("cards")),
            "payments": Agent("payments", "You answer payment questions.", RuleBrain("payments")),
            "general": Agent("general", "You answer general questions.", RuleBrain("general")),
        },
        verifier=banking_verifier,
    )


def default_tools() -> ToolRegistry:
    return ToolRegistry().register(
        "get_balance", "Look up the user's balance", lambda p: 1234.56
    )

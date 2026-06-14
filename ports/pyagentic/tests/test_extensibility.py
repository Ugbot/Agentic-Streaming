"""Extensibility guard for the pyagentic core.

The whole point of the port architecture is that the *core* is the single source of
truth: every engine adapter (Faust, Ray, Dask, Airflow, Celery, …) imports the same
abstractions and runs ``RoutedGraph.handle`` — it never re-implements routing, tools,
or retrieval. So a new tool / new path / new brain added through the public API must
"just work", and by construction every adapter then gets it for free.

These tests add a brand-new tool and a brand-new path using only the public
abstractions (no edits to the framework), and prove the core routes to it and invokes
it. The adapter-level counterpart (same extension, run through a real engine seam)
lives in ../../tests/test_adapters.py::test_celery_*.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.banking import build_banking_graph, default_tools  # noqa: E402
from pyagentic.core import Agent, AgentContext, Event, RoutedGraph  # noqa: E402
from pyagentic.memory import InMemoryConversationStore, InMemoryKeyedStateStore  # noqa: E402
from pyagentic.runtime import LocalRuntime  # noqa: E402
from pyagentic.tools import ToolRegistry  # noqa: E402


class FraudBrain:
    """A new brain added purely from the public Brain protocol — it calls a new tool."""

    def turn(self, user_text: str, ctx: AgentContext) -> str:
        ref = ctx.call_tool("freeze_card", {"user": ctx.user_id})
        return f"[fraud] Your card is frozen (ref {ref}). A specialist will call you."


def extended_tools() -> ToolRegistry:
    """The core's default tools PLUS a brand-new one — registered via the public API."""
    reg = default_tools()
    reg.register("freeze_card", "Freeze the user's card", lambda p: f"FRZ-{p['user']}")
    return reg


def extended_graph() -> RoutedGraph:
    """The banking graph PLUS a brand-new 'fraud' path, with a router that prefers it.
    Reuses the framework's RoutedGraph/Agent verbatim — no framework edits."""
    base = build_banking_graph()
    paths = dict(base.paths)
    paths["fraud"] = Agent("fraud", "You handle fraud and stolen cards.", FraudBrain())

    def router(event: Event, ctx: AgentContext) -> str:
        low = event.text.lower()
        if "stolen" in low or "fraud" in low or "freeze" in low:
            return "fraud"
        return base.router(event, ctx)  # delegate to the framework's router otherwise

    return RoutedGraph(router=router, paths=paths, verifier=base.verifier)


def _runtime() -> LocalRuntime:
    return LocalRuntime(
        extended_graph(),
        InMemoryConversationStore(),
        InMemoryKeyedStateStore(),
        extended_tools(),
        None,
    )


def test_new_path_and_tool_are_reachable_via_public_api():
    rt = _runtime()
    res = rt.submit(Event("c-fraud", "my card was stolen, please freeze it", "alice"))
    assert res.path == "fraud"
    assert res.ok
    assert "frozen" in res.reply.lower()
    assert "freeze_card" in res.tool_calls
    assert "FRZ-alice" in res.reply  # the new tool actually executed


def test_existing_paths_still_work_after_extension():
    rt = _runtime()
    cards = rt.submit(Event("c1", "what card types do you offer?", "bob"))
    pay = rt.submit(Event("c2", "what is my balance?", "bob"))
    general = rt.submit(Event("c3", "hello there", "bob"))
    assert (cards.path, pay.path, general.path) == ("cards", "payments", "general")
    assert "get_balance" in pay.tool_calls  # original tool untouched


def test_routed_phase_state_persisted_for_new_path():
    rt = _runtime()
    rt.submit(Event("c-fraud", "freeze my card", "carol"))
    # The framework persists the routed path/phase regardless of which path is new.
    assert rt.store.get_attribute("c-fraud", "graph.path") == "fraud"
    assert rt.store.get_attribute("c-fraud", "graph.phase") == "done"

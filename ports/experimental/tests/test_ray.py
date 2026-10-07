"""agentic-ray: one Ray actor per conversation on a local Ray cluster started in-process.

Ray is the engine and an optional extra; the test skips (with the adapter's install hint)
only when ``agentic_ray`` or ``ray`` is not importable. On this box Ray runs for real.

The cluster is started with this directory as its ``working_dir`` so actors can import
the module-level dependency factory below (Ray ships functions by module reference).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from adapter_support import (
    BALANCE_TEXTS,
    CARD_TEXTS,
    FRAUD_TEXTS,
    GENERAL_TEXTS,
    extended_graph,
    extended_tools,
    import_adapter,
    pick,
    random_conversation_id,
)
from pyagentic.core import Event

pytestmark = pytest.mark.ray


def fraud_deps():
    """Module-level so Ray can ship it to the actor process by reference."""
    return extended_graph(), extended_tools(), None


@pytest.fixture(scope="module")
def ra():
    adapter = import_adapter("agentic_ray")
    adapter.ray.init(
        namespace="agentic-tests",
        runtime_env={"working_dir": str(Path(__file__).parent)},
        ignore_reinit_error=True,
        log_to_driver=False,
    )
    yield adapter
    adapter.ray.shutdown()


def test_one_actor_per_conversation_orders_turns(ra):
    cid_a, cid_b = random_conversation_id("a"), random_conversation_id("b")
    with ra.RayRuntime(namespace=random_conversation_id("ns")) as rt:
        cards = rt.submit(Event(conversation_id=cid_a, text=pick(CARD_TEXTS), user_id="demo"))
        pay = rt.submit(Event(conversation_id=cid_a, text=pick(BALANCE_TEXTS), user_id="demo"))
        general = rt.submit(Event(conversation_id=cid_b, text=pick(GENERAL_TEXTS), user_id="demo"))
        assert (cards.path, pay.path, general.path) == ("cards", "payments", "general")
        assert "get_balance" in pay.tool_calls and "1234.56" in pay.reply
        assert rt.message_count(cid_a) == 4 and rt.message_count(cid_b) == 2
        assert rt._actor(cid_a) is rt._actor(cid_a)
        assert len(rt._actors) == 2
    assert rt._actors == {}


def test_namespaces_isolate_actors_with_different_dependencies(ra):
    cid = random_conversation_id("iso")
    with ra.RayRuntime(namespace=random_conversation_id("ns")) as banking, ra.RayRuntime(
        deps_factory=fraud_deps, namespace=random_conversation_id("ns")
    ) as fraud:
        text = pick(FRAUD_TEXTS)
        assert banking.submit(Event(conversation_id=cid, text=text, user_id="alice")).path != "fraud"
        res = fraud.submit(Event(conversation_id=cid, text=text, user_id="alice"))
        assert res.path == "fraud" and "freeze_card" in res.tool_calls and "FRZ-alice" in res.reply
        assert banking.message_count(cid) == 2 and fraud.message_count(cid) == 2


def test_ray_import_resolves_to_the_installed_distribution(ra):
    import ray

    assert "agentic_ray" not in ray.__file__
    assert ra.__file__.endswith("agentic_ray/__init__.py")

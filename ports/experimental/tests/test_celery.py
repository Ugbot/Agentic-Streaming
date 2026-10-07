"""agentic-celery: the banking graph as a Celery task, one queue per conversation bucket.

Celery runs here in eager mode (the task body executes in-process, no broker), which is
the same seam the pipeline ``backend: celery`` uses.
"""

from __future__ import annotations

import subprocess
import sys
import zlib

import pytest
from adapter_support import (
    BALANCE_TEXTS,
    CARD_TEXTS,
    FRAUD_TEXTS,
    GENERAL_TEXTS,
    import_adapter,
    pick,
    random_conversation_id,
)
from pyagentic.core import Event

pytestmark = pytest.mark.celery


@pytest.fixture
def cl():
    return import_adapter("agentic_celery")


def test_conversation_queue_is_crc32_of_the_conversation_id(cl):
    """The queue bucket is a stable function of the id (zlib.crc32), not the per-process
    salted ``hash()``, so every worker maps a conversation to the same single-writer queue."""
    for _ in range(50):
        cid = random_conversation_id("q")
        expected = zlib.crc32(cid.encode("utf-8")) % cl._NUM_CONVERSATION_QUEUES
        assert cl.conversation_queue(cid) == f"agentic.conv.{expected}"
        assert cl.conversation_queue(cid) == cl.conversation_queue(cid)


def test_conversation_queue_is_identical_in_a_fresh_interpreter(cl):
    """Cross-process determinism: a second interpreter (its own hash salt) computes the
    same queue names for the same ids."""
    ids = [random_conversation_id("x") for _ in range(8)]
    code = (
        "import sys, agentic_celery as cl; "
        "print('\\n'.join(cl.conversation_queue(c) for c in sys.argv[1:]))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code, *ids], check=True, capture_output=True, text=True, timeout=120
    ).stdout.split()
    assert out == [cl.conversation_queue(c) for c in ids]


def test_eager_runtime_routes_the_banking_paths(cl):
    rt = cl.CeleryRuntime(eager=True)
    cards = rt.submit(Event(conversation_id=random_conversation_id("c"), text=pick(CARD_TEXTS), user_id="demo"))
    pay = rt.submit(Event(conversation_id=random_conversation_id("c"), text=pick(BALANCE_TEXTS), user_id="demo"))
    general = rt.submit(Event(conversation_id=random_conversation_id("c"), text=pick(GENERAL_TEXTS), user_id="demo"))
    assert (cards.path, pay.path, general.path) == ("cards", "payments", "general")
    assert "get_balance" in pay.tool_calls and "1234.56" in pay.reply


def test_eager_runtime_keeps_multi_turn_memory(cl, conversation_id):
    rt = cl.CeleryRuntime(eager=True)
    rt.submit(Event(conversation_id=conversation_id, text=pick(CARD_TEXTS), user_id="demo"))
    rt.submit(Event(conversation_id=conversation_id, text=pick(BALANCE_TEXTS), user_id="demo"))
    assert cl._Deps.get().store.message_count(conversation_id) == 4


def test_extended_core_graph_flows_through_the_task_seam(cl, extension):
    graph, tools = extension
    cl.configure(graph=graph, tools=tools)
    try:
        rt = cl.CeleryRuntime(eager=True)
        res = rt.submit(Event(conversation_id=random_conversation_id("f"), text=pick(FRAUD_TEXTS), user_id="alice"))
        assert res.path == "fraud" and "freeze_card" in res.tool_calls and "FRZ-alice" in res.reply
        balance = Event(conversation_id=random_conversation_id("f"), text=pick(BALANCE_TEXTS), user_id="alice")
        assert rt.submit(balance).path == "payments"
    finally:
        cl.configure()


def test_celery_import_resolves_to_the_installed_distribution(cl):
    import celery

    assert "agentic_celery" not in celery.__file__
    assert cl.__file__.endswith("agentic_celery/__init__.py")

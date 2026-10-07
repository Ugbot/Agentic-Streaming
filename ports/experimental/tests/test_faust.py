"""agentic-faust: the banking graph as a Faust agent body over Table-backed state.

The turn body (``handle_turn``) and the Table-backed ``ConversationStore`` run over plain
dicts here, exactly as they run over Faust Tables in the worker. The worker ``App`` needs
the optional ``faust`` extra (and Kafka to actually run), so only its construction is
tested, and only when faust is installed.
"""

from __future__ import annotations

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
from pyagentic.memory import ChatMessage

pytestmark = pytest.mark.faust


@pytest.fixture
def fa():
    return import_adapter("agentic_faust")


@pytest.fixture
def store(fa):
    return fa.FaustTableConversationStore({}, {})


def test_table_store_round_trips_messages_attributes_and_owner(fa, store, conversation_id):
    store.append(conversation_id, ChatMessage(role="user", content="hi"))
    tool_msg = ChatMessage(role="tool", content="1234.56", tool_name="get_balance", tool_call_id="t1")
    store.append(conversation_id, tool_msg)
    assert store.message_count(conversation_id) == 2
    tool = store.history(conversation_id)[1]
    assert (tool.role, tool.content, tool.tool_name, tool.tool_call_id) == ("tool", "1234.56", "get_balance", "t1")
    store.put_attribute(conversation_id, "tier", "gold")
    store.associate_user(conversation_id, "alice")
    assert store.get_attribute(conversation_id, "tier") == "gold"
    assert store.attributes(conversation_id) == {"tier": "gold"}
    assert store.conversations_for_user("alice") == [conversation_id]
    store.clear(conversation_id)
    assert store.message_count(conversation_id) == 0 and store.attributes(conversation_id) == {}


def test_table_store_bounds_the_transcript(fa):
    store = fa.FaustTableConversationStore({}, {}, max_messages=6)
    cid = random_conversation_id("b")
    for i in range(20):
        store.append(cid, ChatMessage(role="user", content=str(i)))
    assert [m.content for m in store.history(cid)] == [str(i) for i in range(14, 20)]


def test_handle_turn_routes_and_remembers(fa, store, conversation_id):
    pay = fa.handle_turn(conversation_id, "demo", pick(BALANCE_TEXTS), store)
    assert pay.path == "payments" and "get_balance" in pay.tool_calls and "1234.56" in pay.reply
    assert fa.handle_turn(conversation_id, "demo", pick(CARD_TEXTS), store).path == "cards"
    assert fa.handle_turn(random_conversation_id("g"), "demo", pick(GENERAL_TEXTS), store).path == "general"
    assert store.message_count(conversation_id) == 4

    reply = fa.result_to_dict(pay)
    assert reply["conversation_id"] == conversation_id
    assert reply["path"] == "payments" and reply["ok"] is True and reply["tool_calls"] == list(pay.tool_calls)


def test_extended_core_graph_flows_through_handle_turn(fa, store, extension):
    graph, tools = extension
    fa.configure(graph=graph, tools=tools)
    try:
        res = fa.handle_turn(random_conversation_id("f"), "alice", pick(FRAUD_TEXTS), store)
        assert res.path == "fraud" and "freeze_card" in res.tool_calls and "FRZ-alice" in res.reply
    finally:
        fa.configure(graph=fa.build_banking_graph(), tools=fa.default_tools())


def test_worker_app_wires_agent_topics_and_tables(fa):
    pytest.importorskip("faust", reason="faust not installed; the worker app needs the 'faust' extra")
    from agentic_faust import app as worker

    assert worker.app.conf.id == "agentic-faust"
    assert "banking_agent" in {name.rsplit(".", 1)[-1] for name in worker.app.agents}
    assert worker.requests_topic.get_topic_name() == "agentic.requests"
    assert worker.replies_topic.get_topic_name() == "agentic.replies"
    assert isinstance(worker.store, fa.FaustTableConversationStore)
    record = worker.RequestRecord(conversation_id="c", text="hi")
    assert (record.conversation_id, record.user_id, record.text) == ("c", "anonymous", "hi")


def test_faust_import_resolves_to_the_installed_distribution(fa):
    faust = pytest.importorskip("faust", reason="faust not installed")
    assert "agentic_faust" not in faust.__file__
    assert fa.__file__.endswith("agentic_faust/__init__.py")

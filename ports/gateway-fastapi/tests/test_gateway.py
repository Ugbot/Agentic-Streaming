"""Gateway tests against the FastAPI app via TestClient (no running server).

Inputs are randomized: conversation ids are uuid4, and turn texts are drawn from
representative pools per expected path. The LocalBackend is exercised end-to-end
(it is always available, no celery/nats required).
"""

from __future__ import annotations

import random
import uuid

import pytest
from fastapi.testclient import TestClient

from gateway_fastapi.app import create_app
from gateway_fastapi.backends import LocalBackend

# Representative text pools — picked randomly so tests don't rely on one happy path.
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
    "hi, how are you?",
    "where is the nearest branch?",
    "good morning",
]


@pytest.fixture()
def client() -> TestClient:
    # Fresh LocalBackend per test for clean transcript isolation.
    return TestClient(create_app(backend=LocalBackend()))


def _cid() -> str:
    return str(uuid.uuid4())


def test_healthz_ok(client: TestClient) -> None:
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"status": "ok", "backend": "local"}


def test_agent_card_shape(client: TestClient) -> None:
    resp = client.get("/.well-known/agent-card.json")
    assert resp.status_code == 200
    card = resp.json()
    for key in ("name", "description", "version", "url", "capabilities",
                "defaultInputModes", "defaultOutputModes", "skills"):
        assert key in card
    assert card["name"] == "Agentic-Flink Banking Agent"
    assert card["url"] == "/agent"
    assert card["capabilities"] == {"streaming": False, "pushNotifications": False}
    skill_ids = {s["id"] for s in card["skills"]}
    assert "banking" in skill_ids
    banking = next(s for s in card["skills"] if s["id"] == "banking")
    assert banking["name"] == "Banking Q&A"
    assert "rag" in banking["tags"]


def test_agent_routes_cards(client: TestClient) -> None:
    text = random.choice(CARD_TEXTS)
    resp = client.post("/agent", json={"conversation_id": _cid(), "text": text})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["path"] == "cards"
    assert body["ok"] is True


def test_agent_routes_payments_balance_with_tool(client: TestClient) -> None:
    text = random.choice(BALANCE_TEXTS)
    resp = client.post("/agent", json={"conversation_id": _cid(), "text": text, "user_id": "u-" + uuid.uuid4().hex[:6]})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["path"] == "payments"
    assert "get_balance" in body["tool_calls"]
    assert "1234.56" in body["reply"]
    assert body["ok"] is True


def test_agent_routes_general(client: TestClient) -> None:
    text = random.choice(GENERAL_TEXTS)
    resp = client.post("/agent", json={"conversation_id": _cid(), "text": text})
    assert resp.status_code == 200, resp.text
    assert resp.json()["path"] == "general"


def test_multi_turn_accumulates_transcript(client: TestClient) -> None:
    cid = _cid()
    first = client.post("/agent", json={"conversation_id": cid, "text": random.choice(CARD_TEXTS)})
    assert first.status_code == 200
    second = client.post("/agent", json={"conversation_id": cid, "text": random.choice(BALANCE_TEXTS)})
    assert second.status_code == 200

    convo = client.get(f"/conversations/{cid}")
    assert convo.status_code == 200
    body = convo.json()
    assert body["conversation_id"] == cid
    # Two turns -> user+assistant each -> 4 messages for the local backend.
    assert body["message_count"] == 4
    assert len(body["messages"]) == 4
    roles = [m["role"] for m in body["messages"]]
    assert roles == ["user", "assistant", "user", "assistant"]


def test_conversation_isolation(client: TestClient) -> None:
    cid_a, cid_b = _cid(), _cid()
    client.post("/agent", json={"conversation_id": cid_a, "text": random.choice(CARD_TEXTS)})
    # cid_b never gets a turn.
    a = client.get(f"/conversations/{cid_a}").json()
    b = client.get(f"/conversations/{cid_b}").json()
    assert a["message_count"] == 2
    assert b["message_count"] == 0
    assert b["messages"] == []


def test_bad_request_missing_text_returns_422(client: TestClient) -> None:
    resp = client.post("/agent", json={"conversation_id": _cid()})
    assert resp.status_code == 422


def test_bad_request_empty_text_returns_422(client: TestClient) -> None:
    resp = client.post("/agent", json={"conversation_id": _cid(), "text": ""})
    assert resp.status_code == 422

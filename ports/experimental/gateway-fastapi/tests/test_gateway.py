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
from gateway_fastapi.auth import DEV_SUBJECT, BearerAuth, parse_tokens
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


ALICE_TOKEN = "tok-" + uuid.uuid4().hex
BOB_TOKEN = "tok-" + uuid.uuid4().hex
ALICE = {"Authorization": f"Bearer {ALICE_TOKEN}"}
BOB = {"Authorization": f"Bearer {BOB_TOKEN}"}


def _auth() -> BearerAuth:
    return BearerAuth({ALICE_TOKEN: "alice", BOB_TOKEN: "bob"})


@pytest.fixture()
def client() -> TestClient:
    # Fresh LocalBackend per test for clean transcript isolation; alice's token on every call.
    c = TestClient(create_app(backend=LocalBackend(), auth=_auth()))
    c.headers.update(ALICE)
    return c


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
    resp = client.post("/agent", json={"conversation_id": _cid(), "text": text, "user_id": "alice"})
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


def _bare_client(auth: BearerAuth) -> TestClient:
    return TestClient(create_app(backend=LocalBackend(), auth=auth))


def test_missing_or_wrong_token_is_401() -> None:
    c = _bare_client(_auth())
    cid = _cid()
    assert c.get("/healthz").status_code == 200
    assert c.get("/.well-known/agent-card.json").status_code == 200
    for headers in (
        {},
        {"Authorization": "Bearer " + uuid.uuid4().hex},
        {"Authorization": "Basic " + ALICE_TOKEN},
        {"Authorization": ALICE_TOKEN},
        {"Authorization": "Bearer "},
    ):
        resp = c.post("/agent", json={"conversation_id": cid, "text": random.choice(CARD_TEXTS)}, headers=headers)
        assert resp.status_code == 401, (headers, resp.text)
        assert resp.headers.get("www-authenticate") == "Bearer"
        assert c.get(f"/conversations/{cid}", headers=headers).status_code == 401
    # Nothing was recorded for the guessed id under any principal.
    assert c.get(f"/conversations/{cid}", headers=ALICE).json()["message_count"] == 0


def test_client_supplied_user_id_cannot_impersonate() -> None:
    c = _bare_client(_auth())
    other = "u-" + uuid.uuid4().hex[:8]
    resp = c.post("/agent", json={"conversation_id": _cid(), "text": random.choice(BALANCE_TEXTS), "user_id": other}, headers=ALICE)
    assert resp.status_code == 403
    ok = c.post("/agent", json={"conversation_id": _cid(), "text": random.choice(BALANCE_TEXTS), "user_id": "alice"}, headers=ALICE)
    assert ok.status_code == 200


def test_cross_user_conversation_access_is_isolated() -> None:
    c = _bare_client(_auth())
    cid = _cid()
    turns = random.randint(1, 3)
    for _ in range(turns):
        assert c.post("/agent", json={"conversation_id": cid, "text": random.choice(CARD_TEXTS)}, headers=ALICE).status_code == 200
    mine = c.get(f"/conversations/{cid}", headers=ALICE).json()
    assert mine["message_count"] == 2 * turns
    # Bob knows the id but gets his own empty scope, never alice's transcript.
    theirs = c.get(f"/conversations/{cid}", headers=BOB).json()
    assert theirs["conversation_id"] == cid
    assert theirs["message_count"] == 0
    # Bob continuing "the same" id starts a separate conversation.
    assert c.post("/agent", json={"conversation_id": cid, "text": random.choice(GENERAL_TEXTS)}, headers=BOB).status_code == 200
    assert c.get(f"/conversations/{cid}", headers=ALICE).json()["message_count"] == 2 * turns
    assert c.get(f"/conversations/{cid}", headers=BOB).json()["message_count"] == 2


def test_conversation_id_cannot_escape_scope() -> None:
    c = _bare_client(_auth())
    cid = _cid()
    c.post("/agent", json={"conversation_id": cid, "text": random.choice(CARD_TEXTS)}, headers=ALICE)
    resp = c.post("/agent", json={"conversation_id": f"../alice/{cid}", "text": "x"}, headers=BOB)
    assert resp.status_code == 422
    assert c.get(f"/conversations/alice/{cid}", headers=BOB).status_code in (404, 422)


def test_no_token_configured_fails_closed_unless_dev_mode() -> None:
    closed = _bare_client(BearerAuth({}))
    resp = closed.post("/agent", json={"conversation_id": _cid(), "text": random.choice(CARD_TEXTS)}, headers={"Authorization": "Bearer " + uuid.uuid4().hex})
    assert resp.status_code == 401
    assert closed.get(f"/conversations/{_cid()}").status_code == 401

    dev = _bare_client(BearerAuth({}, dev_mode=True))
    cid = _cid()
    ok = dev.post("/agent", json={"conversation_id": cid, "text": random.choice(CARD_TEXTS)})
    assert ok.status_code == 200
    assert dev.get(f"/conversations/{cid}").json()["message_count"] == 2
    assert dev.post("/agent", json={"conversation_id": cid, "text": "x", "user_id": "someone-else"}).status_code == 403
    assert dev.post("/agent", json={"conversation_id": cid, "text": "x", "user_id": DEV_SUBJECT}).status_code == 200


def test_parse_tokens_and_env_loading() -> None:
    subjects = {"u" + uuid.uuid4().hex[:6]: "t" + uuid.uuid4().hex for _ in range(random.randint(1, 5))}
    spec = ",".join(f"{s}={t}" for s, t in subjects.items())
    assert parse_tokens(spec) == {t: s for s, t in subjects.items()}
    single = uuid.uuid4().hex
    assert parse_tokens(single) == {single: "gateway-client"}
    assert parse_tokens("") == {}
    with pytest.raises(ValueError):
        parse_tokens("alice=")
    auth = BearerAuth.from_env({"AGENTIC_GATEWAY_TOKENS": spec})
    subj, tok = random.choice(list(subjects.items()))
    assert auth.authenticate(f"Bearer {tok}").subject == subj
    assert not BearerAuth.from_env({}).configured

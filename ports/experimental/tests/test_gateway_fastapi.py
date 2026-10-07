"""agentic-gateway-fastapi: the HTTP edge over the local, celery and nats backends.

The HTTP surface (auth, routing, isolation) is covered by gateway-fastapi/tests, which
``pytest ports/experimental`` also collects. This module covers the backend factory over
the installed adapter packages: the celery backend runs eagerly through ``agentic_celery``
and the nats backend against a live JetStream server (skipped with the reason otherwise).
"""

from __future__ import annotations

import pytest
from adapter_support import BALANCE_TEXTS, CARD_TEXTS, import_adapter, pick, random_conversation_id

pytestmark = pytest.mark.gateway


@pytest.fixture
def gw():
    return import_adapter("gateway_fastapi.backends")


def test_backend_factory_names_and_env_default(gw, monkeypatch):
    monkeypatch.delenv("AGENTIC_GATEWAY_BACKEND", raising=False)
    assert gw.make_backend().name == "local"
    monkeypatch.setenv("AGENTIC_GATEWAY_BACKEND", "LOCAL")
    assert gw.make_backend().name == "local"
    with pytest.raises(ValueError, match="unknown backend 'ray'.*celery.*local.*nats"):
        gw.make_backend("ray")


def test_local_backend_submits_and_records_history(gw):
    backend = gw.make_backend("local")
    cid = random_conversation_id("gl")
    res = backend.submit(cid, pick(BALANCE_TEXTS), user_id="alice")
    assert res["conversation_id"] == cid and res["path"] == "payments" and "get_balance" in res["tool_calls"]
    assert [m["role"] for m in backend.history(cid)] == ["user", "assistant"]


def test_celery_backend_runs_through_the_installed_adapter(gw):
    if gw.agentic_celery is None:
        pytest.skip(f"agentic_celery not importable: {gw._CELERY_IMPORT_ERROR}")
    backend = gw.make_backend("celery")
    cid = random_conversation_id("gc")
    assert backend.submit(cid, pick(CARD_TEXTS), user_id="alice")["path"] == "cards"
    assert backend.submit(cid, pick(BALANCE_TEXTS), user_id="alice")["path"] == "payments"
    assert len(backend.history(cid)) == 4


def test_missing_celery_adapter_is_reported_when_selected(gw, monkeypatch):
    monkeypatch.setattr(gw, "agentic_celery", None)
    monkeypatch.setattr(gw, "_CELERY_IMPORT_ERROR", ImportError("No module named 'agentic_celery'"))
    with pytest.raises(RuntimeError, match=r"celery backend unavailable.*agentic-gateway-fastapi\[celery\]"):
        gw.make_backend("celery")


def test_nats_backend_reads_history_back_from_jetstream(gw):
    if gw.agentic_nats is None:
        pytest.skip(f"agentic_nats not importable: {gw._NATS_IMPORT_ERROR}")
    try:
        backend = gw.make_backend("nats")
    except RuntimeError as exc:
        pytest.skip(str(exc))
    try:
        cid = random_conversation_id("gn")
        assert backend.submit(cid, pick(BALANCE_TEXTS), user_id="alice")["path"] == "payments"
        assert backend.submit(cid, pick(CARD_TEXTS), user_id="alice")["path"] == "cards"
        assert [m["role"] for m in backend.history(cid)] == ["user", "assistant", "user", "assistant"]
    finally:
        backend.close()


def test_nats_backend_names_the_server_when_unreachable(gw):
    if gw.agentic_nats is None:
        pytest.skip(f"agentic_nats not importable: {gw._NATS_IMPORT_ERROR}")
    with pytest.raises(RuntimeError, match=r"could not connect to nats://127\.0\.0\.1:1.*podman run"):
        gw.NatsBackend(url="nats://127.0.0.1:1")

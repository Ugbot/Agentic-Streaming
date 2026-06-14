"""Hot-swappable external store: the SAME pipeline persists to Redis/Valkey when the
YAML `stores:` section selects it — proving the durable backing is behind an interface
and swappable by a connection link."""

from __future__ import annotations

import copy
import os
import uuid

import pytest
import yaml

from agentic_pipeline.loader import build_system
from pyagentic.core import Event

# The compose file maps Valkey to 6379; this dev box runs it on 6380 (6379 was taken).
REDIS_URL = os.environ.get("AGENTIC_TEST_REDIS_URL", "redis://localhost:6380/0")

_SPEC = yaml.safe_load(
    """
    backend: local
    agent:
      router: {kind: keyword, default: general, rules: {payments: [balance], cards: [card]}}
      paths:
        payments: {brain: rule, prompt: pay, tool_triggers: {balance: get_balance}}
        cards: {brain: rule, prompt: cards}
        general: {brain: rule, prompt: gen}
      verifier: {kind: prefix}
    tools:
      - {id: get_balance, kind: constant, value: 1234.56}
    """
)


def _redis_or_skip():
    try:
        import redis
    except ImportError:
        pytest.skip("redis-py not installed")
    try:
        redis.from_url(REDIS_URL, socket_connect_timeout=1).ping()
    except Exception as exc:  # no server
        pytest.skip(f"no Redis/Valkey at {REDIS_URL}: {exc}")


def test_redis_store_hot_swap_persists():
    _redis_or_skip()
    spec = copy.deepcopy(_SPEC)
    spec["stores"] = {"conversation": {"kind": "redis", "url": REDIS_URL}}
    system = build_system(spec, backend="local")

    cid = "c-" + uuid.uuid4().hex[:8]
    res = system.submit(Event(cid, "what is my balance?", "alice"))
    assert res.path == "payments" and "get_balance" in res.tool_calls

    # The backend's store is the Redis one; the transcript is durable in Redis/Valkey.
    store = system.backend.store
    assert type(store).__name__ == "RedisConversationStore"
    assert store.message_count(cid) == 2  # user + assistant
    assert cid in store.conversations_for_user("alice")
    store.clear(cid)


def test_memory_store_is_the_default():
    system = build_system(copy.deepcopy(_SPEC), backend="local")
    assert type(system.backend.store).__name__ == "InMemoryConversationStore"

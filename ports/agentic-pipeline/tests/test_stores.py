"""The stores a pipeline names: the event log and the journal, in memory or in files. A store the
local runtime does not have is refused, not quietly replaced."""

from __future__ import annotations

import copy
import os

import pytest
import yaml

from agentic.events import InMemoryEventLog
from agentic.journal import InMemoryEffectJournal
from agentic_pipeline import Event
from agentic_pipeline.loader import build_system

# The compose file maps Valkey to 6379; this dev box runs it on 6380 (6379 was taken).
REDIS_URL = os.environ.get("AGENTIC_TEST_REDIS_URL", "redis://localhost:6380/0")

_SPEC = yaml.safe_load(
    """
    backend: local
    agent:
      router: {kind: keyword, default: general, rules: {payments: [balance], cards: [card]}}
      paths:
        payments: {brain: rule, prompt: pay, tools: [get_balance], tool_triggers: {balance: get_balance}}
        cards: {brain: rule, prompt: cards}
        general: {brain: rule, prompt: gen}
      verifier: {kind: prefix}
    tools:
      - {id: get_balance, kind: constant, effect: read, value: 1234.56}
    """
)


def test_a_redis_conversation_store_is_refused_rather_than_ignored():
    """The local runtime keeps conversations on its event log, in memory or in files. A Redis store
    the document names is refused when it loads, unless the document says it may degrade."""
    spec = copy.deepcopy(_SPEC)
    spec["stores"] = {"conversation": {"kind": "redis", "url": REDIS_URL}}
    with pytest.raises(ValueError, match="redis"):
        build_system(spec, backend="local")
    spec["stores"]["conversation"]["on_unavailable"] = "degrade"
    system = build_system(spec, backend="local")
    res = system.submit(Event("c1", "what is my balance?", "alice"))
    assert res.ok and any("redis" in d for d in res.result["runtime_detail"]["degraded"])
    system.backend.close()


def test_memory_store_is_the_default():
    system = build_system(copy.deepcopy(_SPEC), backend="local")
    assert isinstance(system.backend.runtime.log, InMemoryEventLog)
    assert isinstance(system.backend.runtime.journal, InMemoryEffectJournal)
    system.backend.close()

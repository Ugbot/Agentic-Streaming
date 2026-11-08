"""The core routes with the spec/v1 keyword semantics (workflow.schema.json
``router.rules``, fixtures 01 and 02): case-insensitive substring on the turn text, first path in
declaration order wins, no match falls to ``router.default``. Unsupported kinds fail with a message
that lists the kinds this runtime does implement."""

from __future__ import annotations

import random
from pathlib import Path

import pytest
import yaml

from agentic.errors import ValidationError
from agentic.runtime import LocalRuntime
from agentic.engine import ROUTER_KINDS

REPO = Path(__file__).resolve().parents[3]
SUPPORT = yaml.safe_load((REPO / "spec" / "conformance" / "v1" / "workflows" / "support.yaml").read_text())
BANKING = yaml.safe_load((REPO / "examples" / "pipelines" / "banking.yaml").read_text())
FIXTURES = REPO / "spec" / "conformance" / "v1" / "fixtures"

# (text, expected path) pairs, straight from fixture 01/02 plus casing and ordering probes.
SUPPORT_CASES = [
    ("I want a refund for this charge", "billing"),  # fixture 01
    ("what are your opening hours?", "general"),  # fixture 02
    ("REFUND please", "billing"),
    ("my LoGiN is broken", "account"),
    ("login to see my balance", "billing"),  # billing is declared first and wins
    ("", "general"),
]


def _fixture_turn(name: str) -> tuple[str, str]:
    doc = yaml.safe_load((FIXTURES / name).read_text())
    return doc["turns"][0]["text"], doc["expect"][0]["path"]


def _pure_route(doc: dict, text: str) -> str:
    rt = LocalRuntime()
    rt.deploy(doc)
    try:
        return rt.submit({"conversation_id": "c", "turn_id": "t", "text": text})["path"]
    finally:
        rt.close()


@pytest.mark.parametrize("text,path", SUPPORT_CASES + [_fixture_turn("01-routing-keyword.yaml"), _fixture_turn("02-routing-default.yaml")])
def test_the_core_agrees_with_the_spec_on_support_workflow(text: str, path: str):
    assert _pure_route(SUPPORT, text) == path


def test_keyword_router_matches_random_casing_and_padding():
    rng = random.Random(20260914)
    rules = SUPPORT["agent"]["router"]["rules"]
    for _ in range(200):
        path = rng.choice(list(rules))
        word = rng.choice(rules[path])
        text = "".join(c.upper() if rng.random() < 0.5 else c for c in f"xx{word}yy")
        assert _pure_route(SUPPORT, text) == path


def test_declaration_order_wins_when_several_paths_match():
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "keyword", "default": "general", "rules": {"account": ["refund"], "billing": ["refund"]}}}}
    assert _pure_route(doc, "refund") == "account"


def test_banking_routes_with_the_banking_yaml_table():
    for text, path in [("crypto CASH-BACK?", "cards"), ("dispute a charge", "payments"), ("hi", "general")]:
        assert _pure_route(BANKING, text) == path


def test_static_router_sends_everything_to_default():
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "static", "default": "account"}}}
    assert _pure_route(doc, "refund") == "account"


@pytest.mark.parametrize("kind", ["llm", "classifier"])
def test_unsupported_router_kinds_list_the_supported_ones(kind: str):
    """Schema-valid kinds this runtime does not implement fail the turn with a validation error, never
    a silent fallback, that names the kinds that are supported."""
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": kind, "default": "general"}}}
    rt = LocalRuntime()
    rt.deploy(doc)
    result = rt.submit({"conversation_id": "c", "turn_id": "t", "text": "hello"})
    rt.close()
    assert result["status"] == "failed" and result["path"] is None
    assert result["error"]["class"] == "validation"
    assert f"router kind {kind!r}" in result["error"]["message"]
    assert "supported kinds: keyword, static" in result["error"]["message"]
    assert ROUTER_KINDS == ("keyword", "static")


def test_schema_rejects_unknown_router_kinds_before_routing():
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "bogus", "default": "general"}}}
    with pytest.raises(ValidationError, match="/agent/router/kind"):
        LocalRuntime().deploy(doc)

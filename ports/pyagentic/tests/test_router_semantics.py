"""Both Python engines route with the spec/v1 keyword semantics (workflow.schema.json
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
from pyagentic import builder
from pyagentic.banking import BANKING_DEFAULT_PATH, BANKING_ROUTES, banking_router
from pyagentic.core import ROUTER_KINDS, Event, keyword_router, static_router

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


def _graph_route(doc: dict, text: str) -> str:
    graph, _tools, _retriever = builder.build(doc)
    return graph.router(Event(conversation_id="c", text=text), None)


@pytest.mark.parametrize("text,path", SUPPORT_CASES + [_fixture_turn("01-routing-keyword.yaml"), _fixture_turn("02-routing-default.yaml")])
def test_both_engines_agree_with_the_spec_on_support_workflow(text: str, path: str):
    assert _pure_route(SUPPORT, text) == path
    assert _graph_route(SUPPORT, text) == path


def test_keyword_router_matches_random_casing_and_padding():
    rng = random.Random(20260914)
    rules = SUPPORT["agent"]["router"]["rules"]
    route = keyword_router(rules, "general")
    for _ in range(200):
        path = rng.choice(list(rules))
        word = rng.choice(rules[path])
        text = "".join(c.upper() if rng.random() < 0.5 else c for c in f"xx{word}yy")
        assert route(Event("c", text), None) == path
        assert _pure_route(SUPPORT, text) == path


def test_declaration_order_wins_when_several_paths_match():
    rules = {"first": ["alpha"], "second": ["beta", "alpha"]}
    assert keyword_router(rules, "second")(Event("c", "ALPHA beta"), None) == "first"
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "keyword", "default": "general", "rules": {"account": ["refund"], "billing": ["refund"]}}}}
    assert _pure_route(doc, "refund") == "account"
    assert _graph_route(doc, "refund") == "account"


def test_banking_router_uses_the_banking_yaml_table():
    assert BANKING_ROUTES == BANKING["agent"]["router"]["rules"]
    assert BANKING_DEFAULT_PATH == BANKING["agent"]["router"]["default"]
    for text, path in [("crypto CASH-BACK?", "cards"), ("dispute a charge", "payments"), ("hi", "general")]:
        assert banking_router(Event("c", text), None) == path
        assert _graph_route(BANKING, text) == path


def test_static_router_sends_everything_to_default():
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "static", "default": "account"}}}
    assert _pure_route(doc, "refund") == "account"
    assert _graph_route(doc, "refund") == "account"
    assert static_router("x")(Event("c", "anything"), None) == "x"


@pytest.mark.parametrize("kind", ["llm", "classifier"])
def test_unsupported_router_kinds_list_the_supported_ones(kind: str):
    """Schema-valid kinds this runtime does not implement: the pure engine fails the turn with a
    validation error (never a silent fallback) and GraphBuilder refuses to build; both name the kinds
    that are supported."""
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": kind, "default": "general"}}}
    rt = LocalRuntime()
    rt.deploy(doc)
    result = rt.submit({"conversation_id": "c", "turn_id": "t", "text": "hello"})
    rt.close()
    assert result["status"] == "failed" and result["path"] is None
    assert result["error"]["class"] == "validation"
    assert f"router kind {kind!r}" in result["error"]["message"]
    assert "supported kinds: keyword, static" in result["error"]["message"]
    with pytest.raises(ValueError, match="supported kinds: keyword, static"):
        builder.build(doc)
    assert ROUTER_KINDS == ("keyword", "static")


def test_schema_rejects_unknown_router_kinds_before_routing():
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "bogus", "default": "general"}}}
    with pytest.raises(ValidationError, match="/agent/router/kind"):
        LocalRuntime().deploy(doc)


def test_graph_builder_requires_a_default_for_multi_path_agents():
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "keyword", "rules": {"billing": ["refund"]}}}}
    with pytest.raises(ValueError, match="router.default is required"):
        builder.build(doc)
    single = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "keyword"}, "paths": {"only": {"brain": "rule"}}}}
    assert _graph_route(single, "whatever") == "only"


def test_graph_builder_rejects_undeclared_paths():
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "keyword", "default": "nope"}}}
    with pytest.raises(ValueError, match="not a declared path"):
        builder.build(doc)
    doc = {**SUPPORT, "agent": {**SUPPORT["agent"], "router": {"kind": "keyword", "default": "general", "rules": {"ghost": ["x"]}}}}
    with pytest.raises(ValueError, match="undeclared paths: ghost"):
        builder.build(doc)

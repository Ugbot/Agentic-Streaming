"""Workflow IR loading, schema validation, cross-field rules, unknown-field policy."""

from __future__ import annotations

import copy
import json
import uuid
from pathlib import Path

import pytest

from agentic import ValidationError, ir
from agentic.conformance import fixtures_dir

SPEC_V1 = fixtures_dir().parents[2] / "v1"


def minimal(**overrides):
    doc = {
        "agent": {
            "id": f"agent-{uuid.uuid4().hex[:8]}",
            "router": {"kind": "keyword", "default": "main", "rules": {"main": ["hi"]}},
            "paths": {"main": {"brain": "rule", "prompt": "Say hi."}},
        }
    }
    doc.update(overrides)
    return doc


def test_package_schemas_are_identical_to_spec_v1():
    for name, bundled in (("workflow.schema.json", ir.WORKFLOW_SCHEMA), ("result.schema.json", ir.RESULT_SCHEMA)):
        assert bundled == json.loads((SPEC_V1 / name).read_text(encoding="utf-8")), name


def test_load_yaml_json_string_and_mapping(tmp_path: Path):
    doc = minimal()
    yaml_path = tmp_path / "w.yaml"
    yaml_path.write_text(ir.dumps(doc, "yaml"), encoding="utf-8")
    json_path = tmp_path / "w.json"
    json_path.write_text(ir.dumps(doc, "json"), encoding="utf-8")
    loaded = [ir.validate_document(ir.read_document(src))
              for src in (yaml_path, str(json_path), ir.dumps(doc, "yaml"), ir.dumps(doc, "json"), doc)]
    assert all(d["agent"] == doc["agent"] for d in loaded)
    assert all(d["spec_version"] == "agentic/v1" for d in loaded)


def test_missing_file_is_a_validation_error(tmp_path: Path):
    with pytest.raises(ValidationError, match="not found"):
        ir.read_document(tmp_path / "nope.yaml")


def test_normalized_copy_does_not_alias_input():
    doc = minimal()
    normalized = ir.validate_document(doc)
    normalized["agent"]["paths"]["main"]["prompt"] = "changed"
    assert doc["agent"]["paths"]["main"]["prompt"] == "Say hi."


def test_newer_major_version_is_rejected():
    with pytest.raises(ValidationError, match="spec_version"):
        ir.validate_document(minimal(spec_version="agentic/v2"))


@pytest.mark.parametrize("mutate,pointer", [
    (lambda d: d.__setitem__("bogus", 1), "/"),
    (lambda d: d["agent"].__setitem__("bogus", 1), "/agent"),
    (lambda d: d["agent"]["paths"]["main"].__setitem__("bogus", 1), "/agent/paths/main"),
    (lambda d: d.__setitem__("tools", [{"id": "t", "bogus": 1}]), "/tools/0"),
])
def test_unknown_fields_are_rejected_with_pointer(mutate, pointer):
    doc = minimal()
    mutate(doc)
    with pytest.raises(ValidationError) as info:
        ir.validate_document(doc)
    assert info.value.pointer == pointer


def test_extension_keys_and_runtime_blocks_are_allowed():
    doc = minimal(runtime={"flink": {"parallelism": 8, "anything": True}})
    doc["x-owner"] = "team"
    doc["agent"]["x-note"] = 1
    doc["agent"]["paths"]["main"]["x-suspend-until"] = "approval"
    doc["tools"] = [{"id": "t", "kind": "failing", "x-fail-attempts": 2}]
    normalized = ir.validate_document(doc)
    assert normalized["runtime"]["flink"]["parallelism"] == 8
    assert normalized["tools"][0]["x-fail-attempts"] == 2


@pytest.mark.parametrize("mutate,pointer", [
    (lambda d: d["agent"]["router"]["rules"].__setitem__("ghost", ["x"]), "/agent/router/rules/ghost"),
    (lambda d: d["agent"]["router"].__setitem__("default", "ghost"), "/agent/router/default"),
    (lambda d: d["agent"]["paths"]["main"].__setitem__("tools", ["ghost"]), "/agent/paths/main/tools/0"),
    (lambda d: d["agent"]["paths"]["main"].__setitem__("tool_triggers", {"hi": "ghost"}),
     "/agent/paths/main/tool_triggers/hi"),
    (lambda d: d["agent"]["paths"]["main"].__setitem__("brain", "llm"), "/agent/paths/main/brain"),
    (lambda d: d["agent"]["paths"]["main"].__setitem__("guardrails", ["ghost"]), "/agent/paths/main/guardrails/0"),
    (lambda d: d.__setitem__("saga", {"steps": [{"tool": "ghost"}]}), "/saga/steps/0/tool"),
    (lambda d: d.__setitem__("tools", [{"id": "a", "compensation": "ghost"}]), "/tools/0/compensation"),
    (lambda d: (d.__setitem__("retrieval", {"dim": 8}), d.__setitem__("embeddings", {"dim": 16})), "/retrieval/dim"),
    (lambda d: d.__setitem__("tools", [{"id": "dup"}, {"id": "dup"}]), "/tools/1/id"),
    (lambda d: (d.__setitem__("tools", [{"id": "dup"}]), d.__setitem__("a2a", [{"name": "dup"}])), "/a2a/0"),
])
def test_cross_field_rules(mutate, pointer):
    doc = minimal()
    mutate(doc)
    with pytest.raises(ValidationError) as info:
        ir.validate_document(doc)
    assert info.value.pointer == pointer


def test_valid_cross_references_pass():
    doc = minimal(
        tools=[{"id": "a", "kind": "constant", "value": 1, "compensation": "b"}, {"id": "b", "kind": "constant"}],
        a2a=[{"name": "peer", "transport": "inproc"}],
        guardrails=[{"name": "g", "kind": "regex", "deny": ["x"]}],
        saga={"steps": [{"tool": "a", "compensate_with": "b"}, {"tool": "peer"}]},
        llm={"provider": "stub", "script": [{"text": "[main] ok"}]},
        retrieval={"dim": 8, "kb": [{"id": "p", "text": "t"}]},
        embeddings={"dim": 8},
    )
    doc["agent"]["paths"]["main"].update({"tools": ["a", "peer"], "guardrails": ["g"], "brain": "llm"})
    ir.validate_document(copy.deepcopy(doc))


def test_shared_support_workflow_and_every_fixture_workflow_validate():
    fixtures = fixtures_dir()
    ir.validate_document(ir.read_document(fixtures.parent / "workflows" / "support.yaml"))
    for path in sorted(fixtures.glob("*.yaml")):
        fixture = ir.read_document(path)
        if "workflow" in fixture:
            ir.validate_document(fixture["workflow"])


def test_result_schema_rejects_unknown_status():
    with pytest.raises(ValidationError):
        ir.validate_result({"conversation_id": "c", "turn_id": "t", "status": "weird", "state": {},
                            "tool_calls": [], "events": []})

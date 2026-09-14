"""Capability derivation has one source: ``agentic.runtime.required_capabilities`` in
``ports/pyagentic``. This package only re-exports it (or mirrors it when that package is absent);
every fixture workflow must yield the identical list through every binding."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
import yaml

from agentic_flink import _contract
from agentic_flink.workflow import AgentSpec, load, required_capabilities, workflow_requirements

REPO = Path(__file__).resolve().parents[2]
FIXTURES = sorted((REPO / "spec" / "conformance" / "v1" / "fixtures").glob("*.yaml"))
canonical = pytest.importorskip("agentic.runtime", reason="pyagentic (ports/pyagentic) must be installed")


def _workflow(fixture: Path) -> dict:
    doc = yaml.safe_load(fixture.read_text(encoding="utf-8"))
    return doc.get("workflow") or yaml.safe_load((fixture.parent / doc["workflow_ref"]).read_text(encoding="utf-8"))


def test_contract_re_exports_the_pure_package():
    assert _contract.CONTRACT_SOURCE == "agentic.runtime"
    assert _contract.required_capabilities is canonical.required_capabilities


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: p.stem)
def test_every_fixture_derives_identically_through_every_binding(fixture: Path):
    workflow = _workflow(fixture)
    expected = canonical.required_capabilities(workflow)
    assert required_capabilities(workflow) == expected
    assert list(workflow_requirements(workflow)) == expected
    assert list(AgentSpec(workflow).requirements()) == expected
    pyflink_workflow = pytest.importorskip("agentic_pyflink.workflow", reason="agentic-pyflink not installed")
    assert pyflink_workflow.required_capabilities(workflow) == expected


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: p.stem)
def test_fallback_mirrors_the_canonical_derivation(fixture: Path, monkeypatch):
    """With ``agentic`` uninstalled the fallback in ``_contract`` must give the same answer."""
    workflow = _workflow(fixture)
    expected = canonical.required_capabilities(workflow)
    for mod in [m for m in sys.modules if m == "agentic" or m.startswith("agentic.")]:
        monkeypatch.delitem(sys.modules, mod)
    monkeypatch.setitem(sys.modules, "agentic", None)
    monkeypatch.setitem(sys.modules, "agentic.runtime", None)
    monkeypatch.setitem(sys.modules, "agentic.errors", None)
    monkeypatch.delitem(sys.modules, "agentic_flink._contract")
    fallback = importlib.import_module("agentic_flink._contract")
    try:
        assert fallback.CONTRACT_SOURCE.startswith("agentic_flink._contract")
        assert fallback.required_capabilities(workflow) == expected
    finally:
        monkeypatch.undo()
        sys.modules["agentic_flink._contract"] = _contract


def test_support_workflow_requirements_name_locations():
    req = workflow_requirements(load(REPO / "spec" / "conformance" / "v1" / "workflows" / "support.yaml").to_dict())
    assert req["routing"] == "agent.router" and req["memory"].startswith("agent")
    assert set(req) == set(canonical.required_capabilities(load(REPO / "spec" / "conformance" / "v1" / "workflows" / "support.yaml").to_dict()))

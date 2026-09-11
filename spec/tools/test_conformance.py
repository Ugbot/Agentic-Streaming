"""Every v1 fixture must pass against the reference runtime, and every spec document in
the repository must validate against the v1 schemas."""

from pathlib import Path

import pytest

from reference_runtime import ReferenceRuntime, SpecError, Turn
from run_conformance import FIXTURES, REFERENCE_CAPABILITIES, load, run_fixture
from validate_spec import main as validate_main

FIXTURE_PATHS = sorted(FIXTURES.glob("*.yaml"))


@pytest.mark.parametrize("path", FIXTURE_PATHS, ids=[p.stem for p in FIXTURE_PATHS])
def test_fixture_passes_on_reference_runtime(path: Path) -> None:
    problems = run_fixture(path)
    assert not problems, "\n".join(problems)


def test_every_fixture_declares_a_unique_id() -> None:
    ids = [load(p)["id"] for p in FIXTURE_PATHS]
    assert len(ids) == len(set(ids))


def test_spec_documents_validate() -> None:
    assert validate_main([]) == 0


def test_every_capability_id_is_required_by_some_fixture() -> None:
    """Every id in primitives.md section 6 is provable; none is stuck at not_tested."""
    primitives = (FIXTURES.parents[2] / "v1" / "primitives.md").read_text(encoding="utf-8")
    start = primitives.index("Capability ids in v1:")
    declared = {tok.strip("`") for tok in primitives[start:primitives.index("\n\n", start)].split("`")[1::2]}
    required = {cap for p in FIXTURE_PATHS for cap in load(p)["requires"]}
    assert declared == required
    assert declared == REFERENCE_CAPABILITIES


def _workflow(**extra):
    return {"spec_version": "agentic/v1", "agent": {"paths": {"main": {"brain": "rule"}},
            "router": {"default": "main"}, "verifier": {"kind": "none"}}, **extra}


@pytest.mark.parametrize("extra", [
    {"context": {"compaction": "moscow"}},
    {"context": {"max_tokens": 10}},
    {"cep": [{"name": "p", "pattern": [{"where": {"text_contains": "x"}}], "on_match": {"kind": "submit", "text": "t"}}]},
])
def test_reference_rejects_semantics_it_does_not_implement(extra) -> None:
    with pytest.raises(SpecError):
        ReferenceRuntime(_workflow(**extra))


def test_reference_rejects_non_stub_llm_provider_and_scripts_without_text() -> None:
    llm_path = {"agent": {"paths": {"main": {"brain": "llm"}}, "router": {"default": "main"}, "verifier": {"kind": "none"}}}
    with pytest.raises(SpecError):
        ReferenceRuntime(_workflow(llm={"provider": "openai"}, **llm_path)).submit(Turn("c", "t", "hi"))
    with pytest.raises(SpecError):
        ReferenceRuntime(_workflow(llm={"provider": "stub", "script": [{"tool": "x", "args": {}}]},
                                   tools=[{"id": "x", "value": 1}], **llm_path)).submit(Turn("c", "t", "hi"))
    with pytest.raises(SpecError):
        ReferenceRuntime(_workflow()).advance(-1)

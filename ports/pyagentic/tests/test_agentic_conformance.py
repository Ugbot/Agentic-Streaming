"""The v1 conformance fixtures, read in place from spec/conformance/v1/fixtures, against the
local runtime. A fixture the runtime cannot claim is a pytest skip with the reason; a
mismatch is a failure. Missing fixtures are an error, not a skip."""

from __future__ import annotations

from pathlib import Path

import pytest

from agentic.conformance import Outcome, check_expectation, fixture_paths, fixtures_dir, load_yaml, run_fixture
from agentic.runtime import LocalRuntime

PATHS = fixture_paths()


def test_fixtures_are_read_from_the_shared_spec_directory():
    directory = fixtures_dir()
    assert directory.parts[-4:] == ("spec", "conformance", "v1", "fixtures")
    assert len(PATHS) == 15
    assert (directory.parent / "workflows" / "support.yaml").exists()


@pytest.mark.parametrize("path", PATHS, ids=[p.stem for p in PATHS])
def test_fixture(path: Path):
    outcome = run_fixture(path, LocalRuntime)
    if outcome.status == "skip":
        pytest.skip(outcome.reason)
    assert outcome.status == "pass", "\n".join(outcome.problems)


def test_unsupported_requirement_is_a_skip_never_a_pass():
    class Narrow(LocalRuntime):
        def capabilities(self):
            caps = super().capabilities()
            caps["retry"] = "not_tested"
            return caps

    outcomes = [run_fixture(p, Narrow) for p in PATHS]
    skipped = [o for o in outcomes if o.status == "skip"]
    assert {o.fixture_id for o in skipped} == {"tool-failure", "retry-tool"}
    assert all("retry=not_tested" in o.reason for o in skipped)
    assert all(o.status == "pass" for o in outcomes if o.status != "skip")


def test_comparator_applies_the_readme_rules():
    actual = {
        "status": "completed", "path": "p", "reply": "[p] hi", "error": None,
        "tool_calls": [{"tool": "a", "index": 0, "attempt": 1, "args": {"x": 1}, "result": 1}],
        "events": [{"type": "turn_received", "sequence": 0}, {"type": "routed", "sequence": 1},
                   {"type": "turn_completed", "sequence": 2}],
        "state": {"turn_count": 1, "extra": True},
        "runtime_detail": {"anything": "ignored"},
    }
    assert check_expectation({"status": "completed", "path": "p", "reply_matches": r"^\[p\]",
                              "tool_calls": [{"tool": "a", "index": 0, "args": {"x": 1}}],
                              "events_include": ["turn_received", "turn_completed"],
                              "events_exclude": ["turn_failed"], "state_includes": {"turn_count": 1}}, actual) == []
    problems = check_expectation({"status": "failed", "error_class": "tool",
                                  "tool_calls": [{"tool": "a", "index": 0, "failed": True}],
                                  "events_include": ["turn_completed", "routed"],
                                  "state_includes": {"turn_count": 2}}, actual)
    assert len(problems) == 5


def test_every_fixture_declares_requirements_the_runtime_knows():
    declared = LocalRuntime().capabilities()
    for path in PATHS:
        for cap in load_yaml(path)["requires"]:
            assert cap in declared, f"{path.name} requires unknown capability {cap}"


def test_outcome_reason_joins_problems():
    assert Outcome("x", Path("x"), "fail", ["a", "b"]).reason == "a; b"

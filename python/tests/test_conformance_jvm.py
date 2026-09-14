"""The shared fixtures (spec/conformance/v1, discovered from the directory) run in place against
the JVM-backed runtimes.

Each fixture is one test per runtime. A fixture whose ``requires`` the runtime does not support
is a pytest *skip* (never a pass); a comparison mismatch is a failure; missing infrastructure
(no jars) is a failure with the fix in the message. Every produced result must also validate
against ``spec/v1/result.schema.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest
import yaml

from agentic_flink._contract import get_runtime
from agentic_flink.conformance import Outcome, default_fixtures_dir, fixture_workflow, load_comparator, run_fixture
from agentic_flink.conformance import _drive, _load

REPO_ROOT = Path(__file__).resolve().parents[2]
RESULT_VALIDATOR = jsonschema.Draft202012Validator(
    json.loads((REPO_ROOT / "spec" / "v1" / "result.schema.json").read_text()))
FIXTURES = sorted(default_fixtures_dir().glob("*.yaml"))
assert FIXTURES, f"no fixtures under {default_fixtures_dir()}"
assert len({p.stem for p in FIXTURES}) == len(FIXTURES), FIXTURES

RUNTIMES = {"local-jvm": {}, "flink-jvm": {"parallelism": 2}}

pytestmark = pytest.mark.usefixtures("af")


def _outcome_or_skip(outcome: Outcome) -> None:
    if outcome.status == "skip":
        pytest.skip(outcome.reason or "unsupported")
    assert outcome.status == "pass", "\n".join([outcome.fixture_id, *outcome.problems])


@pytest.mark.parametrize("runtime_name", sorted(RUNTIMES))
@pytest.mark.parametrize("fixture_path", FIXTURES, ids=lambda p: p.stem)
def test_fixture(fixture_path: Path, runtime_name: str):
    rt = get_runtime(runtime_name, **RUNTIMES[runtime_name])
    _outcome_or_skip(run_fixture(fixture_path, rt, load_comparator()))


@pytest.mark.parametrize("runtime_name", sorted(RUNTIMES))
@pytest.mark.parametrize("fixture_path", FIXTURES, ids=lambda p: p.stem)
def test_fixture_results_validate_against_result_schema(fixture_path: Path, runtime_name: str):
    fixture = _load(fixture_path)
    rt = get_runtime(runtime_name, **RUNTIMES[runtime_name])
    caps = rt.capabilities()
    unsupported = [c for c in fixture["requires"] if caps.get(c) not in ("supported", "partial")]
    if unsupported:
        rt.close()
        pytest.skip(f"{runtime_name} does not support {unsupported}")
    try:
        rt.deploy(fixture_workflow(fixture_path, fixture))
        results = _drive(rt, fixture["turns"])
    finally:
        rt.close()
    assert len(results) == len(fixture["expect"])
    for r in results:
        RESULT_VALIDATOR.validate(r)
        assert type(r) is dict and all(type(k) is str for k in r)


def test_fixture_ids_match_their_file_names():
    for p in FIXTURES:
        assert p.stem.endswith(yaml.safe_load(p.read_text())["id"]), p

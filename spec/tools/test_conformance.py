"""Every v1 fixture must pass against the reference runtime, and every spec document in
the repository must validate against the v1 schemas."""

from pathlib import Path

import pytest

from run_conformance import FIXTURES, load, run_fixture
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

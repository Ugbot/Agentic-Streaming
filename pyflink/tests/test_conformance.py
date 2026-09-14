"""The 24 fixtures of ``spec/conformance/v1`` on the local MiniCluster through PyFlink.

A fixture whose ``requires`` is not covered by :data:`CONFORMANCE_CAPABILITIES` is reported as a
pytest skip with that reason; everything else must pass with zero problems. Nothing here is
skipped because infrastructure is missing: an unbuilt JVM side raises.
"""

from __future__ import annotations

import pytest

from agentic_pyflink.conformance import fixture_files, load, run_fixture

FIXTURES = fixture_files()


def test_all_twenty_four_fixtures_are_present() -> None:
    assert len(FIXTURES) == 24


@pytest.mark.parametrize("path", FIXTURES, ids=[load(p)["id"] for p in FIXTURES])
def test_fixture_passes_on_pyflink(path, jars) -> None:
    outcome = run_fixture(path)
    if outcome.skipped:
        pytest.skip(outcome.skip_reason)
    assert outcome.passed, "\n".join(outcome.problems)

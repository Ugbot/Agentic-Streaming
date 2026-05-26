"""Pytest fixtures shared across the suite.

We start the JVM once per session and put every framework dep on the
classpath so the wrappers' lazy ``jclass(...)`` calls all resolve.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[2]


def _resolve_classpath() -> list[str]:
    """Build the full runtime classpath. Cache it next to this file so we
    don't shell out to Maven on every test run."""
    cache = HERE.parent / ".cp"
    if not cache.exists():
        # Materialize the runtime classpath via Maven.
        subprocess.run(
            [
                "mvn",
                "-q",
                "dependency:build-classpath",
                f"-Dmdep.outputFile={cache}",
            ],
            check=True,
            cwd=str(REPO_ROOT),
        )
    return cache.read_text().strip().split(":")


@pytest.fixture(scope="session", autouse=True)
def jvm():
    """Boot the JVM once for the whole test session."""
    sys.path.insert(0, str(REPO_ROOT / "python"))
    import agentic_flink as af

    extra = _resolve_classpath()
    af.start_jvm(extra_jars=extra)
    assert af.is_started()
    yield af
    # NOTE: JPype JVMs can't restart in the same process; we deliberately do
    # not call shutdown_jvm() so subsequent test sessions reuse the same
    # process if needed (pytest -x reruns).


@pytest.fixture
def af(jvm):
    """Convenience alias — most tests want the top-level package."""
    return jvm

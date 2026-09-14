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
    """The framework's ``provided`` dependencies (Flink and friends), materialized by Maven and
    cached in ``python/tests/.cp``. The cache is machine specific (absolute ``~/.m2`` paths), so
    it is not committed and is rebuilt whenever any cached jar is missing."""
    cache = HERE.parent / ".cp"
    entries = [e for e in cache.read_text().strip().split(":") if e] if cache.exists() else []
    if not entries or not all(Path(e).exists() for e in entries):
        subprocess.run(
            [str(REPO_ROOT / "mvnw"), "-q", "dependency:build-classpath", f"-Dmdep.outputFile={cache}"],
            check=True,
            cwd=str(REPO_ROOT),
        )
        entries = [e for e in cache.read_text().strip().split(":") if e]
    return entries


@pytest.fixture(scope="session", autouse=True)
def jvm():
    """Boot the JVM once for the whole test session."""
    sys.path.insert(0, str(REPO_ROOT / "python"))
    import agentic_flink as af

    from agentic_flink._classpath import MissingJarError, pekko_jars

    extra = _resolve_classpath()
    # The agentic-pekko jars go first when built (their Jackson is newer than the shaded one),
    # so the `pekko` runtime is reachable in the same JVM. Without them, pekko tests fail
    # with RuntimeNotAvailableError naming the build step.
    try:
        prepend = pekko_jars()
    except MissingJarError:
        prepend = []
    af.start_jvm(extra_jars=extra, prepend_jars=prepend)
    assert af.is_started()
    yield af
    # NOTE: JPype JVMs can't restart in the same process; we deliberately do
    # not call shutdown_jvm() so subsequent test sessions reuse the same
    # process if needed (pytest -x reruns).


@pytest.fixture
def af(jvm):
    """Convenience alias — most tests want the top-level package."""
    return jvm

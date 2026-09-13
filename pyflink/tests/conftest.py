"""Shared fixtures. Tests that need the JVM side fail loudly if the jars are missing (no skips)."""

from __future__ import annotations

import random
import string
from pathlib import Path

import pytest

from agentic_pyflink import load_workflow
from agentic_pyflink.jars import classpath_jars, repo_root

REPO = repo_root()
assert REPO is not None, "tests must run from a repository checkout (pyflink/ inside Agentic-Streaming)"
SUPPORT_WORKFLOW = REPO / "spec" / "conformance" / "v1" / "workflows" / "support.yaml"
BANKING_WORKFLOW = REPO / "examples" / "pipelines" / "banking.yaml"


@pytest.fixture(scope="session")
def jars() -> list[Path]:
    """Raises JarNotFoundError (with the mvn commands to run) when the JVM side is not built."""
    return classpath_jars()


@pytest.fixture(scope="session")
def support_workflow(jars):
    return load_workflow(SUPPORT_WORKFLOW)


@pytest.fixture(scope="session")
def banking_workflow(jars):
    return load_workflow(BANKING_WORKFLOW)


@pytest.fixture
def rand_id():
    def make(prefix: str) -> str:
        return prefix + "-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))

    return make

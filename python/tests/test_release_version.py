"""python/tools/check_release_version.py: the tag, pyproject.toml and the artifacts must agree."""

from __future__ import annotations

import importlib.util
import random
from pathlib import Path

import pytest

TOOL = Path(__file__).resolve().parents[1] / "tools" / "check_release_version.py"
spec = importlib.util.spec_from_file_location("check_release_version", TOOL)
crv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crv)


def _version() -> str:
    return f"{random.randint(0, 9)}.{random.randint(0, 20)}.{random.randint(0, 20)}" + random.choice(["", "a1", "rc2", ".post1"])


def _pyproject(tmp_path: Path, version: str) -> Path:
    p = tmp_path / "pyproject.toml"
    p.write_text(f'[project]\nname = "agentic-flink"\nversion = "{version}"\n')
    return p


@pytest.mark.parametrize("prefix", ["", "v", "refs/tags/v"])
def test_tag_forms_are_normalised(prefix):
    v = _version()
    assert crv.version_from_tag(prefix + v) == v


def test_matching_tag_passes_and_returns_the_version(tmp_path):
    v = _version()
    assert crv.check("v" + v, pyproject=_pyproject(tmp_path, v)) == v


def test_mismatched_tag_fails_naming_both_versions(tmp_path):
    declared, tagged = _version(), _version() + "b9"
    with pytest.raises(SystemExit) as info:
        crv.check("v" + tagged, pyproject=_pyproject(tmp_path, declared))
    assert tagged in str(info.value) and declared in str(info.value)


def test_artifacts_with_another_version_fail(tmp_path):
    v = _version()
    wheel = tmp_path / f"agentic_flink-{v}-py3-none-any.whl"
    sdist = tmp_path / f"agentic_flink-{v}.tar.gz"
    assert crv.check("v" + v, [wheel, sdist], pyproject=_pyproject(tmp_path, v)) == v
    stale = tmp_path / "agentic_flink-0.0.0-py3-none-any.whl"
    with pytest.raises(SystemExit, match="0.0.0"):
        crv.check("v" + v, [stale], pyproject=_pyproject(tmp_path, v))


def test_empty_tag_is_rejected():
    with pytest.raises(SystemExit, match="empty tag"):
        crv.version_from_tag("refs/tags/")


def test_repository_pyproject_is_readable():
    assert crv.version_from_pyproject() == crv.check("v" + crv.version_from_pyproject())

"""tools/check_release_version.py: the tag, setuptools-scm and the built artifacts must agree."""

from __future__ import annotations

import importlib.util
import random
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TOOL = REPO / "tools" / "check_release_version.py"
spec = importlib.util.spec_from_file_location("check_release_version", TOOL)
crv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crv)

DISTS = ("pyagentic", "agentic-flink", "agentic-pyflink", "agentic-pipeline")


def _version() -> str:
    return (f"{random.randint(0, 9)}.{random.randint(0, 20)}.{random.randint(0, 20)}"
            + random.choice(["", "a1", "b3", "rc2", ".post1", ".dev4"]))


def _artifacts(tmp_path: Path, version: str, dists=DISTS) -> list[Path]:
    out = []
    for dist in dists:
        file_stem = dist.replace("-", "_")
        out.append(tmp_path / f"{file_stem}-{version}-py3-none-any.whl")
        out.append(tmp_path / f"{file_stem}-{version}.tar.gz")
    return out


@pytest.mark.parametrize("prefix", ["", "v", "refs/tags/v"])
def test_tag_forms_are_normalised(prefix):
    v = _version()
    assert crv.version_from_tag(prefix + v) == v


@pytest.mark.parametrize("bad", ["1.0.0-rc1", "01.0.0", "1.0.0.a1", "1.0.0RC1", "1.0.0+local", "release-1", "v1.0.0.", "1.0.0rc"])
def test_non_canonical_tags_are_rejected(bad):
    with pytest.raises(SystemExit, match="canonical PEP 440"):
        crv.version_from_tag(bad)


def test_empty_tag_is_rejected():
    with pytest.raises(SystemExit, match="empty tag"):
        crv.version_from_tag("refs/tags/")


def test_project_table_points_at_the_four_pyproject_files():
    assert set(crv.PROJECTS) == set(DISTS)
    for rel in crv.PROJECTS.values():
        assert (crv.REPO_ROOT / rel / "pyproject.toml").is_file(), rel


def test_artifact_names_yield_distribution_and_version(tmp_path):
    v = _version()
    for path in _artifacts(tmp_path, v):
        dist, built = crv.version_from_artifact(path)
        assert dist in DISTS and built == v
    with pytest.raises(SystemExit, match="neither a wheel"):
        crv.version_from_artifact(tmp_path / "agentic_flink-1.0.0.zip")


def test_matching_tag_and_artifacts_pass_without_scm(tmp_path):
    v = _version()
    assert crv.check("v" + v, _artifacts(tmp_path, v), scm=False, complete=True) == v


def test_artifacts_with_another_version_fail(tmp_path):
    v = _version()
    stale = tmp_path / f"agentic_flink-9{v}-py3-none-any.whl"
    with pytest.raises(SystemExit, match=f"9{v}"):
        crv.check("v" + v, [stale], scm=False)


def test_artifacts_from_an_unknown_distribution_fail(tmp_path):
    v = _version()
    stranger = tmp_path / f"something_else-{v}-py3-none-any.whl"
    with pytest.raises(SystemExit, match="something-else"):
        crv.check("v" + v, [stranger], scm=False)


def test_complete_requires_sdist_and_wheel_for_every_project(tmp_path):
    v = _version()
    partial = _artifacts(tmp_path, v, dists=DISTS[:-1]) + [tmp_path / f"agentic_pipeline-{v}.tar.gz"]
    with pytest.raises(SystemExit, match="missing artifacts: agentic-pipeline wheel"):
        crv.check("v" + v, partial, scm=False, complete=True)
    assert crv.check("v" + v, partial, scm=False, complete=False) == v


def test_scm_disagreement_names_the_project_and_both_versions(tmp_path, monkeypatch):
    v, other = _version(), _version() + "b7"
    monkeypatch.setattr(crv, "version_from_scm", lambda project_dir, python=None: other)
    with pytest.raises(SystemExit) as info:
        crv.check("v" + v, projects={"pyagentic": Path("ports/pyagentic")})
    message = str(info.value)
    assert v in message and other in message and "ports/pyagentic" in message


def test_scm_versions_of_the_four_projects_agree_with_each_other():
    """Whatever the checkout resolves to, all four projects must resolve to the same version."""
    if subprocess.run([crv.sys.executable, "-m", "setuptools_scm", "--help"], capture_output=True).returncode != 0:
        pytest.skip("setuptools-scm is not installed in this interpreter")
    versions = {dist: crv.version_from_scm(crv.REPO_ROOT / rel) for dist, rel in crv.PROJECTS.items()}
    assert len(set(versions.values())) == 1, versions

"""The build hook in python/setup.py must put the shaded framework jar into package data.

Exercises every branch of ``ensure_framework_jar`` against temporary directories (no Maven
run, no JVM); the end-to-end proof is ``tests/smoke_clean_install.py`` on a built wheel.
"""

from __future__ import annotations

import importlib.util
import random
import string
from pathlib import Path

import pytest

SETUP_PY = Path(__file__).resolve().parents[1] / "setup.py"


@pytest.fixture
def hook(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("agentic_flink_setup", SETUP_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    repo = tmp_path / "repo"
    (repo / "python" / "agentic_flink").mkdir(parents=True)
    monkeypatch.setattr(module, "REPO_ROOT", repo)
    monkeypatch.setattr(module, "JARS_DIR", repo / "python" / "agentic_flink" / "jars")
    monkeypatch.setattr(module, "MVNW", repo / "mvnw")
    monkeypatch.setattr(module, "CORE_POM", repo / "ports" / "jagentic-core" / "pom.xml")
    monkeypatch.delenv(module.OVERRIDE_ENV, raising=False)
    monkeypatch.delenv(module.SKIP_ENV, raising=False)
    return module


def _fake_jar(directory: Path, version: str | None = None) -> Path:
    version = version or "".join(random.choices(string.digits, k=3))
    directory.mkdir(parents=True, exist_ok=True)
    jar = directory / f"agentic-flink-{version}-uber.jar"
    jar.write_bytes(random.randbytes(64))
    return jar


def test_present_jar_is_reused(hook):
    existing = _fake_jar(hook.JARS_DIR)
    assert hook.ensure_framework_jar() == existing


def test_override_env_is_copied_into_package_data(hook, tmp_path, monkeypatch):
    src = _fake_jar(tmp_path / "elsewhere")
    monkeypatch.setenv(hook.OVERRIDE_ENV, str(src))
    out = hook.ensure_framework_jar()
    assert out.parent == hook.JARS_DIR and out.read_bytes() == src.read_bytes()


def test_override_env_pointing_nowhere_fails(hook, tmp_path, monkeypatch):
    monkeypatch.setenv(hook.OVERRIDE_ENV, str(tmp_path / "missing.jar"))
    with pytest.raises(hook.JarBuildError, match=hook.OVERRIDE_ENV):
        hook.ensure_framework_jar()


def test_target_jar_is_copied_without_running_maven(hook, monkeypatch):
    src = _fake_jar(hook.REPO_ROOT / "target")
    monkeypatch.setattr(hook, "_run_maven", lambda args: pytest.fail("Maven must not run"))
    out = hook.ensure_framework_jar()
    assert out.parent == hook.JARS_DIR and out.name == src.name


def test_maven_is_run_in_a_checkout_when_no_jar_exists(hook, monkeypatch):
    hook.MVNW.write_text("#!/bin/sh\n")
    hook.CORE_POM.parent.mkdir(parents=True)
    hook.CORE_POM.write_text("<project/>")
    calls: list[list[str]] = []

    def fake_maven(args):
        calls.append(args)
        if "package" in args:
            _fake_jar(hook.REPO_ROOT / "target")

    monkeypatch.setattr(hook, "_run_maven", fake_maven)
    out = hook.ensure_framework_jar()
    assert out.parent == hook.JARS_DIR
    assert calls[0][:3] == ["-q", "-f", str(hook.CORE_POM)] and "install" in calls[0]
    assert calls[1] == ["-q", "-DskipTests", "package"]


def test_maven_that_produces_no_jar_fails(hook, monkeypatch):
    hook.MVNW.write_text("#!/bin/sh\n")
    hook.CORE_POM.parent.mkdir(parents=True)
    hook.CORE_POM.write_text("<project/>")
    monkeypatch.setattr(hook, "_run_maven", lambda args: None)
    with pytest.raises(hook.JarBuildError, match="produced no agentic-flink"):
        hook.ensure_framework_jar()


def test_outside_a_checkout_fails_with_instructions(hook):
    with pytest.raises(hook.JarBuildError) as info:
        hook.ensure_framework_jar()
    assert hook.OVERRIDE_ENV in str(info.value) and hook.SKIP_ENV in str(info.value)


def test_skip_env_bypasses_the_hook(hook, monkeypatch):
    monkeypatch.setenv(hook.SKIP_ENV, "1")
    monkeypatch.setattr(hook, "ensure_framework_jar", lambda: pytest.fail("must not resolve a jar"))
    hook._bundle_jar_unless_skipped()

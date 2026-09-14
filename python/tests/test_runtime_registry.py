"""Runtime discovery: entry points + ``register_runtime``; unavailable runtimes raise an
actionable error and never fall back; missing jars fail with the fix in the message."""

from __future__ import annotations

import os
import random
import string
from importlib import metadata
from pathlib import Path

import pytest

import agentic_flink as af
from agentic_flink import _classpath
from agentic_flink._contract import (
    KNOWN_EXTRAS,
    CAPABILITY_IDS,
    CAPABILITY_VALUES,
    CONTRACT_SOURCE,
    ENTRY_POINT_GROUP,
    Runtime,
    RuntimeNotAvailableError,
    CapabilityError,
    available_runtimes,
    get_runtime,
    register_runtime,
    unregister_runtime,
)
from agentic_flink.runtimes import FlinkRuntime, JvmLocalRuntime, PekkoRuntime

PYPROJECT = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()


def _rand() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(8))


def test_runtime_is_an_abc_with_the_contract_methods():
    import abc

    assert isinstance(Runtime, abc.ABCMeta)
    for method in ("capabilities", "deploy", "submit", "close"):
        assert method in Runtime.__abstractmethods__, method
    with pytest.raises(TypeError):
        Runtime()  # type: ignore[abstract]
    assert CONTRACT_SOURCE.startswith(("agentic.runtime", "agentic_flink._contract"))


def test_pyproject_declares_the_entry_point_group_for_every_jvm_runtime():
    assert f'[project.entry-points."{ENTRY_POINT_GROUP}"]' in PYPROJECT
    for name, target in (("local-jvm", "agentic_flink.runtimes:local_jvm"),
                         ("flink-jvm", "agentic_flink.runtimes:flink"),
                         ("pekko", "agentic_flink.runtimes:pekko")):
        assert f'{name} = "{target}"' in PYPROJECT


def test_jvm_runtimes_are_selectable_by_name():
    names = available_runtimes()
    assert {"local-jvm", "flink-jvm", "pekko", "local"} <= set(names)
    assert "flink" not in names, "the bare name 'flink' is reserved: flink-jvm (JPype) vs pyflink (agentic-pyflink)"
    installed = {ep.name for ep in metadata.entry_points().select(group=ENTRY_POINT_GROUP)}
    if installed:  # pip-installed: discovery through the entry-point group itself
        assert {"local-jvm", "flink-jvm", "pekko"} <= installed
    rt = get_runtime("local-jvm")
    assert isinstance(rt, JvmLocalRuntime) and isinstance(rt, Runtime)
    assert isinstance(get_runtime("flink-jvm", parallelism=2), FlinkRuntime)
    assert isinstance(get_runtime("pekko"), PekkoRuntime)


def test_capabilities_use_only_spec_vocabulary_and_cover_every_capability():
    for cls in (JvmLocalRuntime, FlinkRuntime, PekkoRuntime):
        caps = cls.__new__(cls)._capabilities  # class-level table, no JVM needed
        assert set(caps) == set(CAPABILITY_IDS)
        assert set(caps.values()) <= set(CAPABILITY_VALUES)


def test_unknown_runtime_raises_and_names_the_extra_without_falling_back():
    name = f"nope-{_rand()}"
    with pytest.raises(RuntimeNotAvailableError) as ei:
        get_runtime(name)
    msg = str(ei.value)
    assert name in msg and "install the package" in msg and "register_runtime" in msg
    assert KNOWN_EXTRAS["flink-jvm"] == "agentic-flink[flink]"  # the hint used when `flink-jvm` is not installed


def test_register_runtime_factory_and_options():
    name = f"custom-{_rand()}"
    seen = {}

    class Fake(Runtime):
        name_ = name

        def __init__(self, **options):
            seen.update(options)

        def capabilities(self):
            return {c: "unsupported" for c in CAPABILITY_IDS}

        def deploy(self, spec):
            raise CapabilityError(name, ["routing (unsupported)"])

        def submit(self, event):
            raise AssertionError("unreachable")

        def close(self):
            pass

    register_runtime(name, Fake)
    try:
        rt = get_runtime(name, parallelism=3)
        assert isinstance(rt, Fake) and seen == {"parallelism": 3}
        with pytest.raises(CapabilityError, match="routing"):
            rt.deploy({})
        with pytest.raises((TypeError, ValueError)):
            register_runtime(f"bad-{_rand()}", "not-callable")  # type: ignore[arg-type]
    finally:
        unregister_runtime(name)
    assert name not in available_runtimes()


def test_deploy_rejects_workflows_needing_unsupported_capabilities(af):
    spec = af.loads(
        "spec_version: agentic/v1\n"
        "agent:\n  id: t\n  router: {kind: keyword, rules: {}, default: p}\n"
        "  paths: {p: {brain: rule, prompt: hi}}\n"
        "timers: [{id: nudge, after_ms: 5000}]\n"
        "cep:\n"
        "  - name: burst\n    key: conversation_id\n    ts: metadata.event_time_ms\n    within: 1000\n"
        "    pattern: [{stage: first, where: {text_contains: x}}]\n"
        "    on_match: {kind: tool, tool: nudge}\n"
    )
    rt = get_runtime("local-jvm")
    with pytest.raises(CapabilityError) as ei:
        rt.deploy(spec)
    assert any(r.startswith("cep ") for r in ei.value.requirements) and ei.value.runtime == "local-jvm"
    # Workflow timers are supported by local-jvm, so they are not what the deploy rejects.
    assert not any(r.startswith("timers ") for r in ei.value.requirements)


def test_missing_framework_jar_message_is_actionable(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("AGENTIC_FLINK_JAR", raising=False)
    monkeypatch.setattr(_classpath, "REPO_ROOT", tmp_path / "nowhere")
    monkeypatch.setattr(_classpath, "BUNDLED_JARS", tmp_path / "jars")
    with pytest.raises(af.MissingJarError) as ei:
        _classpath.framework_jar()
    msg = str(ei.value)
    assert isinstance(ei.value, FileNotFoundError)
    for hint in ("AGENTIC_FLINK_JAR", "agentic_flink/jars/", "mvn -f ports/jagentic-core/pom.xml install"):
        assert hint in msg

    (tmp_path / "jars").mkdir()
    bundled = tmp_path / "jars" / "agentic-flink-9.9.9-uber.jar"
    bundled.write_bytes(b"")
    assert _classpath.framework_jar() == bundled.resolve()  # package data wins once present

    monkeypatch.setenv("AGENTIC_FLINK_JAR", str(tmp_path / "missing.jar"))
    with pytest.raises(af.MissingJarError, match="AGENTIC_FLINK_JAR"):
        _classpath.framework_jar()


def test_missing_flink_and_pekko_jars_messages_are_actionable(monkeypatch, tmp_path: Path):
    for var in ("AGENTIC_FLINK_CLASSPATH", "FLINK_HOME", "AGENTIC_PEKKO_CLASSPATH"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(_classpath, "DEV_CLASSPATH_FILE", tmp_path / "no.cp")
    monkeypatch.setattr(_classpath, "PEKKO_CLASSPATH_FILE", tmp_path / "no-pekko.cp")
    monkeypatch.setattr(_classpath, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(_classpath, "_pyflink_lib", lambda: None)
    with pytest.raises(af.MissingJarError) as fe:
        _classpath.flink_jars()
    assert 'pip install "agentic-flink[flink]"' in str(fe.value) and "FLINK_HOME" in str(fe.value)
    with pytest.raises(af.MissingJarError) as pe:
        _classpath.pekko_jars()
    assert "mvn -f agentic-pekko/pom.xml package" in str(pe.value) and "AGENTIC_PEKKO_CLASSPATH" in str(pe.value)

    monkeypatch.setenv("AGENTIC_PEKKO_CLASSPATH", os.pathsep.join([str(tmp_path / "ghost.jar")]))
    with pytest.raises(af.MissingJarError, match="ghost.jar"):
        _classpath.pekko_jars()

"""Unit tests for the runtime selector. These cover the pure-Python behaviour —
env-var resolution, mode dispatch, lifecycle invariants — without bringing up a
Flink cluster. End-to-end live-cluster verification lives in the notebook
smoke tests.
"""

from __future__ import annotations

import os
import pathlib
import sys

import pytest

HERE = pathlib.Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))

import agentic_flink as af  # noqa: E402  -- after sys.path tweak
from agentic_flink import runtime as rt_mod  # noqa: E402


# ---------------------------------------------------------------------------
# from_env() — mode dispatch via env var or explicit arg
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "env_value,expected_cls",
    [
        ("inproc", rt_mod.InProcRuntime),
        ("session", rt_mod.SessionRuntime),
        ("embedded", rt_mod.EmbeddedClusterRuntime),
        ("INPROC", rt_mod.InProcRuntime),  # case-insensitive
    ],
)
def test_from_env_dispatch(env_value, expected_cls, monkeypatch):
    monkeypatch.setenv("AGENTIC_FLINK_MODE", env_value)
    instance = af.from_env()
    assert isinstance(instance, expected_cls)
    assert instance.mode == expected_cls.mode


def test_from_env_default_is_inproc(monkeypatch):
    monkeypatch.delenv("AGENTIC_FLINK_MODE", raising=False)
    instance = af.from_env()
    assert isinstance(instance, rt_mod.InProcRuntime)


def test_from_env_unknown_mode(monkeypatch):
    monkeypatch.setenv("AGENTIC_FLINK_MODE", "totally-bogus")
    with pytest.raises(rt_mod.RuntimeModeError):
        af.from_env()


def test_explicit_mode_overrides_env(monkeypatch):
    monkeypatch.setenv("AGENTIC_FLINK_MODE", "session")
    instance = af.from_env(mode="inproc")
    assert isinstance(instance, rt_mod.InProcRuntime)


# ---------------------------------------------------------------------------
# SessionRuntime — URL resolution
# ---------------------------------------------------------------------------


def test_session_runtime_honours_env(monkeypatch):
    monkeypatch.setenv("FLINK_REST_URL", "http://remote.example:9999")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    rt = af.session_cluster()
    # Don't start() — we just want to check URL resolution.
    assert rt._base_url == "http://remote.example:9999"
    assert rt.mode == "session"
    assert rt._started is False


def test_session_runtime_explicit_url_wins(monkeypatch):
    monkeypatch.setenv("FLINK_REST_URL", "http://from-env:1111")
    rt = af.session_cluster("http://from-arg:2222")
    assert rt._base_url == "http://from-arg:2222"


def test_session_runtime_default(monkeypatch):
    monkeypatch.delenv("FLINK_REST_URL", raising=False)
    rt = af.session_cluster()
    assert rt._base_url == "http://localhost:8081"


def test_session_runtime_lazy_no_request_on_construct():
    """SessionRuntime must not touch the network until .start() is called.
    Otherwise importing the module would surprise users with a connection
    attempt against a cluster that may not be up."""
    rt = af.session_cluster("http://does-not-exist.invalid:65535")
    # No exception even though that URL is unreachable — we never hit the wire.
    assert rt._started is False
    assert rt._client is None


# ---------------------------------------------------------------------------
# SessionClient — env-var pickup
# ---------------------------------------------------------------------------


def test_session_client_picks_up_env_when_no_url(monkeypatch):
    monkeypatch.setenv("FLINK_REST_URL", "http://from-env:8888")
    from agentic_flink.session import SessionClient

    c = SessionClient()
    assert c.base_url == "http://from-env:8888"


def test_session_client_explicit_url_wins(monkeypatch):
    monkeypatch.setenv("FLINK_REST_URL", "http://from-env:8888")
    from agentic_flink.session import SessionClient

    c = SessionClient("http://explicit:7070")
    assert c.base_url == "http://explicit:7070"


def test_session_client_default_when_unset(monkeypatch):
    monkeypatch.delenv("FLINK_REST_URL", raising=False)
    from agentic_flink.session import SessionClient

    c = SessionClient()
    assert c.base_url == "http://localhost:8081"


# ---------------------------------------------------------------------------
# InProcRuntime — start() is idempotent
# ---------------------------------------------------------------------------


def test_inproc_runtime_start_idempotent(jvm):
    """The conftest fixture already booted the JVM; constructing an
    InProcRuntime and calling start() twice should be a no-op."""
    rt = af.inproc()
    rt.start()
    assert rt._started is True
    # Second call doesn't raise and doesn't try to re-init JPype.
    rt.start()
    assert rt._started is True


def test_inproc_runtime_jclass_resolves(jvm):
    rt = af.inproc()
    rt.start()
    cls = rt.jclass("org.agentic.flink.screening.ScreeningPipeline")
    assert cls is not None


def test_inproc_runtime_jclass_before_start_raises():
    rt = af.inproc()
    # Without start(), should raise JvmNotStartedError — even though the
    # session-wide fixture has booted the JVM, this runtime's _started is False.
    with pytest.raises(af.JvmNotStartedError):
        rt.jclass("org.agentic.flink.screening.ScreeningPipeline")


# ---------------------------------------------------------------------------
# Mode capability boundaries
# ---------------------------------------------------------------------------


def test_inproc_cannot_submit_level():
    rt = af.inproc()
    rt._started = True  # bypass actual start() — we're only testing the API guard
    with pytest.raises(NotImplementedError):
        rt.submit_level("1", in_endpoint="tcp://x:1", out_endpoint="tcp://x:2")


def test_session_cannot_jclass():
    rt = af.session_cluster("http://example:8081")
    # No start() needed for the API guard.
    with pytest.raises(NotImplementedError):
        rt.jclass("org.agentic.flink.screening.ScreeningPipeline")


# ---------------------------------------------------------------------------
# _build_launcher_args parity with SessionClient.submit_level kwargs
# ---------------------------------------------------------------------------


def test_build_launcher_args_minimal():
    args = rt_mod._build_launcher_args("1")
    assert args == ["--level", "1"]


def test_build_launcher_args_full():
    args = rt_mod._build_launcher_args(
        "5",
        in_endpoint="tcp://0.0.0.0:5564",
        out_endpoint="fluss://agentic.alerts",
        control_endpoint="tcp://0.0.0.0:5559",
        debug_sink_endpoint="tcp://0.0.0.0:5558",
        alerts_pub_endpoint="tcp://0.0.0.0:5557",
        anthropic_key="sk-ant-xxx",
        window_ms=5000,
        top_n=5,
        name="L5",
        extra_args={"fluss-bootstrap": "localhost:9120"},
    )
    assert "--level" in args and "5" in args
    assert "--in" in args and "tcp://0.0.0.0:5564" in args
    assert "--out" in args and "fluss://agentic.alerts" in args
    assert "--control" in args and "tcp://0.0.0.0:5559" in args
    assert "--debug-sink" in args and "tcp://0.0.0.0:5558" in args
    assert "--alerts-pub" in args and "tcp://0.0.0.0:5557" in args
    assert "--anthropic-key" in args and "sk-ant-xxx" in args
    assert "--window-ms" in args and "5000" in args
    assert "--top-n" in args and "5" in args
    assert "--name" in args and "L5" in args
    assert "--fluss-bootstrap" in args and "localhost:9120" in args

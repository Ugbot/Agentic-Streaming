"""The `pekko` runtime is reachable only when agentic-pekko is built (see
``agentic_flink._classpath.pekko_jars``); without it, selecting it fails clearly.

The Pekko module on this branch returns only reply/path/tool_calls from ``submit`` (no
``turn_id``/``events``/``state``), so it cannot produce a normalized result yet and every
capability is recorded ``not_tested``; the fixtures therefore all skip. This test proves
what *is* true: discovery, deploy, routing of one turn through the actor system.
"""

from __future__ import annotations

import random
import string

import pytest

from agentic_flink import loads
from agentic_flink._classpath import MissingJarError, pekko_jars
from agentic_flink._contract import RuntimeNotAvailable, get_runtime
from agentic_flink.conformance import default_fixtures_dir, load_comparator, run_fixture
from agentic_flink.workflow import Event

pytestmark = pytest.mark.usefixtures("af")


def _rand() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(8))


def _require_pekko() -> None:
    try:
        pekko_jars()
    except MissingJarError as e:
        pytest.fail(f"pekko infrastructure missing: {e}")


def test_pekko_routes_one_turn_through_the_actor_system():
    _require_pekko()
    keyword = _rand()
    spec = loads(
        "spec_version: agentic/v1\n"
        "agent:\n"
        "  id: pekko-smoke\n"
        f"  router: {{kind: keyword, rules: {{billing: [{keyword}]}}, default: general}}\n"
        "  paths:\n"
        "    billing: {brain: rule, prompt: 'Billing.'}\n"
        "    general: {brain: rule, prompt: 'General.'}\n"
    )
    rt = get_runtime("pekko")
    try:
        rt.deploy(spec)
        result = rt.submit(Event.turn("c1", "t1", f"about {keyword}"))
    finally:
        rt.close()
    assert type(result) is dict
    assert result["conversation_id"] == "c1"
    assert result["path"] == "billing"
    assert isinstance(result["reply"], str) and result["reply"]


def test_pekko_capabilities_are_not_claimed_and_fixtures_skip():
    _require_pekko()
    rt = get_runtime("pekko")
    caps = rt.capabilities()
    assert "supported" not in caps.values() and "partial" not in caps.values()
    path = next(default_fixtures_dir().glob("*routing-keyword.yaml"))
    outcome = run_fixture(path, rt, load_comparator())
    assert outcome.status == "skip" and "does not support" in (outcome.reason or "")


def test_pekko_without_jars_names_the_build_step(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENTIC_PEKKO_CLASSPATH", str(tmp_path / "missing.jar"))
    with pytest.raises((RuntimeNotAvailable, MissingJarError), match="missing.jar"):
        rt = get_runtime("pekko")
        rt.deploy(loads("spec_version: agentic/v1\nagent: {id: a, paths: {p: {brain: rule, prompt: x}}}\n"))

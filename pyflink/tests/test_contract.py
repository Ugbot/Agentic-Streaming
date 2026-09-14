"""The shared contract: ``FlinkRuntime`` is a ``agentic.runtime.Runtime``, resolves through the
shared resolver under the name ``pyflink``, and derives capabilities exactly like the pure core.

These tests need no JVM. They need ``pyagentic`` (``pip install -e ports/pyagentic``) because the
shared resolver lives there; that is the whole point of the check.
"""

from __future__ import annotations

import dataclasses
import random
from importlib import metadata
from pathlib import Path

import pytest
import yaml
from agentic.runtime import ENTRY_POINT_GROUP, Runtime, available_runtimes, get_runtime
from agentic.runtime import required_capabilities as canonical_required_capabilities

from agentic_pyflink import _contract
from agentic_pyflink.capabilities import CAPABILITY_IDS
from agentic_pyflink.connectors import turn_document
from agentic_pyflink.runtime import FlinkRuntime
from agentic_pyflink.workflow import required_capabilities
from tests.conftest import REPO

PYPROJECT = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")
FIXTURES = sorted((REPO / "spec" / "conformance" / "v1" / "fixtures").glob("*.yaml"))


def _installed_entry_points() -> dict[str, str]:
    return {ep.name: ep.value for ep in metadata.entry_points().select(group=ENTRY_POINT_GROUP)}


def test_contract_comes_from_the_pure_package_when_installed() -> None:
    assert _contract.CONTRACT_SOURCE == "agentic.runtime"
    assert _contract.Runtime is Runtime


def test_flink_runtime_is_a_shared_runtime_named_pyflink() -> None:
    assert issubclass(FlinkRuntime, Runtime)
    assert FlinkRuntime.name == "pyflink"
    rt = FlinkRuntime()
    assert isinstance(rt, Runtime)
    rt.close()


def test_entry_point_is_pyflink_not_flink() -> None:
    assert 'pyflink = "agentic_pyflink.runtime:FlinkRuntime"' in PYPROJECT
    assert '\nflink = ' not in PYPROJECT
    installed = _installed_entry_points()
    if "pyflink" not in installed:
        pytest.fail(
            "agentic-pyflink is not installed as a distribution, so its entry point cannot be resolved; "
            "run `pip install -e pyflink` before the test suite"
        )
    assert installed["pyflink"] == "agentic_pyflink.runtime:FlinkRuntime"
    assert available_runtimes()["pyflink"].startswith("entry point agentic_pyflink")
    jvm_facade = installed.get("flink-jvm")
    assert jvm_facade is None or jvm_facade.startswith("agentic_flink.")
    assert "flink" not in installed, "the bare name 'flink' must not be registered by any package"


def test_shared_resolver_returns_a_flink_runtime_that_passes_the_abc_check() -> None:
    parallelism = random.randint(1, 4)
    rt = get_runtime("pyflink", parallelism=parallelism)
    try:
        assert isinstance(rt, FlinkRuntime) and isinstance(rt, Runtime)
        assert rt.config.parallelism == parallelism
        assert set(rt.capabilities()) == set(CAPABILITY_IDS)
    finally:
        rt.close()


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: p.stem)
def test_required_capabilities_match_the_canonical_derivation(fixture: Path) -> None:
    doc = yaml.safe_load(fixture.read_text(encoding="utf-8"))
    workflow = doc.get("workflow") or yaml.safe_load((fixture.parent / doc["workflow_ref"]).read_text(encoding="utf-8"))
    assert required_capabilities(workflow) == canonical_required_capabilities(workflow)


def test_turn_document_accepts_the_shared_turn_dataclass() -> None:
    from agentic.events import Turn

    cid, tid = f"c-{random.randrange(10**6)}", f"t-{random.randrange(10**6)}"
    turn = Turn(conversation_id=cid, turn_id=tid, text="hello", metadata={"k": "v"})
    assert dataclasses.is_dataclass(turn)
    doc = turn_document(turn)
    assert doc == {"conversation_id": cid, "turn_id": tid, "user_id": "anonymous", "text": "hello", "metadata": {"k": "v"}}
    assert doc == turn_document(dataclasses.asdict(turn))
    with pytest.raises(ValueError, match="mapping or a dataclass"):
        turn_document("not a turn")  # type: ignore[arg-type]

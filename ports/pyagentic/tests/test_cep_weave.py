"""Declarative CEP woven into the pipeline loader — the Python parity tests for the Java
``CepSpec``/``CepWiring`` + ``PipelineLoader`` weave.

Covers: the ``submit`` action firing once + the DERIVED recursion guard; the ``tool``
action; the ``where`` mini-language (text_contains / metadata_equals / metadata_gt); and
an end-to-end integration loading ``examples/pipelines/incident.yaml`` and asserting the
escalation path fired for a host after 3 anomalies.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

_PORTS = Path(__file__).resolve().parents[2]
# Make the agentic-pipeline loader importable for the integration test.
sys.path.insert(0, str(_PORTS / "agentic-pipeline"))

from pyagentic.cep import DERIVED, CepWiring, compile_cep  # noqa: E402
from pyagentic.core import Event, TurnResult  # noqa: E402

_REPO = Path(__file__).resolve().parents[3]
INCIDENT_YAML = _REPO / "examples" / "pipelines" / "incident.yaml"


class RecordingRuntime:
    """A runtime that records every submitted event (so a ``submit`` action's derived
    event is observable) and returns a trivial TurnResult."""

    def __init__(self) -> None:
        self.submitted: List[Event] = []

    def submit(self, event: Event) -> TurnResult:
        self.submitted.append(event)
        return TurnResult(event.conversation_id, "ok")


class RecordingTools:
    """A minimal ToolRegistry-shaped object that records ``execute`` calls."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def execute(self, tool_id: str, params: Dict[str, Any]) -> Any:
        self.calls.append({"tool": tool_id, "args": dict(params or {})})
        return "TICKET-OPENED"


def _incident_submit_spec() -> List[Dict[str, Any]]:
    return [{
        "name": "host_incident",
        "key": "conversation_id",
        "ts": "metadata.ts",
        "within": 300_000,
        "pattern": [
            {"stage": "first", "where": {"text_contains": "anomaly"}},
            {"stage": "second", "where": {"text_contains": "anomaly"}, "contiguity": "followedBy"},
            {"stage": "third", "where": {"text_contains": "anomaly"}, "contiguity": "followedBy"},
        ],
        "on_match": {"kind": "submit", "text": "incident on {key}"},
    }]


def _anomaly(host: str, ts: int) -> Event:
    return Event(conversation_id=host, text="anomaly cpu", user_id="monitor", metadata={"ts": str(ts)})


def test_submit_action_fires_once_and_guards_recursion() -> None:
    wirings = compile_cep(_incident_submit_spec())
    assert len(wirings) == 1
    wiring = wirings[0]
    runtime = RecordingRuntime()
    tools = RecordingTools()

    # First two anomalies advance partials but complete nothing.
    wiring.on_event(_anomaly("h1", 0), runtime, tools)
    wiring.on_event(_anomaly("h1", 60_000), runtime, tools)
    assert runtime.submitted == []

    # Third anomaly completes the pattern -> exactly one derived event.
    wiring.on_event(_anomaly("h1", 120_000), runtime, tools)
    assert len(runtime.submitted) == 1
    derived = runtime.submitted[0]
    assert derived.text == "incident on h1"
    assert derived.conversation_id == "h1"
    assert derived.user_id == "cep"
    assert derived.metadata.get(DERIVED) == "true"

    # Feeding the derived event back in fires nothing (recursion guard).
    wiring.on_event(derived, runtime, tools)
    assert len(runtime.submitted) == 1


def test_tool_action_fires_once_with_args() -> None:
    spec = _incident_submit_spec()
    spec[0]["on_match"] = {"kind": "tool", "tool": "open_ticket", "args": {"sev": "high"}}
    wiring = compile_cep(spec)[0]
    runtime = RecordingRuntime()
    tools = RecordingTools()

    for ts in (0, 60_000, 120_000):
        wiring.on_event(_anomaly("h2", ts), runtime, tools)

    assert tools.calls == [{"tool": "open_ticket", "args": {"sev": "high"}}]
    assert runtime.submitted == []  # tool action doesn't submit


def _fire_single_stage(where: Dict[str, Any], event: Event) -> bool:
    """Compile a 1-stage detect-only rule with ``where`` and report whether ``event``
    completes it (i.e. the condition matched)."""
    spec = [{
        "name": "probe",
        "key": "conversation_id",
        "pattern": [{"stage": "only", "where": where}],
        "on_match": None,  # detect-only; we observe via a recording wrapper below
    }]
    fired: List[bool] = []

    # Wrap with a recording action by recompiling the matcher directly.
    wiring = compile_cep(spec)[0]

    # Replace the detect-only action with a recorder to observe completion.
    def record(match, key, runtime, tools):  # noqa: ANN001
        fired.append(True)

    wiring._action = record  # type: ignore[attr-defined]
    wiring.on_event(event, RecordingRuntime(), RecordingTools())
    return bool(fired)


def test_condition_mini_language() -> None:
    # metadata_gt: float(metadata[score]) > 0.9
    gt = {"metadata_gt": {"score": 0.9}}
    assert _fire_single_stage(gt, Event("c", "x", metadata={"score": "0.95"})) is True
    assert _fire_single_stage(gt, Event("c", "x", metadata={"score": "0.5"})) is False
    assert _fire_single_stage(gt, Event("c", "x", metadata={})) is False

    # metadata_equals: metadata[sev] == "high"
    eq = {"metadata_equals": {"sev": "high"}}
    assert _fire_single_stage(eq, Event("c", "x", metadata={"sev": "high"})) is True
    assert _fire_single_stage(eq, Event("c", "x", metadata={"sev": "low"})) is False

    # text_contains: any needle
    tc = {"text_contains": ["anomaly", "alert"]}
    assert _fire_single_stage(tc, Event("c", "anomaly cpu high")) is True
    assert _fire_single_stage(tc, Event("c", "all clear")) is False


def test_integration_incident_yaml_escalates_host() -> None:
    from agentic_pipeline.loader import load
    from pyagentic.core import PATH_ATTR  # the attribute the RoutedGraph writes the chosen path to

    assert INCIDENT_YAML.exists(), f"missing example: {INCIDENT_YAML}"
    system = load(str(INCIDENT_YAML))
    assert system.cep, "incident.yaml should compile a cep wiring"

    host = "host-7"
    for ts in (0, 60_000, 120_000):
        system.submit(Event(conversation_id=host, text="anomaly: cpu high",
                            user_id="monitor", metadata={"ts": str(ts)}))

    # The 3rd anomaly fires the CEP submit action -> a derived "incident: ..." event that
    # routes through the keyword router to the escalate path. The chosen path is persisted
    # to the conversation store under PATH_ATTR ("graph.path").
    store = system.backend.store
    assert store.get_attribute(host, PATH_ATTR) == "escalate"

    # And the derived escalate turn lands in the host's transcript.
    transcript = " ".join(m.content for m in store.history(host))
    assert "incident: 3 anomalies on host-7" in transcript

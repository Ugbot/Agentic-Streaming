"""What the fixtures cannot show: a limit follows the principal, and the operator's switch."""

from pathlib import Path

from agentic.events import Turn
from agentic.limits import Limits
from agentic.runtime import LocalRuntime
from agentic.spec import Agent


def limits(block: dict, env: dict = {}) -> Limits:  # noqa: B006 - never written to
    return Limits({"policies": {"limits": block}}, env)


def test_a_rate_follows_the_principal_across_conversations_and_tenants_do_not_share_one() -> None:
    doc = Agent("limited").path("main").verifier("none").build().document
    rt = LocalRuntime()
    rt.deploy({**doc, "policies": {"limits": {"turns_per_minute": 1}}})
    alice = {"subject": "alice", "tenant": "acme"}
    assert rt.submit(Turn("c1", "t", "hi", principal=alice))["status"] == "completed"
    second = rt.submit(Turn("c2", "t", "hi", principal=alice))
    assert (second["status"], second["error"]["class"]) == ("failed", "policy")
    other = rt.submit(Turn("c3", "t", "hi", principal={"subject": "alice", "tenant": "beta"}))
    assert other["status"] == "completed"


def test_the_operators_switch_works_whatever_the_document_says(tmp_path: Path) -> None:
    file = tmp_path / "stop"
    switched = limits({}, {"AGENTIC_KILL_SWITCH_FILE": str(file)})
    assert not switched.stopped() and switched.admit_turn("a", "t", 0) is None
    file.write_text("")
    assert switched.stopped(), "no restart needed"
    assert switched.admit_turn("a", "t", 0)[0] == "kill_switch"
    assert switched.admit_model("a", 0)[0] == "kill_switch"
    assert switched.admit_tool("a", 0, "pay", False)[0] == "kill_switch"
    assert limits({}, {"AGENTIC_KILL_SWITCH": "true"}).stopped()


def test_a_call_already_begun_does_not_count_against_the_quota() -> None:
    quota = limits({"tool_calls_per_day": {"pay": 1}})
    assert quota.admit_tool("a", 0, "pay", False) is None
    assert quota.admit_tool("a", 0, "pay", True) is None
    assert quota.admit_tool("a", 0, "pay", False) is not None
    assert quota.admit_tool("a", 86_400_000, "pay", False) is None, "a new day"
    assert quota.admit_tool("b", 0, "pay", False) is None, "another principal"


def test_a_budget_is_spent_by_the_call_that_crosses_it() -> None:
    budget = limits({"tokens_per_day": 10})
    assert budget.admit_model("a", 0) is None
    budget.charge_model("a", 0, 1000)
    assert budget.admit_model("a", 0)[0] == "tokens_per_day"
    assert budget.admit_model("b", 0) is None

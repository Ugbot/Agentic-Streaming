"""The matrix runner: its comparator path, its binding parsers, and the rule that a cell is only
`supported` when a passing fixture proves it."""

import json
from pathlib import Path

import pytest

import conformance_matrix as cm

FIXTURES = cm.fixture_index()


def test_capability_ids_match_primitives() -> None:
    assert cm.capability_ids_from_spec() == cm.CAPABILITIES


def test_every_fixture_requires_only_v1_capabilities() -> None:
    for fixture in FIXTURES.values():
        assert set(fixture["requires"]) <= set(cm.CAPABILITIES), fixture["id"]


def test_reference_runtime_passes_and_is_supported_only_where_a_fixture_proves_it() -> None:
    report = cm.run_reference(FIXTURES)
    assert report.status == cm.RAN
    assert {f.status for f in report.fixtures} == {cm.PASSED}
    cells = cm.derive_capabilities(report)
    proven = {c for f in FIXTURES.values() for c in f["requires"]}
    for cap in cm.CAPABILITIES:
        assert cells[cap]["value"] == ("supported" if cap in proven else "not_tested"), cap
    assert cells["llm_brain"]["note"] == "no v1 fixture requires it"


def test_runner_comparator_rejects_schema_violations_and_ignores_runtime_detail() -> None:
    fixture = FIXTURES["routing-keyword"]
    runtime = cm.ReferenceRuntime(fixture["workflow"])
    results = [runtime.submit(cm.Turn(t["conversation_id"], t["turn_id"], t.get("text", ""))) for t in fixture["turns"]]
    assert cm.compare_results(fixture, results) == []

    with_detail = [dict(r, runtime_detail={"checkpoint": "abc", "actor": "/user/x"}) for r in results]
    assert cm.compare_results(fixture, with_detail) == []

    bad = [dict(r, status="done") for r in results]
    problems = cm.compare_results(fixture, bad)
    assert any("invalid against result.schema.json at /status" in p for p in problems)
    assert any("status: expected 'completed', got 'done'" in p for p in problems)

    extra_key = [dict(r, offset=12) for r in results]
    assert any("/" in p and "offset" in p for p in cm.compare_results(fixture, extra_key))


def test_missing_results_are_a_failure_not_a_pass() -> None:
    fixture = FIXTURES["memory-read-write"]
    outcome = cm.outcome_from_results(fixture, [])
    assert outcome.status == cm.FAILED
    assert all("no result produced" in p for p in outcome.problems)


def test_skip_never_counts_as_supported_and_only_against_the_named_capability() -> None:
    report = cm.RuntimeReport("x", "b", "runner", cm.RAN)
    for fixture in FIXTURES.values():
        if "durable_store" in fixture["requires"]:
            report.fixtures.append(cm.skip_outcome(fixture, ["durable_store"]))
        else:
            report.fixtures.append(cm.FixtureOutcome(fixture["id"], cm.PASSED, list(fixture["requires"])))
    cells = cm.derive_capabilities(report)
    assert cells["durable_store"]["value"] == "unsupported"
    assert "replay-after-restart" in cells["durable_store"]["note"]
    # replay is only required by a fixture that was skipped for durable_store: inconclusive, not unsupported
    assert cells["replay"]["value"] == "not_tested"
    # memory is proven by memory-read-write and ordered-concurrent-turns even though 11 was skipped
    assert cells["memory"]["value"] == "supported"


def test_mixed_pass_and_fail_is_partial() -> None:
    report = cm.RuntimeReport("x", "b", "runner", cm.RAN)
    for fixture in FIXTURES.values():
        status = cm.FAILED if fixture["id"] == "retry-tool" else cm.PASSED
        problems = ["tool_calls length: expected 2, got 1"] if status == cm.FAILED else []
        report.fixtures.append(cm.FixtureOutcome(fixture["id"], status, list(fixture["requires"]), problems))
    cells = cm.derive_capabilities(report)
    assert cells["retry"]["value"] == "partial"
    assert "failed retry-tool" in cells["retry"]["note"]
    assert cells["tools"]["value"] == "partial"


def test_runtime_that_did_not_run_is_not_tested_everywhere_with_the_reason() -> None:
    report = cm.RuntimeReport("flink", "b", "binding", cm.NOT_TESTED, reason="toolchain unavailable: mvn not on PATH")
    cells = cm.derive_capabilities(report)
    assert {c["value"] for c in cells.values()} == {"not_tested"}
    assert all(c["note"] == report.reason for c in cells.values())


def test_surefire_parser_maps_dynamic_tests_to_fixtures(tmp_path: Path) -> None:
    cases = []
    for fixture in FIXTURES.values():
        name = fixture["_file"]
        if fixture["id"] == "suspend-resume":
            cases.append(f'<testcase name="{name}" classname="C" time="0.1">'
                         '<skipped message="skip suspend-resume: requires [suspend_resume]"/></testcase>')
        elif fixture["id"] == "retry-tool":
            cases.append(f'<testcase name="{name}" classname="C" time="0.1">'
                         '<failure message="FAIL retry-tool&#10;  tool_calls length: expected 2, got 1" type="AssertionFailedError">trace</failure></testcase>')
        else:
            cases.append(f'<testcase name="{name}" classname="C" time="0.1"/>')
    cases.append('<testcase name="comparatorMatchesReferenceRules" classname="C" time="0.0"/>')
    xml = '<?xml version="1.0"?><testsuite name="C" tests="16">' + "".join(cases) + "</testsuite>"
    (tmp_path / "TEST-org.example.ConformanceTest.xml").write_text(xml, encoding="utf-8")

    outcomes = {o.fixture: o for o in cm.parse_surefire(tmp_path, FIXTURES, "ConformanceTest")}
    assert len(outcomes) == 15
    assert outcomes["suspend-resume"].status == cm.SKIPPED
    assert outcomes["suspend-resume"].missing == ["suspend_resume"]
    assert outcomes["retry-tool"].status == cm.FAILED
    assert outcomes["retry-tool"].problems[0].startswith("FAIL retry-tool")
    assert outcomes["routing-keyword"].status == cm.PASSED


def test_surefire_parser_maps_factory_named_cases_by_position(tmp_path: Path) -> None:
    cases = []
    for fixture in FIXTURES.values():
        if fixture["id"] == "a2a-delegation":
            cases.append('<testcase name="fixtures" classname="C"><skipped message="skip a2a-delegation: requires [a2a]"/></testcase>')
        else:
            cases.append('<testcase name="fixtures" classname="C"/>')
    cases.append('<testcase name="comparatorMatchesReferenceRules" classname="C"/>')
    (tmp_path / "TEST-org.example.PekkoConformanceTest.xml").write_text(
        '<testsuite name="C">' + "".join(cases) + "</testsuite>", encoding="utf-8")
    outcomes = {o.fixture: o for o in cm.parse_surefire(tmp_path, FIXTURES, "PekkoConformanceTest")}
    assert len(outcomes) == 15
    assert outcomes["a2a-delegation"].status == cm.SKIPPED and outcomes["a2a-delegation"].missing == ["a2a"]
    assert outcomes["retrieval"].status == cm.PASSED

    wrong = '<testsuite name="C">' + "".join(
        '<testcase name="fixtures" classname="C"><skipped message="skip retrieval: requires [retrieval]"/></testcase>'
        if i == 0 else '<testcase name="fixtures" classname="C"/>' for i in range(15)) + "</testsuite>"
    (tmp_path / "TEST-org.example.PekkoConformanceTest.xml").write_text(wrong, encoding="utf-8")
    with pytest.raises(cm.BindingError, match="reports \\['retrieval'\\]"):
        cm.parse_surefire(tmp_path, FIXTURES, "PekkoConformanceTest")

    short = '<testsuite name="C">' + '<testcase name="fixtures" classname="C"/>' * 14 + "</testsuite>"
    (tmp_path / "TEST-org.example.PekkoConformanceTest.xml").write_text(short, encoding="utf-8")
    with pytest.raises(cm.BindingError, match="14 dynamic tests"):
        cm.parse_surefire(tmp_path, FIXTURES, "PekkoConformanceTest")


def test_surefire_parser_refuses_incomplete_reports(tmp_path: Path) -> None:
    (tmp_path / "TEST-org.example.ConformanceTest.xml").write_text(
        '<testsuite name="C"><testcase name="01-routing-keyword.yaml" classname="C"/></testsuite>', encoding="utf-8")
    with pytest.raises(cm.BindingError, match="reported no test case"):
        cm.parse_surefire(tmp_path, FIXTURES, "ConformanceTest")
    with pytest.raises(cm.BindingError, match="no surefire report"):
        cm.parse_surefire(tmp_path, FIXTURES, "PekkoConformanceTest")


def test_clojure_outcomes_are_recompared_by_the_runner() -> None:
    outcomes = []
    for fixture in FIXTURES.values():
        if fixture["id"] == "retrieval":
            outcomes.append({"id": fixture["id"], "status": "skipped", "problems": ['requires ["retrieval"]']})
            continue
        runtime = cm.ReferenceRuntime(fixture["workflow"])
        results = []
        for t in fixture["turns"]:
            if t.get("restart_runtime"):
                runtime.restart()
            results.append(runtime.submit(cm.Turn(t["conversation_id"], t["turn_id"], t.get("text", ""), t.get("signal"))))
        if fixture["id"] == "routing-default":
            results[0]["path"] = "billing"
        outcomes.append({"id": fixture["id"], "status": "passed", "problems": [], "results": results})

    text = "SKIP retrieval\n@@AGENTIC_CONFORMANCE@@\n" + json.dumps(outcomes) + "\n@@END@@\n"
    parsed = {o.fixture: o for o in cm.outcomes_from_clojure(cm.parse_clojure_output(text), FIXTURES)}
    assert parsed["retrieval"].status == cm.SKIPPED and parsed["retrieval"].missing == ["retrieval"]
    assert parsed["routing-default"].status == cm.FAILED  # the binding said passed; the runner disagrees
    assert parsed["routing-keyword"].status == cm.PASSED


def test_clojure_output_without_marker_is_a_binding_error() -> None:
    with pytest.raises(cm.BindingError):
        cm.parse_clojure_output("Error building classpath")


def test_absent_bindings_and_toolchains_are_reported_not_omitted(tmp_path: Path) -> None:
    reports = [cm.run_runtime(name, FIXTURES, tmp_path, tmp_path / "logs", None)
               for name in ("jvm-core", "flink", "pekko", "clojure", "python")]
    assert [r.status for r in reports] == [cm.NOT_TESTED] * 5
    assert all(r.reason for r in reports)
    artifact = cm.build_artifact(reports, FIXTURES, tmp_path)
    assert [r["name"] for r in artifact["runtimes"]] == ["jvm-core", "flink", "pekko", "clojure", "python"]


def test_python_binding_hook_uses_the_runner_comparator() -> None:
    report = cm.run_python_binding(FIXTURES, "conformance_matrix_test_binding:run")
    assert report.status == cm.RAN
    by_id = {f.fixture: f for f in report.fixtures}
    assert by_id["routing-keyword"].status == cm.PASSED
    assert by_id["saga-compensation"].status == cm.SKIPPED and by_id["saga-compensation"].missing == ["saga"]
    assert by_id["tool-failure"].status == cm.FAILED
    assert cm.derive_capabilities(report)["saga"]["value"] == "unsupported"

    missing = cm.run_python_binding(FIXTURES, "no_such_module:run")
    assert missing.status == cm.ERROR


def test_rendered_docs_use_only_the_four_cell_values() -> None:
    reports = [cm.run_reference(FIXTURES),
               cm.RuntimeReport("flink", "b", "binding", cm.NOT_TESTED, reason="toolchain unavailable: mvn not on PATH")]
    artifact = cm.build_artifact(reports, FIXTURES, cm.ROOT)
    md = cm.render_docs(artifact)
    assert "| Capability | reference | flink |" in md
    assert "| `routing` | supported | not_tested |" in md
    assert "toolchain unavailable: mvn not on PATH" in md
    for row in md.splitlines():
        if row.startswith("| `") and row.count("|") == 4 and "Requires" not in row:
            cells = [c.strip() for c in row.strip("|").split("|")[1:]]
            assert set(cells) <= set(cm.CELL_VALUES), row


def test_main_render_from_artifact(tmp_path: Path) -> None:
    artifact = cm.build_artifact([cm.run_reference(FIXTURES)], FIXTURES, cm.ROOT)
    src = tmp_path / "a.json"
    src.write_text(json.dumps(artifact), encoding="utf-8")
    out = tmp_path / "capabilities.md"
    assert cm.main(["--render", str(src), "--write-docs", str(out)]) == 0
    assert out.read_text(encoding="utf-8").startswith("# Capability matrix, agentic/v1")


def test_require_present_fails_only_for_missing_toolchains(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "ports" / "jagentic-core" / "src" / "test" / "java").mkdir(parents=True)
    (tmp_path / "ports" / "jagentic-core" / "pom.xml").write_text("<project/>", encoding="utf-8")
    (tmp_path / "ports" / "jagentic-core" / "src" / "test" / "java" / "ConformanceTest.java").write_text("", encoding="utf-8")
    monkeypatch.setattr(cm, "MVN", None)
    monkeypatch.setattr(cm, "CLOJURE", None)
    root = str(tmp_path)
    # clojure binding is absent -> not_tested is acceptable; jvm-core binding is present but mvn is missing -> failure
    assert cm.main(["--runtimes", "reference", "clojure", "--require-present", "--root", root]) == 0
    assert cm.main(["--runtimes", "reference", "jvm-core", "--root", root]) == 0
    assert cm.main(["--runtimes", "reference", "jvm-core", "--require-present", "--root", root]) == 1
    assert cm.main(["--runtimes", "reference", "clojure", "--require", "clojure", "--root", root]) == 1

"""Tests for tools/ci/skip_audit.py: run with `python -m pytest tools/ci -q`."""

from __future__ import annotations

import random
import string
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import skip_audit  # noqa: E402

ALLOWLIST = Path(__file__).resolve().parent / "skip-allowlist.txt"


def _word(n: int = 8) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


def _surefire(path: Path, cases: list[tuple[str, str | None]]) -> Path:
    body = []
    for name, skip in cases:
        if skip is None:
            body.append(f'<testcase name="{name}" classname="org.example.{_word()}" time="0.01"/>')
        else:
            body.append(
                f'<testcase name="{name}" classname="org.example.{_word()}" time="0">'
                f'<skipped message="{skip}"/></testcase>')
    path.write_text(f'<?xml version="1.0"?><testsuite name="s" tests="{len(cases)}">{"".join(body)}</testsuite>')
    return path


def _pytest_report(path: Path, cases: list[tuple[str, str | None]]) -> Path:
    body = []
    for name, skip in cases:
        if skip is None:
            body.append(f'<testcase classname="tests.test_x" name="{name}" time="0.001"/>')
        else:
            body.append(f'<testcase classname="tests.test_x" name="{name}" time="0">'
                        f'<skipped type="pytest.skip" message="{skip}">tests/test_x.py:1: {skip}</skipped></testcase>')
    path.write_text(f'<?xml version="1.0"?><testsuites><testsuite name="pytest" tests="{len(cases)}">'
                    f'{"".join(body)}</testsuite></testsuites>')
    return path


def test_allowlist_file_parses_and_every_block_starts_with_a_reason():
    patterns = skip_audit.load_allowlist(ALLOWLIST)
    assert patterns, "allowlist must not be empty"
    blocks = [b for b in ALLOWLIST.read_text().split("\n\n") if b.strip()]
    for block in blocks:
        first = block.strip().splitlines()[0]
        assert first.startswith("#"), f"allowlist block must start with a comment explaining it:\n{block}"


@pytest.mark.parametrize("message", [
    f"skip {_word()}-{_word()}: requires [timers]",
    f"runtime 'flink-jvm' does not support ['{_word()}']",
    f"local-jvm does not support ['{_word()}']",
    f"requires ['{_word()}']",
    f"requires ['{_word()}'], declared unsupported",
    "Ollama not reachable / model not pulled: connect refused",
    "Requires running Ollama instance with qwen2.5:latest",
    f"Fluss not reachable at localhost:{random.randint(1024, 65535)} — skipping",
    "Assumption failed: Fluss not reachable at localhost:9123 — skipping",
    "could not import 'litellm': No module named 'litellm'",
    "Environment variable [AGENTIC_PEKKO_INTEGRATION] does not exist",
])
def test_declared_capability_and_documented_skips_are_allowed(tmp_path: Path, message: str):
    report = _surefire(tmp_path / "TEST-a.xml", [(_word(), message), (_word(), None)])
    allowed, unexpected = skip_audit.audit([report], skip_audit.load_allowlist(ALLOWLIST))
    assert [s.message for s in allowed] == [message.removeprefix("Assumption failed: ")]
    assert unexpected == []


@pytest.mark.parametrize("message", [
    f"Postgres not reachable: Connection to localhost:{random.randint(1024, 65535)} refused",
    "Redis/Valkey not reachable: Failed to connect",
    "Qdrant not reachable: java.net.ConnectException",
    "no Redis on localhost:6379 — skipping",
    "no Kafka broker on localhost:9092 — skipping",
    "python with mcp SDK not available at python3",
    "MCP stub server didn't start (mcp SDK missing?): timeout",
    "Environment variable [AGENTIC_PEKKO_INTEGRATION] with value [false] does not match regular expression [true]",
    f"Environment variable [AGENTIC_{_word().upper()}] does not exist",
    "Assumption failed: no Kafka broker on localhost:9092 — skipping",
    "could not import 'cloudpickle': No module named 'cloudpickle'",
    f"no Redis/Valkey at redis://localhost:{random.randint(1024, 65535)}/0: refused",
    "",
])
def test_missing_service_skips_are_unexpected(tmp_path: Path, message: str):
    report = _pytest_report(tmp_path / "pytest.xml", [(_word(), message)])
    allowed, unexpected = skip_audit.audit([report], skip_audit.load_allowlist(ALLOWLIST))
    assert allowed == []
    assert [s.message for s in unexpected] == [message.removeprefix("Assumption failed: ")]


def test_main_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    ok = _surefire(tmp_path / "TEST-ok.xml", [(_word(), None), (_word(), "requires ['timers']")])
    assert skip_audit.main(["--allowlist", str(ALLOWLIST), str(ok)]) == 0
    assert "1 allowlisted skip(s), 0 unexpected" in capsys.readouterr().out

    bad = _pytest_report(tmp_path / "bad.xml", [(_word(), "Postgres not reachable: refused")])
    assert skip_audit.main(["--allowlist", str(ALLOWLIST), str(tmp_path / "**" / "*.xml")]) == 1
    out = capsys.readouterr()
    assert "UNEXPECTED" in out.out and "Postgres not reachable" in out.out
    assert bad.exists()

    assert skip_audit.main(["--allowlist", str(ALLOWLIST), str(tmp_path / "nothing" / "*.xml")]) == 2


def test_surefire_aborted_assumption_text_is_reduced_to_its_reason_line(tmp_path: Path):
    fixture = f"{_word()}-{_word()}"
    report = tmp_path / "TEST-conf.xml"
    report.write_text(
        '<testsuite><testcase name="fixtures" classname="c"><skipped type="org.opentest4j.TestAbortedException">'
        f'org.opentest4j.TestAbortedException: skip {fixture}: requires [timers]\n'
        '\tat org.junit.jupiter.api.Assumptions.abort(Assumptions.java:300)\n</skipped></testcase>'
        '<testcase name="pg" classname="c"><skipped type="org.opentest4j.TestAbortedException">'
        'org.opentest4j.TestAbortedException: Postgres not reachable: refused\n\tat x\n'
        '</skipped></testcase></testsuite>')
    allowed, unexpected = skip_audit.audit([report], skip_audit.load_allowlist(ALLOWLIST))
    assert [s.message for s in allowed] == [f"skip {fixture}: requires [timers]"]
    assert [s.message for s in unexpected] == ["Postgres not reachable: refused"]


def test_message_falls_back_to_element_text(tmp_path: Path):
    text = f"Kafka down {_word()}"
    report = tmp_path / "TEST-t.xml"
    report.write_text(f'<testsuite><testcase name="n" classname="c"><skipped>{text}</skipped></testcase></testsuite>')
    _, unexpected = skip_audit.audit([report], skip_audit.load_allowlist(ALLOWLIST))
    assert [s.message for s in unexpected] == [text]


def test_invalid_regex_in_allowlist_is_reported(tmp_path: Path):
    allowlist = tmp_path / "allow.txt"
    allowlist.write_text("valid\n(unclosed  # reason\n")
    with pytest.raises(SystemExit, match="allow.txt:2"):
        skip_audit.load_allowlist(allowlist)

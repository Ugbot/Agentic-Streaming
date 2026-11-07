"""The scan's own logic, without the network: what counts, what passes, and what must fail."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import osv_scan  # noqa: E402
import sbom_python  # noqa: E402


def test_maven_lines_are_coordinates_and_the_reactor_is_left_out() -> None:
    lines = ["The following files have been resolved:",
             "   com.nimbusds:nimbus-jose-jwt:jar:10.5:compile -- module com.nimbusds.jose.jwt",
             "   \x1b[1morg.example:thing:jar:tests:1.2:test\x1b[m",
             "   org.jagentic:jagentic-core:jar:1.0.0-SNAPSHOT:compile"]
    assert osv_scan.parse_maven(lines) == {("Maven", "com.nimbusds:nimbus-jose-jwt", "10.5"),
                                          ("Maven", "org.example:thing", "1.2")}


def test_an_advisory_passes_only_with_a_reason_and_aliases_count_once() -> None:
    package = ("Maven", "g:a", "1")
    found = {package: {"GHSA-1", "CVE-1"}, ("PyPI", "p", "2"): {"PYSEC-9"}}
    passed, failed = osv_scan.verdict(found, {"GHSA-1": {"CVE-1"}, "CVE-1": {"GHSA-1"}},
                                      osv_scan.allowlist("# comment\nCVE-1 only reachable through Flink's REST port\n"))
    assert [r["id"] for r in passed] == ["CVE-1"]
    assert [r["id"] for r in failed] == ["PYSEC-9"]


def test_an_allowlist_entry_without_a_reason_is_a_mistake() -> None:
    with pytest.raises(SystemExit):
        osv_scan.allowlist("CVE-1\n")


def test_a_paged_answer_is_not_silently_cut_short() -> None:
    with pytest.raises(SystemExit):
        osv_scan.advisories([("PyPI", "p", "1")], lambda url, body: {"results": [{"next_page_token": "x"}]})


def test_the_batch_answer_is_read_in_order() -> None:
    def answer(url: str, body: dict) -> dict:
        return {"results": [{"vulns": [{"id": "X"}]} if q["package"]["name"] == "bad" else {}
                            for q in body["queries"]]}
    found = osv_scan.advisories([("PyPI", "bad", "1"), ("PyPI", "good", "1")], answer)
    assert dict(found) == {("PyPI", "bad", "1"): {"X"}}


def test_the_python_sbom_lists_every_locked_package() -> None:
    lock = '[[package]]\nname = "PyYAML"\nversion = "6.0.2"\n\n[[package]]\nname = "local"\nsource = { editable = "." }\n'
    bom = sbom_python.sbom(lock, "x")
    assert bom["bomFormat"] == "CycloneDX"
    assert bom["components"] == [{"type": "library", "name": "PyYAML", "version": "6.0.2", "purl": "pkg:pypi/pyyaml@6.0.2"}]

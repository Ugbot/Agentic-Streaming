"""A document names its secrets, and a document could name any of the process's: `env:HOME`,
`file:/etc/passwd`, or `${AWS_SECRET_ACCESS_KEY}` as a connection link. A reference now has to be on
the operator's list, `AGENTIC_SECRET_REFS`, whose default is `env:AGENTIC_*` and files under
`/run/secrets/`."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentic import secret_refs  # noqa: E402


@pytest.fixture(autouse=True)
def _default_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AGENTIC_SECRET_REFS", raising=False)


def test_an_environment_variable_off_the_list_is_not_read() -> None:
    with pytest.raises(secret_refs.SecretUnresolved, match="AGENTIC_SECRET_REFS"):
        secret_refs.resolve("env:PATH")


def test_a_file_off_the_list_is_not_read() -> None:
    with pytest.raises(secret_refs.SecretUnresolved):
        secret_refs.resolve("file:/etc/hosts")
    with pytest.raises(secret_refs.SecretUnresolved):
        secret_refs.resolve("file:/run/secrets/../../etc/hosts")


def test_what_the_operator_lists_is_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token = tmp_path / "token"
    token.write_text("s3cret\n")
    monkeypatch.setenv("AGENTIC_SECRET_REFS", f"file:{tmp_path}/")
    assert secret_refs.resolve(f"file:{token}") == "s3cret"
    with pytest.raises(secret_refs.SecretUnresolved):
        secret_refs.resolve(f"file:{tmp_path}/../{tmp_path.name}x")


def test_a_connection_link_off_the_list_is_not_expanded() -> None:
    with pytest.raises(ValueError, match="AGENTIC_SECRET_REFS"):
        secret_refs.expand("${PATH}")

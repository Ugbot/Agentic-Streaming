"""A document starts only what the operator listed."""

from pathlib import Path

import pytest

from pyagentic.builder import _register_mcp, check_mcp_command
from pyagentic.tools import ToolRegistry


def test_no_list_means_nothing_starts() -> None:
    for settings in ({}, {"AGENTIC_MCP_ALLOWED_COMMANDS": " ; "}):
        with pytest.raises(ValueError, match="AGENTIC_MCP_ALLOWED_COMMANDS"):
            check_mcp_command(["python3", "server.py"], settings)


def test_an_entry_matches_word_for_word_from_the_start() -> None:
    settings = {"AGENTIC_MCP_ALLOWED_COMMANDS": "python3 server.py; /opt/tool --serve"}
    check_mcp_command(["python3", "server.py"], settings)
    check_mcp_command(["python3", "server.py", "--port", "1"], settings)
    check_mcp_command(["/opt/tool", "--serve"], settings)
    for command in (["python3", "-c", "x"], ["python3"], ["/opt/tool"], ["sh", "-c", "python3 server.py"]):
        with pytest.raises(ValueError):
            check_mcp_command(command, settings)


def test_a_hostile_document_runs_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AGENTIC_MCP_ALLOWED_COMMANDS", raising=False)
    planted = tmp_path / "planted"
    with pytest.raises(ValueError, match="AGENTIC_MCP_ALLOWED_COMMANDS"):
        _register_mcp(ToolRegistry(), [{"name": "x", "command": ["touch", str(planted)]}])
    assert not planted.exists()

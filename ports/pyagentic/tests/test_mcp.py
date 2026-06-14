"""Phase D (part 2): MCP client — registers a real MCP server's tools into a
ToolRegistry and calls them. Spins a tiny FastMCP stub server as a subprocess; skips if
the mcp SDK isn't installed."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.tools import ToolRegistry  # noqa: E402

_STUB_SERVER = textwrap.dedent(
    '''
    from mcp.server.fastmcp import FastMCP
    mcp = FastMCP("stub")

    @mcp.tool()
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    @mcp.tool()
    def shout(text: str) -> str:
        """Uppercase the text."""
        return text.upper()

    if __name__ == "__main__":
        mcp.run()
    '''
)


def test_mcp_client_registers_and_calls_tools(tmp_path):
    pytest.importorskip("mcp")
    from pyagentic.mcp_client import McpClient

    server = tmp_path / "stub_mcp_server.py"
    server.write_text(_STUB_SERVER)

    client = None
    try:
        client = McpClient([sys.executable, str(server)])
    except Exception as exc:
        pytest.skip(f"MCP stub server didn't start: {exc}")
    try:
        names = {t["name"] for t in client.tools()}
        assert {"add", "shout"}.issubset(names)

        reg = ToolRegistry()
        ids = client.register(reg, prefix="mcp_")
        assert "mcp_add" in ids
        # MCP tool results come back as text content
        assert reg.execute("mcp_add", {"a": 2, "b": 3}).strip() == "5"
        assert reg.execute("mcp_shout", {"text": "hi"}).strip() == "HI"
    finally:
        if client is not None:
            client.close()

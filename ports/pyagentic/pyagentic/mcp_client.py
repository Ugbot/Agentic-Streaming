"""MCP client — connect to a Model Context Protocol server (stdio) and surface its tools
in a ``ToolRegistry``, so an agent can call external MCP tool servers. Uses the official
``mcp`` SDK; the async session lives on a dedicated background event loop so the
synchronous ``ToolRegistry`` can call MCP tools.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Dict, List, Optional

from .tools import ToolRegistry


class McpClient:
    """Connects to one stdio MCP server, lists its tools, and registers them as sync
    ``ToolRegistry`` tools. Call ``close()`` to shut the server down."""

    def __init__(self, command: List[str], env: Optional[Dict[str, str]] = None, timeout: float = 30.0) -> None:
        self.command = command
        self.env = env
        self.timeout = timeout
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._ready = threading.Event()
        self._stop = None  # asyncio.Event created on the loop
        self._session = None
        self._tool_specs: List[Dict[str, str]] = []
        self._error: Optional[BaseException] = None
        asyncio.run_coroutine_threadsafe(self._serve(), self._loop)
        if not self._ready.wait(self.timeout):
            raise RuntimeError("MCP server did not become ready in time")
        if self._error is not None:
            raise RuntimeError(f"MCP connect failed: {self._error}")

    async def _serve(self) -> None:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        self._stop = asyncio.Event()
        try:
            params = StdioServerParameters(command=self.command[0], args=self.command[1:], env=self.env)
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    listed = await session.list_tools()
                    self._tool_specs = [{"name": t.name, "description": t.description or ""} for t in listed.tools]
                    self._session = session
                    self._ready.set()
                    await self._stop.wait()
        except BaseException as exc:  # surface to the constructor
            self._error = exc
            self._ready.set()

    def tools(self) -> List[Dict[str, str]]:
        return list(self._tool_specs)

    def _call(self, name: str, args: Dict[str, Any]) -> Any:
        async def run():
            result = await self._session.call_tool(name, args or {})
            parts = []
            for block in result.content:
                text = getattr(block, "text", None)
                parts.append(text if text is not None else str(block))
            return "\n".join(parts)

        return asyncio.run_coroutine_threadsafe(run(), self._loop).result(self.timeout)

    def register(self, registry: ToolRegistry, prefix: str = "") -> List[str]:
        """Register every MCP tool into ``registry`` (optionally id-prefixed). Returns the
        registered tool ids."""
        ids = []
        for spec in self._tool_specs:
            tool_id = prefix + spec["name"]
            name = spec["name"]
            registry.register(tool_id, spec["description"], lambda params, n=name: self._call(n, params))
            ids.append(tool_id)
        return ids

    def close(self) -> None:
        if self._stop is not None:
            self._loop.call_soon_threadsafe(self._stop.set)
        self._loop.call_soon_threadsafe(self._loop.stop)


def register_mcp_server(registry: ToolRegistry, command: List[str], env: Optional[Dict[str, str]] = None,
                        prefix: str = "") -> "McpClient":
    """Convenience: connect to an MCP server and register its tools into ``registry``.
    Returns the live client (keep it; call ``close()`` when done)."""
    client = McpClient(command, env=env)
    client.register(registry, prefix=prefix)
    return client

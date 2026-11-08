"""Tools from an MCP server, started as a subprocess over stdio.

An `mcp:` entry runs a command on this machine, so the operator says which: `AGENTIC_MCP_ALLOWED_COMMANDS`,
entries separated by `;`, each a command with the arguments it must start with. Deny by default. A bare
interpreter allows whatever it will run, so name the script. The JVM's `McpCommands` reads the same list.

The server names its tools and writes their descriptions, so a tool is registered as `<name>_<tool>`, may
not take the id of one already there, and its description is marked as text from outside
(`untrusted.describe`). The entry's `effect` covers every tool the server offers; without one they are
`non_idempotent`, the safe guess.

Needs the MCP SDK (`pip install "mcp<2"`), imported only when a document has an `mcp:` entry. The session
lives on its own event loop thread, so the synchronous tool registry can call it.
"""

from __future__ import annotations

import asyncio
import os
import threading
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any, Dict, List, Mapping, Optional, Sequence

from . import secret_refs
from .errors import OutcomeUnknown, ToolError, ValidationError

START_TIMEOUT_S = 30.0


def check_command(command: Sequence[str], settings: Optional[Mapping[str, str]] = None) -> None:
    """Raises `ValidationError` unless `command` starts, word for word, with an entry on the operator's list."""
    raw = (os.environ if settings is None else settings).get("AGENTIC_MCP_ALLOWED_COMMANDS", "")
    for entry in raw.split(";"):
        words = entry.split()
        if words and list(command[:len(words)]) == words:
            return
    raise ValidationError(f"mcp command {list(command)} is not in AGENTIC_MCP_ALLOWED_COMMANDS", "/mcp")


def command_of(entry: Mapping[str, Any]) -> List[str]:
    """The entry's command with any `${NAME}` link expanded under the operator's secret list."""
    raw = entry.get("command")
    words = raw if isinstance(raw, list) else [raw, *(entry.get("args") or [])]
    if not words or not all(isinstance(w, str) and w for w in words):
        raise ValidationError("an mcp server needs a command", "/mcp")
    try:
        return [secret_refs.expand(w) for w in words]
    except ValueError as exc:
        raise ValidationError(str(exc), "/mcp") from exc


def _first_cause(error: BaseException) -> str:
    """What went wrong, out of the exception groups the SDK's task groups wrap it in, cut short."""
    while getattr(error, "exceptions", None):  # an exception group, which 3.9 and 3.10 cannot name
        error = error.exceptions[0]  # type: ignore[attr-defined]
    return f"{type(error).__name__}: {str(error)[:200]}"


class McpServer:
    """One stdio MCP server: started by the constructor, listed once, called by tool name, `close`d."""

    def __init__(self, command: Sequence[str], timeout_s: float = START_TIMEOUT_S) -> None:
        try:
            from mcp import ClientSession, StdioServerParameters  # noqa: F401
        except ImportError as exc:
            raise ValidationError("an mcp: server needs the MCP SDK; pip install \"mcp<2\"", "/mcp") from exc
        self.command = list(command)
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="mcp-client", daemon=True)
        self._thread.start()
        self._ready = threading.Event()
        self._stop: Optional[asyncio.Event] = None
        self._session: Any = None
        self._tools: List[Dict[str, str]] = []
        self._error: Optional[BaseException] = None
        self._served = asyncio.run_coroutine_threadsafe(self._serve(), self._loop)
        if not self._ready.wait(timeout_s) or self._error is not None:
            reason = "it did not start in time" if self._error is None else _first_cause(self._error)
            self.close()
            raise ValidationError(f"mcp server {self.command[0]} failed to start: {reason}", "/mcp")

    async def _serve(self) -> None:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        self._stop = asyncio.Event()
        try:
            # No env: the SDK passes the server a minimal environment, not this process's secrets.
            params = StdioServerParameters(command=self.command[0], args=self.command[1:])
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    listed = await session.list_tools()
                    self._tools = [{"name": t.name, "description": t.description or ""} for t in listed.tools]
                    self._session = session
                    self._ready.set()
                    await self._stop.wait()
        except BaseException as exc:  # handed to the constructor
            self._error = exc
            self._ready.set()

    def tools(self) -> List[Dict[str, str]]:
        return list(self._tools)

    def call(self, name: str, args: Mapping[str, Any], timeout_s: float) -> str:
        """The tool's text. An error the server reports is a `ToolError`; no answer in time may have
        landed, so it is `OutcomeUnknown`."""
        async def run() -> Any:
            return await self._session.call_tool(name, dict(args))

        future = asyncio.run_coroutine_threadsafe(run(), self._loop)
        try:
            result = future.result(timeout_s)
        except FutureTimeout:
            future.cancel()
            raise OutcomeUnknown(f"mcp tool {name} timed out after {int(timeout_s * 1000)} ms") from None
        except Exception as exc:
            raise OutcomeUnknown(f"mcp tool {name} failed: {type(exc).__name__}") from exc
        text = "\n".join(getattr(block, "text", None) or str(block) for block in result.content)
        if getattr(result, "isError", False):
            raise ToolError(f"mcp tool {name} failed: {text[:200]}")
        return text

    def close(self) -> None:
        """Ends the session, which stops the server process, then the loop."""
        if self._stop is not None:
            self._loop.call_soon_threadsafe(self._stop.set)
        try:
            self._served.result(5)
        except Exception:  # noqa: S110 - closing; the server is going either way
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)

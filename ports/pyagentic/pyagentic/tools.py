"""Tools — the portable analogue of ``ToolExecutor`` / ``ToolRegistry``.

A tool is a named ``params -> result`` function. Sync here for simplicity; engine
adapters that have async (Faust/Ray) wrap these in ``await loop.run_in_executor``
or call native async tools. Mirrors ``org.agentic.flink.tools.ToolExecutor``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List


@dataclass(frozen=True)
class Tool:
    tool_id: str
    description: str
    fn: Callable[[Dict[str, Any]], Any]

    def execute(self, params: Dict[str, Any]) -> Any:
        return self.fn(params or {})


class ToolRegistry:
    """Central registry; agents select tools by id."""

    def __init__(self) -> None:
        self._tools: Dict[str, Tool] = {}

    def register(self, tool_id: str, description: str, fn: Callable[[Dict[str, Any]], Any]) -> "ToolRegistry":
        self._tools[tool_id] = Tool(tool_id, description, fn)
        return self

    def add(self, tool: Tool) -> "ToolRegistry":
        self._tools[tool.tool_id] = tool
        return self

    def get(self, tool_id: str) -> Tool | None:
        return self._tools.get(tool_id)

    def ids(self) -> List[str]:
        return list(self._tools.keys())

    def execute(self, tool_id: str, params: Dict[str, Any]) -> Any:
        tool = self._tools.get(tool_id)
        if tool is None:
            raise KeyError(f"no such tool: {tool_id}")
        return tool.execute(params)

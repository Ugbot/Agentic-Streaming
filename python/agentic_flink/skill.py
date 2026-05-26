"""Skill wrappers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from ._jvm import jclass


@dataclass(frozen=True)
class Skill:
    """Named capability bundle. Maps onto ``org.agentic.flink.skill.Skill``."""

    name: str
    description: str = ""
    tools: tuple[str, ...] = ()
    system_prompt_fragment: str = ""
    required_facts: tuple[str, ...] = ()

    def _to_java(self):
        S = jclass("org.agentic.flink.skill.Skill")
        b = S.builder().withName(self.name)
        if self.description:
            b = b.withDescription(self.description)
        if self.tools:
            ArrayList = jclass("java.util.ArrayList")
            tools = ArrayList()
            for t in self.tools:
                tools.add(t)
            b = b.withTools(tools)
        if self.system_prompt_fragment:
            b = b.withSystemPromptFragment(self.system_prompt_fragment)
        if self.required_facts:
            b = b.withRequiredFacts(*self.required_facts)
        return b.build()


def mcp_stdio(name: str, *command: str):
    """Build a stdio :class:`McpServerSpec` (e.g.
    ``mcp_stdio("everything", "npx", "-y", "@modelcontextprotocol/server-everything")``)."""
    MCP = jclass("org.agentic.flink.tools.mcp.McpServerSpec")
    return MCP.stdio(name, *command)


def mcp_http(name: str, url: str):
    """Build an HTTP :class:`McpServerSpec`."""
    MCP = jclass("org.agentic.flink.tools.mcp.McpServerSpec")
    return MCP.http(name, url)


__all__ = ["Skill", "mcp_stdio", "mcp_http"]

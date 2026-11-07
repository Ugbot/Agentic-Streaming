"""The tool boundary: one deny-by-default decision for every tool call, whatever asked for it.

`primitives.md` section 11. A `ToolPolicy` answers one `ToolRequest`. `DocumentToolPolicy` is what the
workflow document says, and it is the floor: another policy can only narrow it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Protocol, Set

ALLOW = "allow"
DENY = "deny"

UNATTENDED_ORIGINS = frozenset({"timer", "cep"})  # they run outside any path


@dataclass(frozen=True)
class ToolRequest:
    tool: str
    origin: str  # brain, saga, compensation, timer or cep
    path: Optional[str]
    effect: Optional[str]  # None when no such tool is registered
    args: Mapping[str, Any]


@dataclass(frozen=True)
class Decision:
    verdict: str
    reason: str = ""


class ToolPolicy(Protocol):
    def decide(self, request: ToolRequest) -> Decision: ...


class DocumentToolPolicy:
    """A path's `tools` and `policies.unattended_tools`. What neither lists is refused."""

    def __init__(self, doc: Mapping[str, Any]) -> None:
        self._unattended = (doc.get("policies") or {}).get("unattended_tools") or []
        skills = {s.get("name"): s.get("tools") or [] for s in doc.get("skills") or []}
        self._tools: Dict[str, Set[str]] = {}
        for name, path in ((doc.get("agent") or {}).get("paths") or {}).items():
            granted = set(path.get("tools") or [])
            for skill in path.get("skills") or []:
                granted.update(skills.get(skill, []))
            self._tools[name] = granted

    def tools_of(self, path: Optional[str]) -> Set[str]:
        return self._tools.get(path or "", set())

    def decide(self, request: ToolRequest) -> Decision:
        if request.origin in UNATTENDED_ORIGINS:
            if request.tool in self._unattended:
                return Decision(ALLOW)
            return Decision(DENY, f"tool {request.tool} is not in policies.unattended_tools")
        if request.tool in self.tools_of(request.path):
            return Decision(ALLOW)
        return Decision(DENY, f"tool {request.tool} is not in the tools of path {request.path}")

"""The tool boundary: one deny-by-default decision for every tool call, whatever asked for it.

`primitives.md` section 11. A `ToolPolicy` answers one `ToolRequest`. `DocumentToolPolicy` is what the
workflow document says, and it is the floor: another policy can only narrow it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Protocol, Sequence, Set

ALLOW = "allow"
DENY = "deny"
ASK_A_PERSON = "ask_a_person"  # allowed once an approver says so

UNATTENDED_ORIGINS = frozenset({"timer", "cep"})  # they run outside any path


@dataclass(frozen=True)
class Principal:
    """Who a turn or a signal is authenticated as. Whatever builds one vouches for it: the gateway
    from a verified token, or the program embedding the runtime."""

    subject: str
    tenant: Optional[str] = None
    roles: Sequence[str] = ()
    issuer: Optional[str] = None

    def wire(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"subject": self.subject}
        if self.tenant is not None:
            out["tenant"] = self.tenant
        if self.roles:
            out["roles"] = list(self.roles)
        if self.issuer is not None:
            out["issuer"] = self.issuer
        return out


@dataclass(frozen=True)
class ToolRequest:
    tool: str
    origin: str  # brain, saga, compensation, timer or cep
    path: Optional[str]
    effect: Optional[str]  # None when no such tool is registered
    args: Mapping[str, Any]
    principal: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True)
class Decision:
    verdict: str
    reason: str = ""


class ToolPolicy(Protocol):
    def decide(self, request: ToolRequest) -> Decision: ...


class DocumentToolPolicy:
    """A path's `tools` and `policies.unattended_tools`. What neither lists is refused."""

    def __init__(self, doc: Mapping[str, Any]) -> None:
        policies = doc.get("policies") or {}
        self._unattended = policies.get("unattended_tools") or []
        gate = policies.get("tool_approval") or {}
        self._gated_tools = gate.get("tools") or []
        self._gated_effects = gate.get("effects") or []
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
            if request.tool not in self._unattended:
                return Decision(DENY, f"tool {request.tool} is not in policies.unattended_tools")
        elif request.tool not in self.tools_of(request.path):
            return Decision(DENY, f"tool {request.tool} is not in the tools of path {request.path}")
        gated = request.tool in self._gated_tools or request.effect in self._gated_effects
        return Decision(ASK_A_PERSON if gated else ALLOW)


def owner_problem(doc: Mapping[str, Any], conversation_id: str, owner: Optional[Mapping[str, Any]],
                  principal: Optional[Mapping[str, Any]]) -> Optional[str]:
    """Why this principal may not add a turn to a conversation `owner` started, or None when it may.

    A conversation belongs to the principal of its first `turn_received`: its tenant and its subject.
    `policies.sharing` lets others of the same tenant in, by role or the whole tenant. A conversation
    started with no principal has no owner to check (`primitives.md` section 11)."""
    if owner is None:
        return None
    if principal is None or principal.get("tenant") != owner.get("tenant"):
        return f"conversation {conversation_id} belongs to another tenant"
    if principal.get("subject") == owner.get("subject"):
        return None
    sharing = (doc.get("policies") or {}).get("sharing") or {}
    if sharing.get("tenant") is True:
        return None
    if set(principal.get("roles") or []) & set(sharing.get("roles") or []):
        return None
    return f"conversation {conversation_id} belongs to another subject"


def approver_problem(doc: Mapping[str, Any], signal_principal: Optional[Mapping[str, Any]],
                     tenant: Optional[str]) -> Optional[str]:
    """Why this principal may not answer a suspended turn of `tenant`, or None when it may. Deny by
    default: `policies.approvers` names who may, and a name with no principal is nobody."""
    if signal_principal is None:
        return "a signal needs an authenticated principal"
    subject = signal_principal["subject"]
    if tenant is not None and signal_principal.get("tenant") != tenant:
        return f"{subject} is not of this conversation's tenant"
    approvers = (doc.get("policies") or {}).get("approvers") or {}
    if subject in (approvers.get("subjects") or []):
        return None
    if set(signal_principal.get("roles") or []) & set(approvers.get("roles") or []):
        return None
    return f"{subject} is not an approver"


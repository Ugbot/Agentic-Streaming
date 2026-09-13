"""Python callables a workflow document can reference but cannot carry.

The IR is data. `function` tools, in-process peers, and custom brains, verifiers, and
guardrails are Python, so a spec carries them beside the document as `Bindings`, and a
runtime that receives a document referencing a binding it was not given fails at deploy.
Documents mark custom Python pieces with extension keys: `x-brain` on a path, `x-verifier`
on a verifier, `x-python` on a guardrail.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence

from .events import ChatMessage
from .retrieval import Passage
from .tools import PeerFn, ToolFn


class BrainContext(Protocol):
    """What a custom brain sees: the turn, the memory, and the recorded ways to act."""

    @property
    def text(self) -> str: ...
    @property
    def path(self) -> str: ...
    @property
    def user_id(self) -> str: ...
    @property
    def transcript(self) -> Sequence[ChatMessage]: ...
    def invoke(self, tool_id: str, args: Mapping[str, Any]) -> Any: ...
    def retrieve(self, query: str, k: int = 0) -> List[Passage]: ...


BrainFn = Callable[[BrainContext], str]
VerifierFn = Callable[[str], bool]
GuardrailFn = Callable[[str], Optional[str]]


@dataclass
class Bindings:
    tools: Dict[str, ToolFn] = field(default_factory=dict)
    peers: Dict[str, PeerFn] = field(default_factory=dict)
    brains: Dict[str, BrainFn] = field(default_factory=dict)
    verifiers: Dict[str, VerifierFn] = field(default_factory=dict)
    guardrails: Dict[str, GuardrailFn] = field(default_factory=dict)

    def missing(self, doc: Mapping[str, Any]) -> List[str]:
        """Every binding the document references that this object does not provide."""
        problems: List[str] = []
        for i, tool in enumerate(doc.get("tools") or []):
            if tool.get("kind") == "function" and tool["id"] not in self.tools:
                problems.append(f"/tools/{i}: function tool {tool['id']!r} has no bound Python function")
        agent = doc["agent"]
        for name, path in agent["paths"].items():
            brain = path.get("x-brain")
            if brain is not None and brain not in self.brains:
                problems.append(f"/agent/paths/{name}/x-brain: no brain bound under {brain!r}")
            verifier = (path.get("verifier") or {}).get("x-verifier")
            if verifier is not None and verifier not in self.verifiers:
                problems.append(f"/agent/paths/{name}/verifier/x-verifier: no verifier bound under {verifier!r}")
        verifier = (agent.get("verifier") or {}).get("x-verifier")
        if verifier is not None and verifier not in self.verifiers:
            problems.append(f"/agent/verifier/x-verifier: no verifier bound under {verifier!r}")
        for i, rail in enumerate(doc.get("guardrails") or []):
            custom = rail.get("x-python")
            if custom is not None and custom not in self.guardrails:
                problems.append(f"/guardrails/{i}/x-python: no guardrail bound under {custom!r}")
        return problems

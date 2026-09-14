"""The shared high-level API: build an ``agentic/v1`` workflow once, run it on any runtime.

::

    from agentic_flink.workflow import Agent, load

    spec = (Agent("support")
              .route(kind="keyword", rules={"billing": ["refund", "charge"]}, default="general")
              .path("billing", brain="rule", tools=["issue_refund"], guardrails=["authenticated"])
              .use_tool("issue_refund", issue_refund)
              .policies(ordering="per-conversation", idempotency="turn-id", retry="exponential")
              .build())
    spec = load("examples/pipelines/banking.yaml")
    result = spec.run(runtime="local-jvm", text="refund me", conversation_id="c1", turn_id="t1")

``Agent.build()`` produces an :class:`AgentSpec`: a workflow IR document shaped exactly as
``spec/v1/workflow.schema.json`` describes, plus the Python callables bound with
:meth:`Agent.use_tool` (declared in the document as ``kind: function`` tools). ``AgentSpec.run``
is a convenience on top of the runtime registry (:mod:`agentic_flink._contract`): it selects a
runtime, deploys, submits one event and closes. Multi-turn use goes through ``get_runtime``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Union

import yaml

from ._contract import Runtime, get_runtime
from ._contract import required_capabilities as _required_capabilities

SPEC_VERSION = "agentic/v1"

ROUTER_KINDS = ("keyword", "llm", "classifier", "static")
BRAINS = ("rule", "llm")
VERIFIER_KINDS = ("prefix", "schema", "regex", "llm", "none")
ORDERINGS = ("per-conversation", "none")
IDEMPOTENCIES = ("turn-id", "none")
RETRY_KINDS = ("none", "fixed", "exponential")
GUARDRAIL_KINDS = ("regex", "classifier", "schema")
GUARDRAIL_STAGES = ("input", "output", "both")
TOOL_KINDS = ("constant", "http", "agent", "mcp", "function", "failing")
STORE_SLOTS = ("conversation", "keyed_state", "long_term", "vector")

ToolFn = Callable[..., Any]


class WorkflowError(ValueError):
    """The builder or loader was given something the workflow IR cannot express."""


@dataclass(frozen=True)
class Event:
    """One inbound event: a user turn, or a resume signal for a suspended turn."""

    conversation_id: str
    turn_id: str
    text: Optional[str] = None
    user_id: str = "anonymous"
    signal: Optional[Mapping[str, Any]] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.conversation_id or not self.turn_id:
            raise WorkflowError("an event needs a conversation_id and a turn_id (the idempotency key)")
        if self.signal is None and self.text is None:
            raise WorkflowError("an event is either a turn (text=...) or a resume (signal={...})")

    @property
    def is_resume(self) -> bool:
        return self.signal is not None

    @classmethod
    def turn(cls, conversation_id: str, turn_id: str, text: str, user_id: str = "anonymous",
             metadata: Optional[Mapping[str, Any]] = None) -> "Event":
        return cls(conversation_id, turn_id, text=text, user_id=user_id, metadata=dict(metadata or {}))

    @classmethod
    def resume(cls, conversation_id: str, turn_id: str, signal: Mapping[str, Any]) -> "Event":
        return cls(conversation_id, turn_id, signal=dict(signal))


def _check(value: Any, allowed: Sequence[str], what: str) -> str:
    if value not in allowed:
        raise WorkflowError(f"{what} must be one of {list(allowed)}, got {value!r}")
    return value


def _verifier(v: Union[None, str, Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    if v is None:
        return None
    if isinstance(v, str):
        return {"kind": _check(v, VERIFIER_KINDS, "verifier")}
    out = dict(v)
    if "kind" in out:
        _check(out["kind"], VERIFIER_KINDS, "verifier.kind")
    return out


class AgentSpec(MappingABC):
    """A built workflow: an immutable mapping (the IR document) plus bound Python tools."""

    def __init__(self, document: Mapping[str, Any], bindings: Optional[Mapping[str, ToolFn]] = None,
                 source: Optional[Path] = None) -> None:
        self._doc: Dict[str, Any] = json.loads(json.dumps(document))  # plain JSON types, deep copy
        self._bindings: Dict[str, ToolFn] = dict(bindings or {})
        self.source = source
        if "agent" not in self._doc:
            raise WorkflowError("a workflow document needs an `agent` block")
        version = self._doc.setdefault("spec_version", SPEC_VERSION)
        if version != SPEC_VERSION:
            raise WorkflowError(f"unsupported spec_version {version!r}; this package speaks {SPEC_VERSION}")
        declared = {t["id"] for t in self._doc.get("tools", []) if t.get("kind") == "function"}
        unbound = declared - set(self._bindings)
        bound_undeclared = set(self._bindings) - declared
        if bound_undeclared:
            raise WorkflowError(f"bound tools not declared as kind=function in the document: {sorted(bound_undeclared)}")
        self.unbound_functions = frozenset(unbound)

    # Mapping protocol → the document itself.
    def __getitem__(self, key: str) -> Any:
        return self._doc[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._doc)

    def __len__(self) -> int:
        return len(self._doc)

    def __repr__(self) -> str:
        return f"AgentSpec(agent={self.agent_id!r}, paths={list(self._doc['agent'].get('paths', {}))})"

    @property
    def agent_id(self) -> Optional[str]:
        return self._doc["agent"].get("id")

    @property
    def document(self) -> Dict[str, Any]:
        """The workflow IR document (the same shape ``agentic.AgentSpec.document`` exposes)."""
        return self.to_dict()

    @property
    def bindings(self) -> Dict[str, ToolFn]:
        """Python callables for the document's ``kind: function`` tools, by tool id."""
        return dict(self._bindings)

    def to_dict(self) -> Dict[str, Any]:
        return json.loads(json.dumps(self._doc))

    def to_yaml(self) -> str:
        return yaml.safe_dump(self.to_dict(), sort_keys=False)

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), **kw)

    def bind(self, tool_id: str, fn: ToolFn) -> "AgentSpec":
        """A copy with ``fn`` bound to the ``kind: function`` tool ``tool_id``."""
        return AgentSpec(self._doc, {**self._bindings, tool_id: fn}, self.source)

    def requirements(self) -> Dict[str, str]:
        """Capability ids this workflow needs, mapped to the document location that needs them."""
        return workflow_requirements(self._doc)

    def run(self, runtime: Union[str, Runtime] = "local-jvm", *, text: Optional[str] = None,
            conversation_id: str, turn_id: str, user_id: str = "anonymous",
            signal: Optional[Mapping[str, Any]] = None, **runtime_options: Any) -> Dict[str, Any]:
        """Deploy on ``runtime`` (a registered name or an instance), submit one event, return its
        normalized result. A runtime created here is closed before returning."""
        event = Event(conversation_id, turn_id, text=text, user_id=user_id, signal=signal)
        if isinstance(runtime, str):
            rt = get_runtime(runtime, **runtime_options)
            try:
                rt.deploy(self)
                return rt.submit(event)
            finally:
                rt.close()
        if runtime_options:
            raise WorkflowError("runtime options only apply when selecting a runtime by name")
        runtime.deploy(self)
        return runtime.submit(event)


# Where in the document each capability id comes from; used to explain CapabilityError.
REQUIREMENT_LOCATIONS: Dict[str, str] = {
    "routing": "agent.router",
    "rule_brain": "agent.paths[*].brain=rule",
    "llm_brain": "agent.paths[*].brain=llm",
    "tools": "tools / mcp / a2a",
    "structured_tool_args": "tools[*].parameters",
    "guardrails": "guardrails",
    "verifier": "agent.verifier / agent.paths[*].verifier",
    "ordering": "policies.ordering",
    "idempotency": "policies.idempotency",
    "retry": "policies.retry",
    "memory": "agent (conversation transcript)",
    "retrieval": "retrieval",
    "context_window": "context",
    "suspend_resume": "agent.paths[*].x-suspend-until",
    "timers": "timers",
    "saga": "saga",
    "a2a": "a2a",
    "cep": "cep",
    "durable_store": "stores",
}


def required_capabilities(doc: Mapping[str, Any]) -> List[str]:
    """The capability ids a workflow document needs (``spec/v1/primitives.md`` section 6).

    This is the canonical derivation of ``agentic.runtime`` (``ports/pyagentic``), re-exported by
    :mod:`agentic_flink._contract`; every Python binding returns the same list for the same document.
    """
    return list(_required_capabilities(doc))


def workflow_requirements(doc: Mapping[str, Any]) -> Dict[str, str]:
    """:func:`required_capabilities` mapped to the document location that needs each id."""
    return {cap: REQUIREMENT_LOCATIONS.get(cap, cap) for cap in required_capabilities(doc)}


class Agent:
    """Fluent builder for one ``agentic/v1`` workflow document."""

    def __init__(self, agent_id: str) -> None:
        if not agent_id:
            raise WorkflowError("agent id is required")
        self._id = agent_id
        self._router: Optional[Dict[str, Any]] = None
        self._paths: Dict[str, Dict[str, Any]] = {}
        self._verifier: Optional[Dict[str, Any]] = None
        self._policies: Dict[str, Any] = {}
        self._tools: Dict[str, Dict[str, Any]] = {}
        self._bindings: Dict[str, ToolFn] = {}
        self._guardrails: List[Dict[str, Any]] = []
        self._stores: Dict[str, Dict[str, Any]] = {}
        self._sections: Dict[str, Any] = {}
        self._backend: Optional[str] = None
        self._runtime: Dict[str, Dict[str, Any]] = {}

    # ---- agent ----

    def route(self, kind: str = "keyword", *, rules: Optional[Mapping[str, Sequence[str]]] = None,
              default: Optional[str] = None, prompt: Optional[str] = None,
              classifier: Optional[str] = None, threshold: Optional[float] = None) -> "Agent":
        router: Dict[str, Any] = {"kind": _check(kind, ROUTER_KINDS, "router kind")}
        if default is not None:
            router["default"] = default
        if rules is not None:
            router["rules"] = {k: list(v) for k, v in rules.items()}
        if prompt is not None:
            router["prompt"] = prompt
        if classifier is not None:
            router["classifier"] = classifier
        if threshold is not None:
            router["threshold"] = threshold
        self._router = router
        return self

    def path(self, name: str, *, brain: str = "rule", prompt: Optional[str] = None,
             tools: Optional[Sequence[str]] = None, tool_triggers: Optional[Mapping[str, str]] = None,
             guardrails: Optional[Sequence[str]] = None,
             verifier: Union[None, str, Mapping[str, Any]] = None, skills: Optional[Sequence[str]] = None,
             max_iterations: Optional[int] = None, suspend_until: Optional[str] = None,
             **extensions: Any) -> "Agent":
        """Declare a path. ``brain`` is ``rule`` or ``llm`` (the only brains the IR defines);
        ``verifier`` is a kind or a verifier mapping; ``suspend_until`` is the ``x-suspend-until``
        signal kind; extra ``x_*`` keywords become ``x-*`` extension keys."""
        p: Dict[str, Any] = {"brain": _check(brain, BRAINS, f"agent.paths.{name}.brain")}
        if prompt is not None:
            p["prompt"] = prompt
        if tools is not None:
            p["tools"] = list(tools)
        if tool_triggers is not None:
            p["tool_triggers"] = dict(tool_triggers)
        if guardrails is not None:
            p["guardrails"] = list(guardrails)
        if verifier is not None:
            p["verifier"] = _verifier(verifier)
        if skills is not None:
            p["skills"] = list(skills)
        if max_iterations is not None:
            p["max_iterations"] = int(max_iterations)
        if suspend_until is not None:
            p["x-suspend-until"] = suspend_until
        for k, v in extensions.items():
            if not k.startswith("x_"):
                raise WorkflowError(f"unknown path option {k!r}; only x_* extension keys are accepted")
            p["x-" + k[2:].replace("_", "-")] = v
        self._paths[name] = p
        return self

    def verify(self, kind: str = "prefix", **options: Any) -> "Agent":
        self._verifier = {"kind": _check(kind, VERIFIER_KINDS, "verifier kind"), **options}
        return self

    # ---- tools ----

    def tool(self, tool_id: str, kind: str = "constant", *, description: Optional[str] = None,
             value: Any = None, url: Optional[str] = None, parameters: Optional[Mapping[str, Any]] = None,
             compensation: Optional[str] = None, **extra: Any) -> "Agent":
        """Declare a document-defined tool (``constant``, ``http``, ``agent``, ``mcp``, ``failing``)."""
        t: Dict[str, Any] = {"id": tool_id, "kind": _check(kind, TOOL_KINDS, f"tools[{tool_id}].kind")}
        if description is not None:
            t["description"] = description
        if value is not None:
            t["value"] = value
        if url is not None:
            t["url"] = url
        if parameters is not None:
            t["parameters"] = dict(parameters)
        if compensation is not None:
            t["compensation"] = compensation
        for k, v in extra.items():
            key = "x-" + k[2:].replace("_", "-") if k.startswith("x_") else k
            t[key] = v
        self._tools[tool_id] = t
        return self

    def use_tool(self, tool_id: str, fn: ToolFn, *, description: Optional[str] = None,
                 parameters: Optional[Mapping[str, Any]] = None, compensation: Optional[str] = None) -> "Agent":
        """Bind a Python callable as tool ``tool_id`` (declared as ``kind: function``).

        The runtime invokes ``fn`` with the tool call's arguments as keyword arguments, coerced
        to the callable's annotated parameter types (see :mod:`agentic_flink.pytools`)."""
        if not callable(fn):
            raise WorkflowError(f"use_tool({tool_id!r}) needs a callable, got {fn!r}")
        self.tool(tool_id, "function", description=description or (fn.__doc__ or tool_id).strip().splitlines()[0],
                  parameters=parameters, compensation=compensation)
        self._bindings[tool_id] = fn
        return self

    # ---- cross-cutting ----

    def guardrail(self, kind: str = "regex", *, name: Optional[str] = None, stage: str = "input",
                  deny: Optional[Sequence[str]] = None, allow: Optional[Sequence[str]] = None,
                  reason: Optional[str] = None, **options: Any) -> "Agent":
        g: Dict[str, Any] = {"kind": _check(kind, GUARDRAIL_KINDS, "guardrail kind"),
                             "stage": _check(stage, GUARDRAIL_STAGES, "guardrail stage")}
        if name is not None:
            g["name"] = name
        if deny is not None:
            g["deny"] = list(deny)
        if allow is not None:
            g["allow"] = list(allow)
        if reason is not None:
            g["reason"] = reason
        g.update(options)
        self._guardrails.append(g)
        return self

    def with_memory(self, **stores: Union[str, Mapping[str, Any]]) -> "Agent":
        """Declare stores: ``with_memory(conversation="memory", long_term={"kind": "postgres", ...})``.
        Slots are ``conversation``, ``keyed_state``, ``long_term`` and ``vector``."""
        for slot, cfg in stores.items():
            _check(slot, STORE_SLOTS, "memory slot")
            self._stores[slot] = {"kind": cfg} if isinstance(cfg, str) else dict(cfg)
        return self

    def policies(self, *, ordering: Optional[str] = None, idempotency: Optional[str] = None,
                 retry: Union[None, str, Mapping[str, Any]] = None,
                 verification: Optional[Mapping[str, Any]] = None,
                 on_tool_error: Optional[str] = None) -> "Agent":
        if ordering is not None:
            self._policies["ordering"] = _check(ordering, ORDERINGS, "policies.ordering")
        if idempotency is not None:
            self._policies["idempotency"] = _check(idempotency, IDEMPOTENCIES, "policies.idempotency")
        if retry is not None:
            r = {"kind": retry} if isinstance(retry, str) else dict(retry)
            _check(r.get("kind", "none"), RETRY_KINDS, "policies.retry.kind")
            self._policies["retry"] = r
        if verification is not None:
            self._policies["verification"] = dict(verification)
        if on_tool_error is not None:
            self._policies["on_tool_error"] = _check(on_tool_error, ("fail", "continue"), "policies.on_tool_error")
        return self

    def section(self, name: str, value: Any) -> "Agent":
        """Set a top-level IR section verbatim (``saga``, ``a2a``, ``retrieval``, ``context``, ...)."""
        self._sections[name] = value
        return self

    def backend(self, name: str) -> "Agent":
        self._backend = name
        return self

    def runtime_options(self, runtime: str, **options: Any) -> "Agent":
        """Runtime-specific settings for the ``runtime:`` extension block."""
        self._runtime.setdefault(runtime, {}).update(options)
        return self

    # ---- build ----

    def build(self) -> AgentSpec:
        if not self._paths:
            raise WorkflowError("an agent needs at least one path()")
        agent: Dict[str, Any] = {"id": self._id}
        if self._router is not None:
            default = self._router.get("default")
            if default is not None and default not in self._paths:
                raise WorkflowError(f"router default {default!r} is not a declared path")
            for target in self._router.get("rules", {}):
                if target not in self._paths:
                    raise WorkflowError(f"router rule targets undeclared path {target!r}")
            agent["router"] = self._router
        agent["paths"] = self._paths
        if self._verifier is not None:
            agent["verifier"] = self._verifier
        declared_guards = {g["name"] for g in self._guardrails if "name" in g}
        for name, p in self._paths.items():
            for t in p.get("tools", []):
                if t not in self._tools and not any(t == a.get("name", a.get("id")) for a in self._sections.get("a2a", [])):
                    raise WorkflowError(f"path {name!r} lists undeclared tool {t!r}; declare it with tool() or use_tool()")
            for g in p.get("guardrails", []):
                if g not in declared_guards:
                    raise WorkflowError(f"path {name!r} lists undeclared guardrail {g!r}; declare it with guardrail(name=...)")
        doc: Dict[str, Any] = {"spec_version": SPEC_VERSION}
        if self._backend:
            doc["backend"] = self._backend
        if self._runtime:
            doc["runtime"] = self._runtime
        doc["agent"] = agent
        if self._policies:
            doc["policies"] = self._policies
        if self._tools:
            doc["tools"] = list(self._tools.values())
        if self._guardrails:
            doc["guardrails"] = self._guardrails
        if self._stores:
            doc["stores"] = self._stores
        doc.update(self._sections)
        return AgentSpec(doc, self._bindings)


def load(path: Union[str, Path], *, tools: Optional[Mapping[str, ToolFn]] = None) -> AgentSpec:
    """Load a workflow document from YAML or JSON. ``tools`` binds callables to ``kind: function`` tools."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"workflow file {p} does not exist")
    text = p.read_text(encoding="utf-8")
    doc = json.loads(text) if p.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(doc, dict):
        raise WorkflowError(f"{p} does not contain a workflow document (expected a mapping)")
    return AgentSpec(doc, tools, source=p.resolve())


def loads(text: str, *, tools: Optional[Mapping[str, ToolFn]] = None) -> AgentSpec:
    doc = yaml.safe_load(text)
    if not isinstance(doc, dict):
        raise WorkflowError("workflow text does not contain a mapping")
    return AgentSpec(doc, tools)


__all__ = ["Agent", "AgentSpec", "Event", "WorkflowError", "load", "loads", "required_capabilities", "workflow_requirements"]

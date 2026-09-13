"""`Agent` (the fluent builder), `AgentSpec` (a validated workflow + its Python bindings),
and `load()`. Everything here ends in `Runtime.deploy` / `Runtime.submit`.
"""

from __future__ import annotations

import json
import threading
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from . import ir
from .bindings import Bindings, BrainFn, GuardrailFn, VerifierFn
from .errors import ValidationError
from .events import Turn
from .runtime import Runtime, get_runtime, required_capabilities
from .tools import PeerFn, ToolFn

BRAIN_KINDS = ("rule", "llm")
VERIFIER_KINDS = ("none", "prefix", "regex", "schema")
VERIFIER_LIKE = Union[str, Mapping[str, Any], VerifierFn, None]


class AgentSpec:
    """A workflow document valid against `workflow.schema.json`, plus the Python callables
    it references. Immutable: builders and loaders make a new one."""

    def __init__(self, document: Mapping[str, Any], bindings: Optional[Bindings] = None) -> None:
        self.document: Dict[str, Any] = ir.validate_document(document)
        self.bindings = bindings or Bindings()
        self._runtimes: Dict[str, Runtime] = {}
        self._guard = threading.Lock()

    # -- introspection -------------------------------------------------------

    @property
    def id(self) -> str:
        return str(self.document["agent"]["id"])

    @property
    def requirements(self) -> List[str]:
        return required_capabilities(self.document)

    def to_yaml(self) -> str:
        return ir.dumps(self.document, "yaml")

    def to_json(self) -> str:
        return ir.dumps(self.document, "json")

    def save(self, path: str) -> None:
        fmt = "json" if path.lower().endswith(".json") else "yaml"
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(ir.dumps(self.document, fmt))

    def __getitem__(self, key: str) -> Any:
        return self.document[key]

    def __repr__(self) -> str:
        paths = ", ".join(self.document["agent"]["paths"])
        return f"AgentSpec({self.id!r}, paths=[{paths}], requires={self.requirements})"

    # -- running -------------------------------------------------------------

    def runtime(self, name: str = "local", **options: Any) -> Runtime:
        """A deployed runtime for this spec, one per (name, options), reused by `run`."""
        key = name + json.dumps(_stable(options), sort_keys=True, default=repr)
        with self._guard:
            rt = self._runtimes.get(key)
            if rt is None:
                rt = get_runtime(name, **options)
                rt.deploy(self)
                self._runtimes[key] = rt
            return rt

    def run(
        self,
        runtime: str = "local",
        text: str = "",
        conversation_id: str = "default",
        turn_id: Optional[str] = None,
        user_id: str = "anonymous",
        signal: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Mapping[str, str]] = None,
        **runtime_options: Any,
    ) -> Dict[str, Any]:
        """Submit one turn and return the normalized result (`spec/v1/result.schema.json`)."""
        if turn_id is None:
            raise ValidationError("run() needs turn_id=; it is the idempotency key, so the caller owns it")
        turn = Turn(conversation_id, turn_id, text, user_id, signal, dict(metadata or {}))
        return self.runtime(runtime, **runtime_options).submit(turn)

    def close(self) -> None:
        with self._guard:
            runtimes = list(self._runtimes.values())
            self._runtimes.clear()
        for rt in runtimes:
            rt.close()

    def __enter__(self) -> "AgentSpec":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def _stable(options: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: (v if isinstance(v, (str, int, float, bool, type(None))) else repr(v))
            for k, v in options.items()}


def load(source: ir.Source, bindings: Optional[Bindings] = None, **binds: Union[ToolFn, PeerFn]) -> AgentSpec:
    """An `AgentSpec` from a YAML/JSON path, a YAML/JSON string, or a mapping.

    Keyword arguments bind Python callables by tool id or peer name:
    `load("banking.yaml", issue_refund=fn, specialist=peer)`.
    """
    doc = ir.read_document(source)
    bound = bindings or Bindings()
    tool_kinds = {t["id"]: t.get("kind") for t in doc.get("tools") or []}
    peers = {p["name"] for p in doc.get("a2a") or []}
    for name, fn in binds.items():
        if name in peers:
            bound.peers[name] = fn
        elif tool_kinds.get(name) == "function":
            bound.tools[name] = fn
        elif name in tool_kinds:
            raise ValidationError(f"tool {name!r} has kind {tool_kinds[name]!r}; only function tools take a binding")
        else:
            raise ValidationError(f"{name!r} is neither a function tool nor an a2a peer of this workflow")
    return AgentSpec(doc, bound)


class Agent:
    """Fluent builder. Every method returns the builder; `build()` returns the `AgentSpec`.

    Strings passed as `brain=` or `verifier=` that are not a built-in kind name a Python
    callable registered with `.brain(name, fn)` / `.verifier(name, fn)`.
    """

    def __init__(self, agent_id: str, description: Optional[str] = None) -> None:
        if not agent_id:
            raise ValidationError("Agent() needs a non-empty id")
        self._agent: Dict[str, Any] = {"id": agent_id, "paths": {}}
        if description:
            self._agent["description"] = description
        self._doc: Dict[str, Any] = {"spec_version": ir.SPEC_VERSION, "backend": "local", "agent": self._agent}
        self._tools: Dict[str, Dict[str, Any]] = {}
        self._peers: Dict[str, Dict[str, Any]] = {}
        self._guardrails: Dict[str, Dict[str, Any]] = {}
        self._bindings = Bindings()

    # -- graph ---------------------------------------------------------------

    def route(self, kind: str = "keyword", rules: Optional[Mapping[str, Sequence[str]]] = None,
              default: Optional[str] = None) -> "Agent":
        router: Dict[str, Any] = {"kind": kind}
        if rules:
            router["rules"] = {path: list(words) for path, words in rules.items()}
        if default is not None:
            router["default"] = default
        self._agent["router"] = router
        return self

    def path(
        self,
        name: str,
        brain: Union[str, BrainFn] = "rule",
        prompt: Optional[str] = None,
        tools: Optional[Sequence[str]] = None,
        tool_triggers: Optional[Mapping[str, str]] = None,
        guardrails: Optional[Sequence[str]] = None,
        verifier: VERIFIER_LIKE = None,
        suspend_until: Optional[str] = None,
        **extra: Any,
    ) -> "Agent":
        spec: Dict[str, Any] = {"prompt": prompt or f"You handle {name} requests."}
        if callable(brain):
            self._bindings.brains[name] = brain
            spec["brain"] = "rule"
            spec["x-brain"] = name
        elif brain in BRAIN_KINDS:
            spec["brain"] = brain
        else:
            spec["brain"] = "rule"
            spec["x-brain"] = brain
        if tools:
            spec["tools"] = list(tools)
        if tool_triggers:
            spec["tool_triggers"] = dict(tool_triggers)
        if guardrails:
            spec["guardrails"] = list(guardrails)
        if verifier is not None:
            spec["verifier"] = self._verifier_block(verifier, f"{name}-verifier")
        if suspend_until:
            spec["x-suspend-until"] = suspend_until
        for key, value in extra.items():
            spec["x-" + key[2:].replace("_", "-") if key.startswith("x_") else key] = value
        self._agent["paths"][name] = spec
        return self

    def brain(self, name: str, fn: BrainFn) -> "Agent":
        self._bindings.brains[name] = fn
        return self

    def verifier(self, verifier: VERIFIER_LIKE = "prefix", fn: Optional[VerifierFn] = None,
                 **fields: Any) -> "Agent":
        """Agent-wide verifier: a kind (`prefix`, `regex` with pattern=, `schema` with
        schema=, `none`), or a name plus `fn=` for a Python verifier."""
        if fn is not None:
            if not isinstance(verifier, str):
                raise ValidationError("verifier(name, fn=...) needs a string name")
            self._bindings.verifiers[verifier] = fn
            self._agent["verifier"] = {"kind": "none", "x-verifier": verifier}
            return self
        block = self._verifier_block(verifier, "verifier")
        block.update(fields)
        self._agent["verifier"] = block
        return self

    def _verifier_block(self, verifier: VERIFIER_LIKE, default_name: str) -> Dict[str, Any]:
        if callable(verifier):
            self._bindings.verifiers[default_name] = verifier
            return {"kind": "none", "x-verifier": default_name}
        if isinstance(verifier, Mapping):
            return dict(verifier)
        if verifier in VERIFIER_KINDS:
            return {"kind": verifier}
        return {"kind": "none", "x-verifier": str(verifier)}

    # -- tools -------------------------------------------------------------------

    def tool(self, tool_id: str, kind: str = "constant", description: Optional[str] = None,
             compensation: Optional[str] = None, parameters: Optional[Mapping[str, Any]] = None,
             **fields: Any) -> "Agent":
        """Declare a data-only tool: `constant` (value=), `http` (url=, method=), `failing`."""
        entry: Dict[str, Any] = {"id": tool_id, "kind": kind}
        if description:
            entry["description"] = description
        if compensation:
            entry["compensation"] = compensation
        if parameters is not None:
            entry["parameters"] = dict(parameters)
        entry.update(fields)
        self._tools[tool_id] = entry
        return self

    def use_tool(self, tool_id: str, fn: ToolFn, description: Optional[str] = None,
                 compensation: Optional[str] = None, parameters: Optional[Mapping[str, Any]] = None) -> "Agent":
        """Register a Python function as a tool. Arguments arrive as keyword arguments."""
        if not callable(fn):
            raise ValidationError(f"use_tool({tool_id!r}) needs a callable")
        doc_lines = (fn.__doc__ or "").strip().splitlines()
        self.tool(tool_id, "function", description or (doc_lines[0] if doc_lines else f"Python function {tool_id}"),
                  compensation, parameters)
        self._bindings.tools[tool_id] = fn
        return self

    def use_peer(self, name: str, fn: PeerFn, description: Optional[str] = None, transport: str = "inproc") -> "Agent":
        """Register an A2A peer callable as a tool. `fn(args, delegation)` receives the
        turn's idempotency key on `delegation`."""
        self._peers[name] = {"name": name, "transport": transport,
                             "description": description or f"Peer agent {name}"}
        self._bindings.peers[name] = fn
        return self

    def peer(self, name: str, transport: str, url: Optional[str] = None, description: Optional[str] = None) -> "Agent":
        entry: Dict[str, Any] = {"name": name, "transport": transport,
                                 "description": description or f"Peer agent {name}"}
        if url:
            entry["url"] = url
        self._peers[name] = entry
        return self

    # -- guardrails, memory, retrieval, saga, policies -------------------------

    def guardrail(self, name: str, fn: Optional[GuardrailFn] = None, stage: str = "input",
                  deny: Optional[Sequence[str]] = None, allow: Optional[Sequence[str]] = None,
                  reason: Optional[str] = None) -> "Agent":
        """A named guardrail. Regex by default; `fn(text) -> reason | None` for Python."""
        rail: Dict[str, Any] = {"name": name, "kind": "regex", "stage": stage}
        if reason:
            rail["reason"] = reason
        if fn is not None:
            self._bindings.guardrails[name] = fn
            rail["x-python"] = name
        else:
            if deny:
                rail["deny"] = list(deny)
            if allow:
                rail["allow"] = list(allow)
            if not deny and not allow:
                raise ValidationError(f"guardrail {name!r} needs deny=, allow=, or fn=")
        self._guardrails[name] = rail
        return self

    def with_memory(self, kind: str = "memory", url: Optional[str] = None,
                    on_unavailable: str = "fail", **fields: Any) -> "Agent":
        """Where the conversation log lives: `memory` (default) or `file` with url=<dir>."""
        store: Dict[str, Any] = {"kind": kind, "on_unavailable": on_unavailable}
        if url:
            store["url"] = url
        store.update(fields)
        self._doc.setdefault("stores", {})["conversation"] = store
        return self

    def with_retrieval(self, kb: Sequence[Mapping[str, Any]], dim: int = 256, top_k: int = 4,
                       **fields: Any) -> "Agent":
        block: Dict[str, Any] = {"kb": [dict(p) for p in kb], "dim": dim, "top_k": top_k}
        block.update(fields)
        self._doc["retrieval"] = block
        return self

    def with_llm(self, provider: str = "stub", script: Optional[Sequence[Mapping[str, Any]]] = None,
                 **fields: Any) -> "Agent":
        block: Dict[str, Any] = {"provider": provider}
        if script is not None:
            block["script"] = [dict(s) for s in script]
        block.update(fields)
        self._doc["llm"] = block
        return self

    def saga(self, *steps: Mapping[str, Any]) -> "Agent":
        """Steps run in order; on failure the completed steps compensate in reverse."""
        self._doc["saga"] = {"steps": [dict(s) for s in steps]}
        return self

    def policies(self, ordering: str = "per-conversation", idempotency: str = "turn-id",
                 retry: Union[str, Mapping[str, Any], None] = None, max_attempts: Optional[int] = None,
                 verification: Optional[Mapping[str, Any]] = None, on_tool_error: str = "fail",
                 **fields: Any) -> "Agent":
        block: Dict[str, Any] = {"ordering": ordering, "idempotency": idempotency, "on_tool_error": on_tool_error}
        if isinstance(retry, str):
            block["retry"] = {"kind": retry, "max_attempts": max_attempts or (3 if retry != "none" else 1)}
        elif retry is not None:
            block["retry"] = dict(retry)
        if verification is not None:
            block["verification"] = dict(verification)
        block.update(fields)
        self._doc["policies"] = block
        return self

    def extension(self, key: str, value: Any) -> "Agent":
        if not key.startswith("x-"):
            raise ValidationError("top-level extensions must be x-* keys")
        self._doc[key] = value
        return self

    # -- build -------------------------------------------------------------------

    def document(self) -> Dict[str, Any]:
        doc = dict(self._doc)
        doc["agent"] = dict(self._agent)
        if self._tools:
            doc["tools"] = list(self._tools.values())
        if self._peers:
            doc["a2a"] = list(self._peers.values())
        if self._guardrails:
            doc["guardrails"] = list(self._guardrails.values())
        if "router" not in doc["agent"] and len(doc["agent"]["paths"]) == 1:
            only = next(iter(doc["agent"]["paths"]))
            doc["agent"]["router"] = {"kind": "static", "default": only}
        return doc

    def build(self) -> AgentSpec:
        if not self._agent["paths"]:
            raise ValidationError("an agent needs at least one .path()", "/agent/paths")
        bindings = Bindings(dict(self._bindings.tools), dict(self._bindings.peers), dict(self._bindings.brains),
                            dict(self._bindings.verifiers), dict(self._bindings.guardrails))
        return AgentSpec(self.document(), bindings)

"""Tools: named, side-effecting functions with structured arguments.

Arguments are always mappings validated against the tool's `parameters` JSON Schema when
one is declared; nothing is ever parsed out of reply text.
"""

from __future__ import annotations

import inspect
import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Protocol

from jsonschema import Draft202012Validator

from .errors import ToolError, ValidationError

ToolFn = Callable[..., Any]


class PeerFn(Protocol):
    def __call__(self, args: Mapping[str, Any], delegation: "Delegation") -> Any: ...


@dataclass(frozen=True)
class Delegation:
    """The deduplication key an outbound effect carries (`primitives.md` section 4)."""

    peer: str
    conversation_id: str
    turn_id: str
    call_index: int
    text: str


@dataclass(frozen=True)
class ToolCall:
    """Identity of one invocation inside a turn, `(turn_id, index)`."""

    conversation_id: str
    turn_id: str
    index: int
    text: str = ""


@dataclass
class ToolSpec:
    id: str
    kind: str = "constant"
    value: Any = None
    url: Optional[str] = None
    method: str = "POST"
    timeout_ms: Optional[int] = None
    parameters: Optional[Mapping[str, Any]] = None
    compensation: Optional[str] = None
    fail_attempts: Optional[int] = None
    peer: Optional[Mapping[str, Any]] = None
    extensions: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_document(cls, entry: Mapping[str, Any]) -> "ToolSpec":
        return cls(
            id=entry["id"],
            kind=entry.get("kind", "constant"),
            value=entry.get("value"),
            url=entry.get("url"),
            method=entry.get("method", "POST"),
            timeout_ms=entry.get("timeout_ms"),
            parameters=entry.get("parameters"),
            compensation=entry.get("compensation"),
            fail_attempts=entry.get("x-fail-attempts"),
            extensions={k: v for k, v in entry.items() if k.startswith("x-")},
        )

    @classmethod
    def from_peer(cls, peer: Mapping[str, Any]) -> "ToolSpec":
        return cls(id=peer.get("name") or peer["id"], kind="agent", peer=dict(peer),
                   timeout_ms=peer.get("timeout_ms"))


class ToolRegistry:
    """Tool specs plus the Python bindings (`function` tools, `inproc` peers) behind them.

    A registry represents the outside world, so it is shared across runtime restarts: a
    `failing` tool's attempt budget keeps counting when the runtime is rebuilt from its log.
    """

    def __init__(self) -> None:
        self._specs: Dict[str, ToolSpec] = {}
        self._functions: Dict[str, ToolFn] = {}
        self._peers: Dict[str, PeerFn] = {}
        self._validators: Dict[str, Draft202012Validator] = {}
        self._failing_attempts: Dict[str, int] = {}
        self._guard = threading.Lock()

    def declare(self, spec: ToolSpec) -> None:
        if spec.id in self._specs:
            raise ValidationError(f"duplicate tool id {spec.id!r}")
        self._specs[spec.id] = spec
        if spec.parameters is not None:
            self._validators[spec.id] = Draft202012Validator(spec.parameters)

    def bind_function(self, tool_id: str, fn: ToolFn) -> None:
        self._functions[tool_id] = fn

    def bind_peer(self, name: str, fn: PeerFn) -> None:
        self._peers[name] = fn

    def has_function(self, tool_id: str) -> bool:
        return tool_id in self._functions

    def has_peer(self, name: str) -> bool:
        return name in self._peers

    def spec(self, tool_id: str) -> ToolSpec:
        spec = self._specs.get(tool_id)
        if spec is None:
            raise ValidationError(f"unknown tool {tool_id!r}")
        return spec

    def ids(self) -> Dict[str, ToolSpec]:
        return dict(self._specs)

    def validate_args(self, tool_id: str, args: Mapping[str, Any]) -> None:
        if not isinstance(args, Mapping):
            raise ValidationError(f"tool {tool_id!r} arguments must be a mapping, got {type(args).__name__}")
        validator = self._validators.get(tool_id)
        if validator is None:
            return
        errors = sorted(validator.iter_errors(dict(args)), key=lambda e: list(e.absolute_path))
        if errors:
            pointer = "/".join(str(p) for p in errors[0].absolute_path)
            raise ValidationError(f"tool {tool_id!r} arguments rejected: {errors[0].message}", f"/{pointer}")

    def execute(self, tool_id: str, args: Mapping[str, Any], call: ToolCall) -> Any:
        """Run one attempt. Raises `ToolError` when the tool fails, `ValidationError` for
        bad arguments or an unrunnable tool kind."""
        spec = self.spec(tool_id)
        self.validate_args(tool_id, args)
        kind = spec.kind
        if kind == "constant":
            return spec.value
        if kind == "failing":
            return self._execute_failing(spec)
        if kind == "function":
            return self._execute_function(spec, args)
        if kind == "agent":
            return self._execute_peer(spec, args, call)
        if kind == "http":
            return self._execute_http(spec, args)
        raise ValidationError(f"tool {tool_id!r} has kind {kind!r}, which this runtime cannot execute")

    def _execute_failing(self, spec: ToolSpec) -> Any:
        with self._guard:
            seen = self._failing_attempts.get(spec.id, 0) + 1
            self._failing_attempts[spec.id] = seen
        if spec.fail_attempts is None or seen <= spec.fail_attempts:
            raise ToolError(f"{spec.id} failed")
        return spec.value

    def _execute_function(self, spec: ToolSpec, args: Mapping[str, Any]) -> Any:
        fn = self._functions.get(spec.id)
        if fn is None:
            raise ValidationError(f"tool {spec.id!r} has kind function but no Python function is bound; "
                                  f"call Agent.use_tool({spec.id!r}, fn)")
        try:
            return _call_with_args(fn, args)
        except ToolError:
            raise
        except Exception as exc:  # a tool raising anything is a tool failure, by definition
            raise ToolError(f"{spec.id} failed: {exc}") from exc

    def _execute_peer(self, spec: ToolSpec, args: Mapping[str, Any], call: ToolCall) -> Any:
        peer = spec.peer or {}
        transport = peer.get("transport", "http")
        handler = self._peers.get(spec.id)
        if handler is not None:
            delegation = Delegation(spec.id, call.conversation_id, call.turn_id, call.index, call.text)
            try:
                return handler(args, delegation)
            except ToolError:
                raise
            except Exception as exc:
                raise ToolError(f"{spec.id} failed: {exc}") from exc
        if transport == "inproc":
            return f"[{spec.id}] delegated"
        raise ValidationError(f"peer {spec.id!r} uses transport {transport!r} and no in-process handler is "
                              f"bound; this runtime delegates in-process only (Agent.use_peer)")

    def _execute_http(self, spec: ToolSpec, args: Mapping[str, Any]) -> Any:
        if not spec.url:
            raise ValidationError(f"http tool {spec.id!r} has no url")
        timeout = (spec.timeout_ms or 10_000) / 1000.0
        if spec.method == "GET":
            query = urllib.parse.urlencode({k: json.dumps(v) if not isinstance(v, str) else v
                                            for k, v in args.items()})
            url = f"{spec.url}?{query}" if query else spec.url
            request = urllib.request.Request(url, method="GET")
        else:
            body = json.dumps(dict(args)).encode("utf-8")
            request = urllib.request.Request(spec.url, data=body, method="POST",
                                             headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise ToolError(f"{spec.id} failed: {exc}") from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw


def _call_with_args(fn: ToolFn, args: Mapping[str, Any]) -> Any:
    """Call `fn(**args)`; a function declaring exactly one parameter that the arguments do
    not name receives the whole mapping instead."""
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return fn(**args)
    try:
        signature.bind(**args)
    except TypeError as exc:
        params = list(signature.parameters.values())
        if len(params) == 1 and params[0].kind in (params[0].POSITIONAL_ONLY, params[0].POSITIONAL_OR_KEYWORD):
            return fn(dict(args))
        name = getattr(fn, "__name__", repr(fn))
        raise ValidationError(f"arguments {sorted(args)} do not match the signature of {name}") from exc
    return fn(**args)

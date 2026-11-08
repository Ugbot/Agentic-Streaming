"""Tools: named, side-effecting functions with structured arguments.

Arguments are always mappings validated against the tool's `parameters` JSON Schema when
one is declared; nothing is ever parsed out of reply text.
"""

from __future__ import annotations

import contextvars
import hashlib
import http.client
import inspect
import json
import queue
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Tuple

from jsonschema import Draft202012Validator

from . import egress, secret_refs
from .errors import OutcomeUnknown, ToolError, ValidationError

ToolFn = Callable[..., Any]

DEFAULT_TOOL_TIMEOUT_MS = 10_000
DEFAULT_PEER_TIMEOUT_MS = 30_000
PEER_BACKOFF_MS = 200  # before a peer's own retry, doubling, as the JVM client waits


def idempotency_key(conversation_id: str, turn_id: str, index: int) -> str:
    """`primitives.md` section 10: derived from recorded input alone, so a re-run regenerates it."""
    material = b"\x00".join((conversation_id.encode("utf-8"), turn_id.encode("utf-8"), str(index).encode("ascii")))
    return hashlib.sha256(material).hexdigest()


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

    @property
    def idempotency_key(self) -> str:
        return idempotency_key(self.conversation_id, self.turn_id, self.call_index)


@dataclass(frozen=True)
class ToolCall:
    """Identity of one invocation inside a turn, `(turn_id, index)`."""

    conversation_id: str
    turn_id: str
    index: int
    text: str = ""

    @property
    def idempotency_key(self) -> str:
        return idempotency_key(self.conversation_id, self.turn_id, self.index)


_CURRENT_CALL: "contextvars.ContextVar[Optional[ToolCall]]" = contextvars.ContextVar("agentic_tool_call", default=None)


def current_call() -> Optional[ToolCall]:
    """The call a function tool is running as, with its `idempotency_key`. None outside a tool."""
    return _CURRENT_CALL.get()


@dataclass
class ToolWorld:
    """What the outside world has seen: it is not the runtime's memory, so it outlives a restart.

    The fixtures' `failing`, `counting` and stubbed model steps count here. A deployed runtime hands
    its world to the runtime `restart()` builds, which is how a crash leaves the world as it was.
    """

    failing_attempts: Dict[str, int] = field(default_factory=dict)
    counted: Dict[Tuple[str, str], List[str]] = field(default_factory=dict)
    model_attempts: Dict[int, int] = field(default_factory=dict)
    model_calls: Dict[int, int] = field(default_factory=dict)
    guard: threading.Lock = field(default_factory=threading.Lock)


DEFAULT_EFFECT = "non_idempotent"  # the safe guess about a tool nobody has described


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
    effect: str = DEFAULT_EFFECT
    description: str = ""
    fail_attempts: Optional[int] = None
    peer: Optional[Mapping[str, Any]] = None
    extensions: Dict[str, Any] = field(default_factory=dict)
    auth: Optional[Mapping[str, Any]] = None

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
            effect=entry.get("effect", DEFAULT_EFFECT),
            description=entry.get("description", ""),
            fail_attempts=entry.get("x-fail-attempts"),
            extensions={k: v for k, v in entry.items() if k.startswith("x-")},
            auth=entry.get("auth"),
        )

    @classmethod
    def from_peer(cls, peer: Mapping[str, Any]) -> "ToolSpec":
        return cls(id=peer.get("name") or peer["id"], kind="agent", peer=dict(peer),
                   effect=peer.get("effect", DEFAULT_EFFECT),
                   timeout_ms=peer.get("timeout_ms"), description=peer.get("description", ""))


class ToolRegistry:
    """Tool specs plus the Python bindings (`function` tools, `inproc` peers) behind them.

    The `world` behind a registry is the outside world, so it is shared across runtime restarts: a
    `failing` tool's attempt budget keeps counting when the runtime is rebuilt from its log.
    """

    def __init__(self, world: Optional[ToolWorld] = None) -> None:
        self.world = world or ToolWorld()
        self._specs: Dict[str, ToolSpec] = {}
        self._functions: Dict[str, ToolFn] = {}
        self._peers: Dict[str, PeerFn] = {}
        self._validators: Dict[str, Draft202012Validator] = {}

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
        token = _CURRENT_CALL.set(call)
        try:
            return self._run(spec, args, call)
        finally:
            _CURRENT_CALL.reset(token)

    def _run(self, spec: ToolSpec, args: Mapping[str, Any], call: ToolCall) -> Any:
        kind = spec.kind
        if kind == "constant":
            return spec.value
        if kind == "failing":
            return self._execute_failing(spec)
        if kind == "counting":
            return self._execute_counting(spec, call)
        if kind == "hanging":
            return _bounded(spec, DEFAULT_TOOL_TIMEOUT_MS, threading.Event().wait)
        if kind == "function":
            return self._execute_function(spec, args)
        if kind == "agent":
            return self._execute_peer(spec, args, call)
        if kind == "http":
            return self._execute_http(spec, args, call)
        raise ValidationError(f"tool {spec.id!r} has kind {kind!r}, which this runtime cannot execute")

    def _execute_failing(self, spec: ToolSpec) -> Any:
        with self.world.guard:
            seen = self.world.failing_attempts.get(spec.id, 0) + 1
            self.world.failing_attempts[spec.id] = seen
        if spec.fail_attempts is None or seen <= spec.fail_attempts:
            raise ToolError(f"{spec.id} failed")
        return spec.value

    def _execute_counting(self, spec: ToolSpec, call: ToolCall) -> int:
        """The outside world in miniature: how many calls landed for this conversation, or how many
        distinct keys under `x-count-by: keys`. It answers with that number, so a repeated effect
        shows in the reply."""
        key = call.idempotency_key
        with self.world.guard:
            seen = self.world.counted.setdefault((spec.id, call.conversation_id), [])
            if spec.extensions.get("x-count-by") != "keys" or key not in seen:
                seen.append(key)
            return len(seen)

    def _execute_function(self, spec: ToolSpec, args: Mapping[str, Any]) -> Any:
        fn = self._functions.get(spec.id)
        if fn is None:
            raise ValidationError(f"tool {spec.id!r} has kind function but no Python function is bound; "
                                  f"call Agent.use_tool({spec.id!r}, fn)")
        try:
            return _bounded(spec, DEFAULT_TOOL_TIMEOUT_MS, lambda: _call_with_args(fn, args))
        except (ToolError, ValidationError):
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
                return _bounded(spec, DEFAULT_PEER_TIMEOUT_MS, lambda: handler(args, delegation))
            except ToolError:
                raise
            except Exception as exc:
                raise ToolError(f"{spec.id} failed: {exc}") from exc
        if transport == "inproc":
            return f"[{spec.id}] delegated"
        if transport == "http" and peer.get("url"):
            return self._execute_http_peer(spec, args, call)
        raise ValidationError(f"peer {spec.id!r} uses transport {transport!r} and no in-process handler is "
                              f"bound; this runtime delegates in-process only (Agent.use_peer)")

    def _execute_http_peer(self, spec: ToolSpec, args: Mapping[str, Any], call: ToolCall) -> Any:
        """One delegation to a peer's A2A front door, `POST <url>/agent`, under the call's key. The url
        may be a `${NAME}` link, expanded now under the operator's list; the bearer is a reference,
        resolved now. A 4xx, a redirect or a refusal before sending is a definite failure; a 5xx, a
        timeout or a lost connection may have landed, so it is `OutcomeUnknown`."""
        peer = spec.peer or {}
        try:
            base = secret_refs.expand(str(peer["url"])).rstrip("/")
        except ValueError as exc:
            raise ToolError(f"{spec.id} failed: {exc}") from exc
        if not base:
            raise ToolError(f"{spec.id} failed: its url {peer['url']} is empty; set it in the environment")
        target = base + "/agent"
        try:
            pin = egress.check(target)
        except egress.EgressBlocked as exc:
            raise ToolError(f"{spec.id} failed: blocked by egress policy: {exc}") from exc
        headers = {"Content-Type": "application/json", "Idempotency-Key": call.idempotency_key}
        auth = peer.get("auth")
        if auth is not None:
            try:
                headers["Authorization"] = f"Bearer {secret_refs.resolve(auth['bearer'])}"
            except secret_refs.SecretUnresolved as exc:
                raise ToolError(f"{spec.id} failed: {exc}") from exc
        body = {"conversation_id": str(args.get("conversation_id", call.conversation_id)),
                "text": str(args.get("text", call.text)),
                "user_id": str(args.get("user_id", args.get("user", "anonymous")))}
        # The client's own retries, under one key, so the peer can deduplicate: what is sent again is
        # what may not have landed. A non_idempotent peer gets none (`primitives.md` section 10).
        retries = 0 if spec.effect == "non_idempotent" else int(peer.get("retries", 0))
        for attempt in range(retries + 1):
            try:
                return self._post_to_peer(spec, target, pin, headers, body)
            except OutcomeUnknown:
                if attempt >= retries:
                    raise
                time.sleep(PEER_BACKOFF_MS * 2 ** attempt / 1000.0)
        raise AssertionError("unreachable")

    def _post_to_peer(self, spec: ToolSpec, target: str, pin: egress.Pin, headers: Mapping[str, str],
                      body: Mapping[str, Any]) -> Any:
        request = urllib.request.Request(target, data=json.dumps(body).encode("utf-8"), method="POST",
                                         headers=dict(headers))
        timeout = (spec.timeout_ms or DEFAULT_PEER_TIMEOUT_MS) / 1000.0
        try:
            with egress.urlopen(request, timeout, pin) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            failure = OutcomeUnknown if exc.code >= 500 else ToolError
            raise failure(f"{spec.id} failed: HTTP {exc.code}") from exc
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            raise OutcomeUnknown(f"{spec.id} failed: {type(exc).__name__}") from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    def _execute_http(self, spec: ToolSpec, args: Mapping[str, Any], call: ToolCall) -> Any:
        """A 2xx is the result. A 4xx is a definite failure. A 5xx, a timeout or a lost connection
        may have landed, so it is `OutcomeUnknown`."""
        if not spec.url:
            raise ValidationError(f"http tool {spec.id!r} has no url")
        try:
            pin = egress.check(spec.url)
        except egress.EgressBlocked as exc:
            raise ToolError(f"{spec.id} failed: blocked by egress policy") from exc
        timeout = (spec.timeout_ms or DEFAULT_TOOL_TIMEOUT_MS) / 1000.0
        headers = {"Idempotency-Key": call.idempotency_key}
        if spec.auth is not None:
            try:
                headers["Authorization"] = f"Bearer {secret_refs.resolve(spec.auth['bearer'])}"
            except secret_refs.SecretUnresolved as exc:
                raise ToolError(f"{spec.id} failed: {exc}") from exc
        if spec.method == "GET":
            query = urllib.parse.urlencode({k: json.dumps(v) if not isinstance(v, str) else v
                                            for k, v in args.items()})
            url = f"{spec.url}?{query}" if query else spec.url
            request = urllib.request.Request(url, method="GET", headers=headers)
        else:
            body = json.dumps(dict(args)).encode("utf-8")
            request = urllib.request.Request(spec.url, data=body, method="POST",
                                             headers={**headers, "Content-Type": "application/json"})
        try:
            with egress.urlopen(request, timeout, pin) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            failure = OutcomeUnknown if exc.code >= 500 else ToolError
            raise failure(f"{spec.id} failed: HTTP {exc.code}") from exc
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            raise OutcomeUnknown(f"{spec.id} failed: {exc}") from exc
        except ValueError as exc:  # a url that could not be sent, so nothing can have landed
            raise ToolError(f"{spec.id} failed: {exc}") from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw


def _bounded(spec: ToolSpec, default_ms: int, call: Callable[[], Any]) -> Any:
    return bounded(spec.id, spec.timeout_ms or default_ms, call)


def bounded(name: str, limit_ms: int, call: Callable[[], Any]) -> Any:
    """Run `call` for at most `limit_ms`. It runs on its own daemon thread, because a limit must hold
    whatever the call is doing. Python cannot stop a thread, so a late call runs on to its end and
    its answer is dropped: a timeout says the answer did not arrive, not that nothing happened."""
    outcome: "queue.Queue[tuple]" = queue.Queue(maxsize=1)
    context = contextvars.copy_context()

    def run() -> None:
        try:
            outcome.put((True, context.run(call)))
        except BaseException as exc:  # handed to the caller, who decides what it means
            outcome.put((False, exc))

    threading.Thread(target=run, name=f"tool-{name}", daemon=True).start()
    try:
        ok, value = outcome.get(timeout=limit_ms / 1000.0)
    except queue.Empty:
        raise OutcomeUnknown(f"{name} timed out after {limit_ms} ms") from None
    if ok:
        return value
    raise value


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

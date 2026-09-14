"""The turn engine: `handle(turn) -> normalized result`, one conversation at a time.

The engine owns no ordering and no threads. A runtime supplies the single writer per
conversation and calls `handle` from it. Everything the engine remembers about a
conversation is a fold over the event log, so `recover()` rebuilds it after a restart.
"""

from __future__ import annotations

import json
import random
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from jsonschema import Draft202012Validator

from .bindings import Bindings
from .errors import AgenticError, ToolError, ValidationError
from .events import ChatMessage, Event, EventLog, Turn, reduce_state, transcript
from .retrieval import KnowledgeBase, Passage
from .tools import ToolCall, ToolRegistry, ToolSpec

Clock = Callable[[], int]
Sleep = Callable[[float], None]

_RECORDED_CALL_EVENTS = frozenset({"tool_called", "delegated", "compensation_step"})

# router.kind values this runtime implements (spec/v1 also names llm and classifier).
ROUTER_KINDS = ("keyword", "static")


def wall_clock_ms() -> int:
    return int(time.time() * 1000)


def retry_delay_ms(policy: Mapping[str, Any], attempt: int, rng: Optional[random.Random] = None) -> int:
    """Delay before retry number `attempt` (1 = after the first failure), per `policies.retry`."""
    kind = policy.get("kind", "none")
    if kind == "none":
        return 0
    initial = policy.get("initial_delay_ms", 100)
    cap = policy.get("max_delay_ms", 5000)
    if kind == "fixed":
        delay = initial
    else:
        delay = initial * (policy.get("multiplier", 2.0) ** (attempt - 1))
    delay = min(int(delay), cap)
    if policy.get("jitter", False) and delay > 0:
        delay = (rng or random).randint(0, delay)
    return int(delay)


@dataclass
class _Pending:
    turn_id: str
    path: str
    text: str
    user_id: str


@dataclass
class _TurnContext:
    turn: Turn
    text: str
    started_ms: int
    events: List[Event] = field(default_factory=list)
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    next_index: int = 0

    def take_index(self) -> int:
        index = self.next_index
        self.next_index += 1
        return index


class _Rejected(AgenticError):
    error_class = "guardrail"


class Engine:
    def __init__(
        self,
        doc: Mapping[str, Any],
        tools: ToolRegistry,
        log: EventLog,
        bindings: Optional[Bindings] = None,
        clock: Clock = wall_clock_ms,
        sleep: Sleep = time.sleep,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.doc = doc
        self.tools = tools
        self.log = log
        self.bindings = bindings or Bindings()
        self.clock = clock
        self.sleep = sleep
        self.rng = rng
        agent = doc["agent"]
        self.paths: Dict[str, Mapping[str, Any]] = agent["paths"]
        self.router: Mapping[str, Any] = agent.get("router") or {}
        self.verifier: Mapping[str, Any] = agent.get("verifier") or {"kind": "prefix"}
        self.policies: Mapping[str, Any] = doc.get("policies") or {}
        self.saga: Optional[Mapping[str, Any]] = doc.get("saga")
        self.context: Mapping[str, Any] = doc.get("context") or {}
        self.llm: Mapping[str, Any] = doc.get("llm") or {}
        self.guardrails: Sequence[Mapping[str, Any]] = doc.get("guardrails") or []
        path_scoped = {name for path in self.paths.values() for name in path.get("guardrails") or []}
        self.global_guardrails = [g for g in self.guardrails if g.get("name") not in path_scoped]
        self.named_guardrails = {g["name"]: g for g in self.guardrails if g.get("name")}
        retrieval = doc.get("retrieval") or {}
        dim = (doc.get("embeddings") or {}).get("dim", retrieval.get("dim", 256))
        self.kb = KnowledgeBase(retrieval.get("kb") or [], dim, retrieval.get("top_k", 4))
        self._results: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._suspended: Dict[Tuple[str, str], _Pending] = {}
        self._guard = threading.Lock()
        self._schema_validators: Dict[int, Draft202012Validator] = {}

    # -- lifecycle -----------------------------------------------------------

    def recover(self) -> None:
        """Rebuild every derived view from the log. Runs no tool and appends nothing."""
        results: Dict[Tuple[str, str], Dict[str, Any]] = {}
        suspended: Dict[Tuple[str, str], _Pending] = {}
        for conversation_id in self.log.conversation_ids():
            log = self.log.read(conversation_id)
            by_turn: Dict[str, List[Event]] = {}
            for event in log:
                by_turn.setdefault(event.turn_id, []).append(event)
            for turn_id, events in by_turn.items():
                terminal = events[-1]
                if terminal.type == "turn_suspended":
                    p = terminal.payload
                    suspended[(conversation_id, turn_id)] = _Pending(
                        turn_id, str(p["path"]), str(p.get("text", "")), str(p.get("user_id", "anonymous")))
                    results[(conversation_id, turn_id)] = self._result_from_events(
                        conversation_id, turn_id, events, log, "suspended")
                elif terminal.type == "turn_completed":
                    results[(conversation_id, turn_id)] = self._result_from_events(
                        conversation_id, turn_id, events, log, "completed")
                elif terminal.type == "turn_failed":
                    results[(conversation_id, turn_id)] = self._result_from_events(
                        conversation_id, turn_id, events, log, str(terminal.payload.get("status", "failed")))
        with self._guard:
            self._results = results
            self._suspended = suspended

    def state(self, conversation_id: str) -> Dict[str, Any]:
        return reduce_state(self.log.read(conversation_id), self.context)

    def transcript(self, conversation_id: str) -> List[ChatMessage]:
        return transcript(self.log.read(conversation_id), self.context)

    def suspended_turns(self, conversation_id: str) -> List[str]:
        with self._guard:
            return [turn_id for (cid, turn_id) in self._suspended if cid == conversation_id]

    # -- the turn ------------------------------------------------------------

    def handle(self, turn: Turn) -> Dict[str, Any]:
        """Process one turn to a terminal status. Must be called by the conversation's
        single writer."""
        key = (turn.conversation_id, turn.turn_id)
        idempotent = self.policies.get("idempotency", "turn-id") == "turn-id"
        with self._guard:
            pending = self._suspended.get(key)
            prior = self._results.get(key)

        if turn.signal is not None:
            if pending is None:
                ctx = _TurnContext(turn, turn.text, self.clock())
                return self._fail_validation(ctx, None, f"no suspended turn {turn.turn_id!r} to resume")
            return self._resume(turn, pending)
        if idempotent and prior is not None:
            duplicate = dict(prior)
            duplicate["status"] = "duplicate"
            duplicate["events"] = []
            duplicate["runtime_detail"] = {"duplicate_of": prior.get("runtime_detail", {}), "re_executed": False}
            return duplicate

        ctx = _TurnContext(turn, turn.text, self.clock())
        self._append(ctx, "turn_received", {"turn_id": turn.turn_id, "text": turn.text, "user_id": turn.user_id})
        try:
            blocked = self._check_guardrails(self.global_guardrails, "input", turn.text)
            if blocked is not None:
                self._append(ctx, "guardrail_rejected", {"reason": blocked, "stage": "input"})
                return self._finish(ctx, "rejected", None, None, {"class": "guardrail", "message": blocked})

            path = self._route(turn.text)
            self._append(ctx, "routed", {"path": path})
            spec = self.paths[path]

            blocked = self._check_guardrails(self._path_guardrails(spec), "input", turn.text)
            if blocked is not None:
                self._append(ctx, "guardrail_rejected", {"reason": blocked, "stage": "input", "path": path})
                return self._finish(ctx, "rejected", path, None, {"class": "guardrail", "message": blocked})

            if "x-suspend-until" in spec:
                payload = {"turn_id": turn.turn_id, "path": path, "text": turn.text,
                           "user_id": turn.user_id, "until": spec["x-suspend-until"]}
                self._append(ctx, "turn_suspended", payload)
                with self._guard:
                    self._suspended[key] = _Pending(turn.turn_id, path, turn.text, turn.user_id)
                return self._finish(ctx, "suspended", path, None, None)

            if self.saga is not None:
                return self._run_saga(ctx, path)
            return self._run_brain(ctx, path)
        except ValidationError as exc:
            return self._fail_validation(ctx, self._routed_path(ctx), str(exc))

    def _resume(self, turn: Turn, pending: _Pending) -> Dict[str, Any]:
        key = (turn.conversation_id, turn.turn_id)
        with self._guard:
            self._suspended.pop(key, None)
        resumed = Turn(turn.conversation_id, turn.turn_id, pending.text, pending.user_id, None, turn.metadata)
        ctx = _TurnContext(resumed, pending.text, self.clock())
        self._append(ctx, "turn_resumed", {"turn_id": turn.turn_id, "signal": dict(turn.signal or {})})
        try:
            if self.saga is not None:
                return self._run_saga(ctx, pending.path)
            return self._run_brain(ctx, pending.path)
        except ValidationError as exc:
            return self._fail_validation(ctx, pending.path, str(exc))

    def _run_brain(self, ctx: _TurnContext, path: str) -> Dict[str, Any]:
        verification = self.policies.get("verification") or {}
        attempts = verification.get("max_attempts", 1)
        spec = self.paths[path]
        self._append(ctx, "brain_started", {"path": path, "brain": spec.get("x-brain") or spec.get("brain", "rule")})
        reply: Optional[str] = None
        for _ in range(attempts):
            try:
                reply = self._draft(ctx, path)
            except ToolError as exc:
                return self._finish(ctx, "failed", path, None, {"class": "tool", "message": str(exc)},
                                    event=("turn_failed", {"status": "failed", "reason": str(exc)}))
            self._append(ctx, "reply_drafted", {"reply": reply})
            blocked = self._check_guardrails(list(self.global_guardrails) + self._path_guardrails(spec),
                                             "output", reply)
            if blocked is not None:
                self._append(ctx, "guardrail_rejected", {"reason": blocked, "stage": "output", "path": path})
                return self._finish(ctx, "rejected", path, reply, {"class": "guardrail", "message": blocked})
            if self._verify(spec, reply):
                return self._complete(ctx, path, reply)
            self._append(ctx, "verification_failed", {"reply": reply})
        status = "unverified" if verification.get("on_exhausted", "unverified") == "unverified" else "failed"
        return self._finish(ctx, status, path, reply,
                            {"class": "verification", "message": "verifier rejected the reply"})

    def _draft(self, ctx: _TurnContext, path: str) -> str:
        spec = self.paths[path]
        custom = spec.get("x-brain")
        if custom is not None:
            return str(self.bindings.brains[custom](_BrainView(self, ctx, path)))
        brain = spec.get("brain", "rule")
        if brain == "llm":
            return self._draft_llm(ctx, path)
        low = ctx.text.lower()
        for keyword, tool_id in (spec.get("tool_triggers") or {}).items():
            if keyword.lower() in low:
                result = self._invoke(ctx, tool_id, {"user": ctx.turn.user_id})
                return f"[{path}] {tool_id} returned {result}"
        if len(self.kb):
            passages = self._retrieve(ctx, ctx.text)
            if passages and passages[0].score > spec.get("threshold", 0.15):
                return f"[{path}] {passages[0].text}"
        return f'[{path}] I can help with {path} questions. You said: "{ctx.text}"'

    def _draft_llm(self, ctx: _TurnContext, path: str) -> str:
        provider = self.llm.get("provider", "stub")
        if provider != "stub":
            raise ValidationError(f"llm provider {provider!r} is not available in this runtime; "
                                  f"only the scripted stub provider runs locally", "/llm/provider")
        allowed = set(self.paths[path].get("tools") or [])
        for i, step in enumerate(self.llm.get("script") or []):
            if "tool" in step:
                if allowed and step["tool"] not in allowed:
                    raise ValidationError(f"scripted tool {step['tool']!r} is not in the path's tools",
                                          f"/llm/script/{i}/tool")
                self._invoke(ctx, step["tool"], step.get("args") or {})
            else:
                return str(step["text"])
        raise ValidationError("the llm script ended without a final text", "/llm/script")

    def _run_saga(self, ctx: _TurnContext, path: str) -> Dict[str, Any]:
        assert self.saga is not None
        done: List[Mapping[str, Any]] = []
        for step in self.saga["steps"]:
            try:
                self._invoke(ctx, step["tool"], step.get("args") or {})
            except ToolError as exc:
                self._append(ctx, "compensation_started", {"failed_step": step.get("name", step["tool"])})
                compensated = 0
                for completed in reversed(done):
                    undo = completed.get("compensate_with") or self.tools.spec(completed["tool"]).compensation
                    if undo:
                        self._invoke(ctx, undo, {}, event="compensation_step")
                        compensated += 1
                self._append(ctx, "compensation_completed", {"steps": len(done), "compensated": compensated})
                return self._finish(ctx, "failed", path, None, {"class": "tool", "message": str(exc)},
                                    event=("turn_failed", {"status": "failed", "reason": str(exc)}))
            done.append(step)
        return self._complete(ctx, path, f"[{path}] saga completed")

    # -- pieces --------------------------------------------------------------

    def _route(self, text: str) -> str:
        kind = self.router.get("kind", "keyword")
        if kind not in ROUTER_KINDS:
            raise ValidationError(
                f"router kind {kind!r} is not available in this runtime; supported kinds: {', '.join(ROUTER_KINDS)}",
                "/agent/router/kind")
        low = (text or "").lower()
        if kind == "keyword":
            for path, keywords in (self.router.get("rules") or {}).items():
                if any(str(k).lower() in low for k in keywords):
                    return str(path)
        default = self.router.get("default")
        if default is None:
            if len(self.paths) == 1:
                return next(iter(self.paths))
            raise ValidationError("no router rule matched and router.default is not set", "/agent/router/default")
        return str(default)

    def _path_guardrails(self, spec: Mapping[str, Any]) -> List[Mapping[str, Any]]:
        return [self.named_guardrails[name] for name in spec.get("guardrails") or []]

    def _check_guardrails(self, rails: Sequence[Mapping[str, Any]], stage: str, text: str) -> Optional[str]:
        for rail in rails:
            applies = rail.get("stage", "input")
            if applies != stage and applies != "both":
                continue
            custom = rail.get("x-python")
            if custom is not None:
                reason = self.bindings.guardrails[custom](text or "")
                if reason is not None:
                    return str(reason) or rail.get("reason", "denied")
                continue
            if rail["kind"] != "regex":
                raise ValidationError(f"guardrail kind {rail['kind']!r} is not available in this runtime")
            for pattern in rail.get("allow") or []:
                if not re.search(pattern, text or "", re.IGNORECASE):
                    return str(rail.get("reason", "denied"))
            for pattern in rail.get("deny") or []:
                if re.search(pattern, text or "", re.IGNORECASE):
                    return str(rail.get("reason", "denied"))
        return None

    def _verifier_for(self, path_spec: Mapping[str, Any]) -> Mapping[str, Any]:
        """primitives.md section 5: the path's verifier, else ``agent.verifier``, else ``prefix``."""
        own = path_spec.get("verifier")
        return own if own is not None else self.verifier

    def _verify(self, path_spec: Mapping[str, Any], reply: str) -> bool:
        verifier = self._verifier_for(path_spec)
        custom = verifier.get("x-verifier")
        if custom is not None:
            return bool(self.bindings.verifiers[custom](reply))
        kind = verifier.get("kind", "prefix")
        if kind == "none":
            return True
        if kind == "prefix":
            return reply.startswith("[")
        if kind == "regex":
            return re.search(verifier["pattern"], reply) is not None
        if kind == "schema":
            schema = verifier.get("schema") or path_spec.get("output_schema") or {}
            validator = self._schema_validators.get(id(schema))
            if validator is None:
                validator = self._schema_validators[id(schema)] = Draft202012Validator(schema)
            try:
                return bool(validator.is_valid(json.loads(reply)))
            except json.JSONDecodeError:
                return False
        raise ValidationError(f"verifier kind {kind!r} is not available in this runtime")

    def _retrieve(self, ctx: _TurnContext, query: str, k: int = 0) -> List[Passage]:
        passages = self.kb.retrieve(query, k)
        self._append(ctx, "retrieved", {"ids": [p.id for p in passages],
                                        "scores": [round(p.score, 6) for p in passages]})
        return passages

    def _invoke(self, ctx: _TurnContext, tool_id: str, args: Mapping[str, Any],
                event: Optional[str] = None) -> Any:
        spec: ToolSpec = self.tools.spec(tool_id)
        self.tools.validate_args(tool_id, args)
        index = ctx.take_index()
        call = ToolCall(ctx.turn.conversation_id, ctx.turn.turn_id, index, ctx.text)
        retry = self.policies.get("retry") or {}
        max_attempts = retry.get("max_attempts", 1) if retry.get("kind", "none") != "none" else 1
        args = dict(args)
        for attempt in range(1, max_attempts + 1):
            try:
                result = self.tools.execute(tool_id, args, call)
            except ToolError as exc:
                ctx.tool_calls.append({"tool": tool_id, "index": index, "args": args,
                                       "error": str(exc), "attempt": attempt})
                self._append(ctx, "tool_failed", {"tool": tool_id, "index": index, "attempt": attempt,
                                                  "args": args, "error": str(exc)})
                if attempt == max_attempts:
                    if self.policies.get("on_tool_error", "fail") == "continue":
                        return None
                    raise
                delay = retry_delay_ms(retry, attempt, self.rng)
                if delay:
                    self.sleep(delay / 1000.0)
                continue
            ctx.tool_calls.append({"tool": tool_id, "index": index, "args": args,
                                   "result": result, "attempt": attempt})
            kind = event or ("delegated" if spec.kind == "agent" else "tool_called")
            self._append(ctx, kind, {"tool": tool_id, "index": index, "attempt": attempt,
                                     "args": args, "result": result})
            return result
        raise AssertionError("unreachable")

    # -- bookkeeping ---------------------------------------------------------

    def _append(self, ctx: _TurnContext, kind: str, payload: Mapping[str, Any]) -> Event:
        event = self.log.append(ctx.turn.conversation_id, ctx.turn.turn_id, kind, payload,
                                self.clock(), ctx.turn.metadata)
        ctx.events.append(event)
        return event

    def _routed_path(self, ctx: _TurnContext) -> Optional[str]:
        for event in reversed(ctx.events):
            if event.type == "routed":
                return str(event.payload["path"])
        return None

    def _complete(self, ctx: _TurnContext, path: str, reply: str) -> Dict[str, Any]:
        self._append(ctx, "memory_written", {"messages": [{"role": "user", "text": ctx.text},
                                                          {"role": "assistant", "text": reply}]})
        return self._finish(ctx, "completed", path, reply, None, event=("turn_completed", {"reply": reply}))

    def _fail_validation(self, ctx: _TurnContext, path: Optional[str], message: str) -> Dict[str, Any]:
        return self._finish(ctx, "failed", path, None, {"class": "validation", "message": message},
                            event=("turn_failed", {"status": "failed", "reason": message}))

    def _finish(self, ctx: _TurnContext, status: str, path: Optional[str], reply: Optional[str],
                error: Optional[Dict[str, Any]],
                event: Optional[Tuple[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        if event is not None:
            payload = dict(event[1])
            if error is not None:
                payload["error"] = error
            self._append(ctx, event[0], payload)
        elif status in ("rejected", "unverified"):
            self._append(ctx, "turn_failed", {"status": status, "error": error})
        result = {
            "conversation_id": ctx.turn.conversation_id,
            "turn_id": ctx.turn.turn_id,
            "status": status,
            "path": path,
            "reply": reply,
            "state": self.state(ctx.turn.conversation_id),
            "tool_calls": list(ctx.tool_calls),
            "events": [e.normalized() for e in ctx.events],
            "error": error,
            "runtime_detail": {"runtime": "local", "started_ms": ctx.started_ms,
                               "elapsed_ms": self.clock() - ctx.started_ms},
        }
        with self._guard:
            self._results[(ctx.turn.conversation_id, ctx.turn.turn_id)] = result
        return result

    def _result_from_events(self, conversation_id: str, turn_id: str, events: Sequence[Event],
                            log: Sequence[Event], status: str) -> Dict[str, Any]:
        """A recorded result, rebuilt from the turn's events alone."""
        path: Optional[str] = None
        reply: Optional[str] = None
        error: Optional[Dict[str, Any]] = None
        tool_calls: List[Dict[str, Any]] = []
        for event in events:
            p = event.payload
            if event.type == "routed":
                path = str(p["path"])
            elif event.type == "reply_drafted":
                reply = str(p["reply"])
            elif event.type == "turn_completed":
                reply = str(p["reply"])
            elif event.type == "turn_failed":
                if p.get("error"):
                    error = dict(p["error"])
                elif error is None:
                    error = {"class": "fatal", "message": str(p.get("reason", "turn failed"))}
            elif event.type == "guardrail_rejected":
                error = {"class": "guardrail", "message": str(p.get("reason", "denied"))}
            elif event.type in _RECORDED_CALL_EVENTS:
                tool_calls.append({"tool": p["tool"], "index": p["index"], "args": dict(p.get("args") or {}),
                                   "result": p.get("result"), "attempt": p.get("attempt", 1)})
            elif event.type == "tool_failed":
                tool_calls.append({"tool": p["tool"], "index": p["index"], "args": dict(p.get("args") or {}),
                                   "error": str(p.get("error", "failed")), "attempt": p.get("attempt", 1)})
        last_sequence = events[-1].sequence
        return {
            "conversation_id": conversation_id,
            "turn_id": turn_id,
            "status": status,
            "path": path,
            "reply": reply if status in ("completed", "unverified", "rejected") else None,
            "state": reduce_state((e for e in log if e.sequence <= last_sequence), self.context),
            "tool_calls": tool_calls,
            "events": [e.normalized() for e in events],
            "error": error,
            "runtime_detail": {"runtime": "local", "recovered": True},
        }


class _BrainView:
    """The `BrainContext` handed to a custom brain."""

    def __init__(self, engine: Engine, ctx: _TurnContext, path: str) -> None:
        self._engine = engine
        self._ctx = ctx
        self._path = path

    @property
    def text(self) -> str:
        return self._ctx.text

    @property
    def path(self) -> str:
        return self._path

    @property
    def user_id(self) -> str:
        return self._ctx.turn.user_id

    @property
    def transcript(self) -> Sequence[ChatMessage]:
        return self._engine.transcript(self._ctx.turn.conversation_id)

    def invoke(self, tool_id: str, args: Mapping[str, Any]) -> Any:
        allowed = self._engine.paths[self._path].get("tools")
        if allowed and tool_id not in allowed:
            raise ValidationError(f"tool {tool_id!r} is not in path {self._path!r} tools {sorted(allowed)}")
        return self._engine._invoke(self._ctx, tool_id, args)

    def retrieve(self, query: str, k: int = 0) -> List[Passage]:
        return self._engine._retrieve(self._ctx, query, k)

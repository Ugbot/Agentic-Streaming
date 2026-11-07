#!/usr/bin/env python3
"""The reference runtime for the v1 spec.

It is not a production runtime and never will be: no concurrency, no durability beyond
the in-process log, no models. It exists so the fixtures are executable and so every real
runtime has a golden oracle for routing, ordering, idempotency, retry, replay, saga
compensation, timers on a logical clock and on event time, sequence CEP, the scripted
LLM stub, the transcript window, and the normalized result shape defined in
`spec/v1/result.schema.json`.

Semantics follow `spec/v1/primitives.md`. Where the existing JVM and Python cores already
agreed on a detail (the `[path] ...` reply prefix, the FNV-1a hashing embedder, tool
arguments of `{"user": user_id}` from the rule brain), the reference reproduces it.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

_TOKEN = re.compile(r"[a-z0-9]+")
_FNV_OFFSET_32 = 0x811C9DC5
_FNV_PRIME_32 = 0x01000193
_MASK_32 = 0xFFFFFFFF


class ToolError(RuntimeError):
    """A tool raised. Carries no runtime detail so results stay comparable. `unknown` says the
    attempt ended without a definite answer, so the effect may have landed."""

    def __init__(self, message: str, unknown: bool = False) -> None:
        super().__init__(message)
        self.unknown = unknown


class ValidationFailure(RuntimeError):
    """Bad arguments or an unknown tool inside a turn. Error class `validation`."""


class UnknownOutcome(Exception):
    """A non_idempotent call has no recorded result. The turn suspends for a person."""

    def __init__(self, tool: str, index: int) -> None:
        super().__init__(f"{tool} outcome unknown")
        self.tool = tool
        self.index = index


class JournalDivergence(RuntimeError):
    """A re-run asked for a different call under a recorded key. Error class `fatal`."""


class SimulatedCrash(BaseException):
    """The `crash_at` verb. A BaseException, so that nothing a turn does can catch it."""


class TransientError(RuntimeError):
    """The model failed in a way that may pass. Error class `transient`."""


class SpecError(ValueError):
    """The workflow document is not executable by this runtime."""


def fnv1a_32(token: str) -> int:
    h = _FNV_OFFSET_32
    for b in token.encode("utf-8"):
        h ^= b
        h = (h * _FNV_PRIME_32) & _MASK_32
    return h


def idempotency_key(conversation_id: str, turn_id: str, index: int) -> str:
    """primitives.md section 10: derived from recorded input alone, so a re-run regenerates it."""
    material = conversation_id.encode() + b"\x00" + turn_id.encode() + b"\x00" + str(index).encode()
    return hashlib.sha256(material).hexdigest()


def intent_of(tool_id: str, args: Dict[str, Any]) -> str:
    return tool_id + ":" + hashlib.sha256(json.dumps(args, sort_keys=True).encode()).hexdigest()


def embed(text: str, dim: int) -> List[float]:
    v = [0.0] * dim
    for tok in _TOKEN.findall((text or "").lower()):
        v[fnv1a_32(tok) % dim] += 1.0
    norm = math.sqrt(sum(x * x for x in v))
    return [x / norm for x in v] if norm else v


def cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


@dataclass
class Turn:
    conversation_id: str
    turn_id: str
    text: str = ""
    user_id: str = "anonymous"
    signal: Optional[Dict[str, Any]] = None
    metadata: Dict[str, str] = field(default_factory=dict)

    def event_time_ms(self) -> Optional[int]:
        raw = self.metadata.get("event_time_ms")
        return None if raw is None else int(raw)


@dataclass
class Conversation:
    conversation_id: str = ""
    log: List[Dict[str, Any]] = field(default_factory=list)
    results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    suspended: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    timers: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # pending, by timer id
    watermark_ms: Optional[int] = None
    tool_indices: Dict[str, int] = field(default_factory=dict)  # next call index, by turn id
    llm_indices: Dict[str, int] = field(default_factory=dict)  # next model call index, by turn id


def reduce_state(log: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The only definition of conversation state: a fold over the log.

    `context` is the workflow's `context` block: with `compaction: window` the retained
    transcript holds at most `max_items` messages and `transcript_length` counts what is
    retained, not what was ever written.
    """
    context = context or {}
    window = context.get("max_items") if context.get("compaction", "none") == "window" else None
    state: Dict[str, Any] = {"turn_count": 0, "transcript_length": 0}
    for event in log:
        kind = event["type"]
        payload = event["payload"]
        if kind == "turn_received":
            state["turn_count"] += 1
            if "event_time_ms" in payload:
                state["watermark_ms"] = max(state.get("watermark_ms", payload["event_time_ms"]),
                                            payload["event_time_ms"])
        elif kind == "memory_written":
            state["transcript_length"] += len(payload["messages"])
            if window is not None:
                state["transcript_length"] = min(state["transcript_length"], window)
        elif kind == "retrieved":
            state["last_retrieved_ids"] = list(payload["ids"])
        elif kind == "timer_fired":
            state.setdefault("fired_timers", []).append(payload["timer_id"])
    return state


class ReferenceRuntime:
    def __init__(self, workflow: Dict[str, Any]) -> None:
        version = workflow.get("spec_version", "agentic/v1")
        if version != "agentic/v1":
            raise SpecError(f"unsupported spec_version {version}")
        self.workflow = workflow
        agent = workflow["agent"]
        self.paths: Dict[str, Dict[str, Any]] = agent["paths"]
        self.router: Dict[str, Any] = agent.get("router", {})
        self.verifier: Dict[str, Any] = agent.get("verifier", {"kind": "prefix"})
        self.policies: Dict[str, Any] = workflow.get("policies", {})
        self.saga: Optional[Dict[str, Any]] = workflow.get("saga")
        self.tools: Dict[str, Dict[str, Any]] = {t["id"]: t for t in workflow.get("tools", [])}
        self.peers = {p.get("name") or p["id"]: p for p in workflow.get("a2a", [])}
        for name, peer in self.peers.items():
            self.tools[name] = {"id": name, "kind": "agent", "peer": peer}
        self.guardrails = workflow.get("guardrails", [])
        self.llm: Dict[str, Any] = workflow.get("llm") or {}
        self.context: Dict[str, Any] = workflow.get("context") or {}
        if self.context.get("compaction", "none") not in ("none", "window"):
            raise SpecError(f"the reference runtime implements context compaction none|window only, "
                            f"got {self.context['compaction']}")
        if "max_tokens" in self.context:
            raise SpecError("the reference runtime has no tokenizer; use context.max_items")
        self.timers: List[Dict[str, Any]] = workflow.get("timers", [])
        self.cep: List[Dict[str, Any]] = workflow.get("cep", [])
        for pattern in self.cep:
            if pattern.get("on_match", {}).get("kind", "tool") != "tool":
                raise SpecError("the reference runtime implements cep on_match.kind tool only")
        self.clock_ms = 0  # logical processing time, durable across restart()
        retrieval = workflow.get("retrieval") or {}
        self.kb = retrieval.get("kb", [])
        self.top_k = retrieval.get("top_k", 4)
        self.dim = (workflow.get("embeddings") or {}).get("dim", retrieval.get("dim", 256))
        self.conversations: Dict[str, Conversation] = {}
        self._tool_attempts: Dict[str, int] = {}
        self.journal: Dict[Any, Dict[str, Any]] = {}  # the effect journal: outlives every restart
        self._world: Dict[Any, Any] = {}  # what the `counting` tools and stub model have seen
        self._crash: Optional[Dict[str, Any]] = None

    # -- lifecycle ---------------------------------------------------------

    def restart(self) -> None:
        """Drop everything derived and rebuild from the log, exercising replay. The journal and
        the tools' outside world are not this runtime's memory, so they stay."""
        for conv in self.conversations.values():
            self._rebuild(conv)

    def _rebuild(self, conv: Conversation, without_turn: Optional[str] = None) -> None:
        conv.suspended = {}
        conv.timers = {}
        conv.watermark_ms = None
        for event in conv.log:
            kind, payload = event["type"], event["payload"]
            if kind == "turn_suspended":
                conv.suspended[payload["turn_id"]] = payload
            elif kind in ("turn_resumed", "turn_failed") and event["turn_id"] in conv.suspended:
                conv.suspended.pop(event["turn_id"], None)
            if event["turn_id"] == without_turn:
                continue
            if kind == "timer_scheduled":
                conv.timers[payload["timer_id"]] = dict(payload)
            elif kind == "timer_fired":
                conv.timers.pop(payload["timer_id"], None)
            elif kind == "turn_received" and "event_time_ms" in payload:
                conv.watermark_ms = max(conv.watermark_ms or payload["event_time_ms"], payload["event_time_ms"])

    def advance(self, ms: int) -> None:
        """Advance logical processing time. Due timers fire on the next turn of their conversation."""
        if ms < 0:
            raise SpecError("logical time never moves backwards")
        self.clock_ms += ms

    # -- turn handling -----------------------------------------------------

    def submit(self, turn: Turn, crash_at: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """One delivery. `crash_at` is the fixture verb: the process dies at that point, this
        delivery has no result, and the runtime recovers from what it keeps across a crash."""
        self._crash = {"seen": 0, **crash_at} if crash_at else None
        try:
            return self._deliver(turn)
        except SimulatedCrash:
            self.restart()
            return {"conversation_id": turn.conversation_id, "turn_id": turn.turn_id,
                    "status": "crashed", "events": []}
        finally:
            self._crash = None

    def _deliver(self, turn: Turn) -> Dict[str, Any]:
        """The resume state machine of primitives.md section 10, one branch per cell."""
        conv = self.conversations.setdefault(turn.conversation_id, Conversation(turn.conversation_id))
        if not turn.turn_id:
            return self._refused(conv, turn, "a turn needs a non-empty turn_id")
        idempotent = self.policies.get("idempotency", "turn-id") == "turn-id"
        state = self._turn_state(conv, turn.turn_id)
        retryable = state == "transient" and idempotent

        if turn.signal is not None:
            if state == "suspended":
                return self._resume(conv, turn)
            if state in ("absent", "running"):
                return self._refused(conv, turn, f"no suspended turn {turn.turn_id!r} to resume")
            if state == "resuming":
                return self._run(conv, turn, resuming=True)
            if retryable:
                return self._rerun_resumed(conv, turn)
            return self._duplicate(conv, turn)
        if state in ("running", "resuming"):
            return self._run(conv, turn, resuming=state == "resuming")
        if retryable:
            return self._run(conv, turn, rerun=True)
        if state != "absent" and idempotent:
            return self._duplicate(conv, turn)
        return self._run(conv, turn, fresh=not idempotent)

    @staticmethod
    def _events_of(conv: Conversation, turn_id: str) -> List[Dict[str, Any]]:
        return [e for e in conv.log if e["turn_id"] == turn_id]

    def _turn_state(self, conv: Conversation, turn_id: str) -> str:
        state = "absent"
        for event in self._events_of(conv, turn_id):
            kind = event["type"]
            if kind == "turn_suspended":
                state = "suspended"
            elif kind == "turn_resumed":
                state = "resuming"
            elif kind == "turn_completed":
                state = "terminal"
            elif kind == "turn_failed":
                state = "transient" if event["payload"].get("error_class") == "transient" else "terminal"
            elif state == "absent":
                state = "running"
        return state

    def _refused(self, conv: Conversation, turn: Turn, message: str) -> Dict[str, Any]:
        """Status `failed`, class `validation`, nothing appended: the turn_id stays unspent."""
        return {"conversation_id": turn.conversation_id, "turn_id": turn.turn_id, "status": "failed",
                "path": None, "reply": None, "state": reduce_state(conv.log, self.context),
                "tool_calls": [], "events": [], "error": {"class": "validation", "message": message}}

    def _duplicate(self, conv: Conversation, turn: Turn) -> Dict[str, Any]:
        recorded = conv.results.get(turn.turn_id) or self._result_from_log(conv, turn.turn_id)
        prior = dict(recorded)
        prior["status"] = "duplicate"
        prior["events"] = []
        prior["state"] = reduce_state(conv.log, self.context)
        return prior

    def _result_from_log(self, conv: Conversation, turn_id: str) -> Dict[str, Any]:
        """A turn that ended on the log but whose delivery died before it could answer."""
        path = reply = error = None
        calls: List[Dict[str, Any]] = []
        for event in self._events_of(conv, turn_id):
            kind, payload = event["type"], event["payload"]
            if kind == "routed":
                path = payload["path"]
            elif kind == "turn_completed":
                reply = payload["reply"]
            elif kind == "turn_failed":
                error = {"class": payload.get("error_class", "fatal"), "message": payload.get("reason", "")}
            elif kind in ("tool_called", "delegated", "compensation_step"):
                calls.append({"tool": payload["tool"], "index": payload["index"], "args": payload["args"],
                              "result": payload.get("result"), "attempt": payload["attempt"]})
            elif kind == "tool_failed":
                calls.append({"tool": payload["tool"], "index": payload["index"], "args": payload["args"],
                              "error": payload["error"], "attempt": payload["attempt"]})
        return {"conversation_id": conv.conversation_id, "turn_id": turn_id, "status": "completed", "path": path,
                "reply": reply, "state": reduce_state(conv.log, self.context), "tool_calls": calls,
                "events": [], "error": error}

    def _context(self, conv: Conversation, turn: Turn, rerun: bool, fresh: bool = False) -> "_TurnContext":
        """The turn as first received, and what its earlier run already put on the log."""
        events = self._events_of(conv, turn.turn_id)
        received = next((e["payload"] for e in events if e["type"] == "turn_received"), None)
        original = turn
        if received is not None and not fresh:
            original = Turn(turn.conversation_id, turn.turn_id, received["text"],
                            received.get("user_id", "anonymous"), None, dict(received.get("metadata") or {}))
        ctx = _TurnContext(original)
        if rerun:
            ctx._tool_index = conv.tool_indices.get(turn.turn_id, 0)
            ctx._llm_index = conv.llm_indices.get(turn.turn_id, 0)
        elif not fresh:
            ctx.prior = _Prior(events)
        return ctx

    def _resume(self, conv: Conversation, turn: Turn) -> Dict[str, Any]:
        pending = conv.suspended[turn.turn_id]
        if pending.get("until") == "unknown_outcome":
            problem = self._resolve(turn, pending)
            if problem is not None:
                return self._refused(conv, turn, problem)
        conv.suspended.pop(turn.turn_id)
        ctx = self._context(conv, turn, rerun=False)
        self._append(conv, ctx, "turn_resumed", {"turn_id": turn.turn_id, "signal": turn.signal,
                                                 "signal_sender": turn.user_id})
        return self._continue(conv, turn, ctx, resuming=True)

    def _resolve(self, turn: Turn, pending: Dict[str, Any]) -> Optional[str]:
        """A person says what happened to a call. Returns the reason a signal is refused."""
        resolution = (turn.signal or {}).get("resolution")
        key = (turn.conversation_id, turn.turn_id, "tool", pending["index"])
        entry = self.journal[key]
        if resolution == "applied":
            entry.update(state="succeeded", result=(turn.signal or {}).get("result"))
        elif resolution == "not_applied":
            entry.update(state="pending", released=True)
        elif resolution == "failed":
            error = f"{pending['tool']} resolved as failed"
            entry["attempts"].append({"attempt": len(entry["attempts"]) + 1, "error": error})
            entry.update(state="failed", error=error)
        else:
            return f"signal needs a resolution of applied, not_applied or failed, got {resolution!r}"
        return None

    def _rerun_resumed(self, conv: Conversation, turn: Turn) -> Dict[str, Any]:
        ctx = self._context(conv, turn, rerun=True)
        path = next(e["payload"]["path"] for e in self._events_of(conv, turn.turn_id) if e["type"] == "routed")
        self._append(conv, ctx, "turn_resumed", {"turn_id": turn.turn_id, "signal": turn.signal,
                                                 "signal_sender": turn.user_id})
        return self._after_routing(conv, ctx, path)

    def _run(self, conv: Conversation, turn: Turn, resuming: bool = False, rerun: bool = False,
             fresh: bool = False) -> Dict[str, Any]:
        ctx = self._context(conv, turn, rerun, fresh)
        if resuming:
            ctx.events.extend(ctx.prior.resumes)
        return self._continue(conv, turn, ctx, resuming, rerun)

    def _continue(self, conv: Conversation, delivered: Turn, ctx: "_TurnContext", resuming: bool,
                  rerun: bool = False) -> Dict[str, Any]:
        turn = ctx.turn
        event_time = turn.event_time_ms()
        if ctx.prior and not resuming:
            self._rebuild(conv, without_turn=turn.turn_id)
        if event_time is not None:
            conv.watermark_ms = max(conv.watermark_ms if conv.watermark_ms is not None else event_time, event_time)
        first_turn = all(e["turn_id"] == turn.turn_id for e in conv.log) and not rerun
        if resuming:
            ctx.skip_indices_of_timers()
        elif not first_turn:
            try:
                self._fire_due_timers(conv, ctx)
            except ToolError as exc:
                return self._fail(conv, ctx, None, "tool", str(exc))
        received: Dict[str, Any] = {"turn_id": turn.turn_id, "text": turn.text, "user_id": turn.user_id}
        if event_time is not None:
            received["event_time_ms"] = event_time
        if turn.metadata:
            received["metadata"] = dict(turn.metadata)
        if not rerun:
            self._append(conv, ctx, "turn_received", received)
        if first_turn:
            self._schedule_timers(conv, ctx)

        blocked = self._check_guardrails(self._global_rails(), "input", turn.text)
        if blocked is not None:
            self._append(conv, ctx, "guardrail_rejected", {"reason": blocked})
            return self._finish(conv, ctx, "rejected", None, None, {"class": "guardrail", "message": blocked})

        path = self._route(turn.text)
        self._append(conv, ctx, "routed", {"path": path})
        try:
            self._match_patterns(conv, ctx)
        except ToolError as exc:
            return self._fail(conv, ctx, path, "tool", str(exc))
        blocked = self._check_guardrails(self._path_rails(path), "input", turn.text)
        if blocked is not None:
            self._append(conv, ctx, "guardrail_rejected", {"reason": blocked, "path": path})
            return self._finish(conv, ctx, "rejected", path, None, {"class": "guardrail", "message": blocked})

        if "x-suspend-until" in self.paths[path] and not resuming:
            payload = {"turn_id": turn.turn_id, "path": path, "text": turn.text, "user_id": turn.user_id,
                       "until": self.paths[path]["x-suspend-until"]}
            self._append(conv, ctx, "turn_suspended", payload)
            conv.suspended[turn.turn_id] = payload
            return self._finish(conv, ctx, "suspended", path, None, None)
        return self._after_routing(conv, ctx, path)

    def _after_routing(self, conv: Conversation, ctx: "_TurnContext", path: str) -> Dict[str, Any]:
        try:
            if self.saga is not None:
                return self._run_saga(conv, ctx, path)
            return self._run_brain(conv, ctx, path)
        except UnknownOutcome as exc:
            spec = self.tools[exc.tool]
            payload = {"turn_id": ctx.turn.turn_id, "path": path, "text": ctx.turn.text,
                       "user_id": ctx.turn.user_id, "until": "unknown_outcome",
                       "tool": spec["id"], "index": exc.index}
            self._append(conv, ctx, "turn_suspended", payload)
            conv.suspended[ctx.turn.turn_id] = payload
            return self._finish(conv, ctx, "suspended", path, None, None)
        except (ValidationFailure, JournalDivergence) as exc:
            cls = "validation" if isinstance(exc, ValidationFailure) else "fatal"
            return self._fail(conv, ctx, path, cls, str(exc))

    def _fail(self, conv: Conversation, ctx: "_TurnContext", path: Optional[str], cls: str,
              message: str) -> Dict[str, Any]:
        return self._finish(conv, ctx, "failed", path, None, {"class": cls, "message": message},
                            event=("turn_failed", {"reason": message, "error_class": cls}))

    def _run_brain(self, conv: Conversation, ctx: "_TurnContext", path: str) -> Dict[str, Any]:
        attempts = self.policies.get("verification", {}).get("max_attempts", 1)
        self._append(conv, ctx, "brain_started", {"path": path})
        reply = None
        for _ in range(attempts):
            try:
                reply = self._draft(conv, ctx, path)
            except ToolError as exc:
                return self._fail(conv, ctx, path, "tool", str(exc))
            except TransientError as exc:
                return self._fail(conv, ctx, path, "transient", str(exc))
            self._append(conv, ctx, "reply_drafted", {"reply": reply})
            if self._verify(path, reply):
                blocked = self._check_guardrails(self._global_rails() + self._path_rails(path), "output", reply)
                if blocked is not None:
                    self._append(conv, ctx, "guardrail_rejected", {"reason": blocked, "stage": "output"})
                    return self._finish(conv, ctx, "rejected", path, reply, {"class": "guardrail", "message": blocked})
                return self._complete(conv, ctx, path, reply)
            self._append(conv, ctx, "verification_failed", {"reply": reply})
        on_exhausted = self.policies.get("verification", {}).get("on_exhausted", "unverified")
        status = "unverified" if on_exhausted == "unverified" else "failed"
        return self._finish(conv, ctx, status, path, reply,
                            {"class": "verification", "message": "verifier rejected the reply"})

    def _draft(self, conv: Conversation, ctx: "_TurnContext", path: str) -> str:
        spec = self.paths[path]
        brain = spec.get("brain", "rule")
        if brain == "llm":
            return self._draft_scripted(conv, ctx)
        if brain != "rule":
            raise SpecError(f"the reference runtime does not implement the {brain} brain")
        low = ctx.turn.text.lower()
        for keyword, tool_id in (spec.get("tool_triggers") or {}).items():
            if keyword.lower() in low:
                result = self._invoke(conv, ctx, tool_id, {"user": ctx.turn.user_id})
                return f"[{path}] {tool_id} returned {result}"
        if self.kb:
            query = embed(ctx.turn.text, self.dim)
            scored = sorted(
                ((cosine(query, embed(doc["text"], self.dim)), doc) for doc in self.kb),
                key=lambda pair: (-pair[0], pair[1]["id"]),
            )[: self.top_k]
            self._append(conv, ctx, "retrieved", {"ids": [doc["id"] for _, doc in scored]})
            if scored and scored[0][0] > spec.get("threshold", 0.15):
                return f"[{path}] {scored[0][1]['text']}"
        return f'[{path}] I can help with {path} questions. You said: "{ctx.turn.text}"'

    def _draft_scripted(self, conv: Conversation, ctx: "_TurnContext") -> str:
        """The `stub` LLM provider: replay `llm.script` from the top on every turn."""
        if self.llm.get("provider", "stub") != "stub":
            raise SpecError(f"the reference runtime implements the stub LLM provider only, got {self.llm['provider']}")
        script = self.llm.get("script")
        if not script:
            raise SpecError("an llm brain needs llm.script under the stub provider")
        for index, step in enumerate(script):
            answer = self._ask_model(conv, ctx, index, step)
            if "tool" in answer:
                self._invoke(conv, ctx, answer["tool"], dict(answer.get("args", {})))
            else:
                return answer["text"]
        raise SpecError("llm.script ended without a final text")

    def _ask_model(self, conv: Conversation, ctx: "_TurnContext", step_index: int, step: Dict[str, Any]) -> Dict[str, Any]:
        """One model call, journaled by its index in the turn: a recorded reply is reused."""
        index = ctx.next_llm_index()
        key = (ctx.turn.conversation_id, ctx.turn.turn_id, "llm", index)
        entry = self.journal.get(key)
        if entry is None:
            self._call_model(step_index, step)
            answer = {k: step[k] for k in ("tool", "args", "text") if k in step}
            if step.get("x-count-calls"):
                seen = self._world.get(("llm", step_index), 0) + 1
                self._world[("llm", step_index)] = seen
                answer["text"] = f"{answer['text']} #{seen}"
            entry = self.journal[key] = {"state": "succeeded", "result": answer}
        self._append(conv, ctx, "llm_called", {"index": index, **entry["result"]})
        return entry["result"]

    def _call_model(self, index: int, step: Dict[str, Any]) -> None:
        """One scripted model call under `policies.retry`. Only the call is retried, so a tool
        the model already called is not called again."""
        retry = self.policies.get("retry", {})
        max_attempts = retry.get("max_attempts", 1) if retry.get("kind", "none") != "none" else 1
        for attempt in range(1, max_attempts + 1):
            seen = self._tool_attempts.get(f"llm.script[{index}]", 0) + 1
            self._tool_attempts[f"llm.script[{index}]"] = seen
            if seen > step.get("x-fail-attempts", 0):
                return
        raise TransientError("stub model unavailable")

    # -- time and patterns -------------------------------------------------

    def _clock(self, conv: Conversation, clock: str) -> int:
        if clock == "event":
            return conv.watermark_ms if conv.watermark_ms is not None else 0
        if clock == "processing":
            return self.clock_ms
        raise SpecError(f"unknown timer clock {clock}")

    def _schedule_timers(self, conv: Conversation, ctx: "_TurnContext") -> None:
        for timer in self.timers:
            clock = timer.get("clock", "processing")
            now = self._clock(conv, clock)
            payload = {"timer_id": timer["id"], "clock": clock, "due_ms": now + timer["after_ms"]}
            self._append(conv, ctx, "timer_scheduled", payload)
            conv.timers[timer["id"]] = payload

    def _fire_due_timers(self, conv: Conversation, ctx: "_TurnContext") -> None:
        due = sorted((t for t in conv.timers.values() if self._clock(conv, t["clock"]) >= t["due_ms"]),
                     key=lambda t: (t["due_ms"], t["timer_id"]))
        for pending in due:
            conv.timers.pop(pending["timer_id"])
            self._append(conv, ctx, "timer_fired", {"timer_id": pending["timer_id"], "due_ms": pending["due_ms"]})
            spec = next(t for t in self.timers if t["id"] == pending["timer_id"])
            if spec.get("tool"):
                self._invoke(conv, ctx, spec["tool"], dict(spec.get("payload") or {}))

    def _match_patterns(self, conv: Conversation, ctx: "_TurnContext") -> None:
        """Sequence CEP over this conversation's turns; a match completing on this turn fires on_match."""
        turns = [e["payload"] for e in conv.log if e["type"] == "turn_received"]
        for pattern in self.cep:
            if self._matches_on_last_turn(pattern, turns):
                on_match = pattern.get("on_match") or {}
                if "tool" not in on_match:
                    raise SpecError(f"cep pattern {pattern['name']} has on_match.kind tool without a tool id")
                self._invoke(conv, ctx, on_match["tool"],
                             {"pattern": pattern["name"], "key": ctx.turn.conversation_id})

    @staticmethod
    def _matches_on_last_turn(pattern: Dict[str, Any], turns: List[Dict[str, Any]]) -> bool:
        stages = pattern["pattern"]
        ts_key = pattern.get("ts")
        within = pattern.get("within")
        if within is not None and not ts_key:
            raise SpecError(f"cep pattern {pattern['name']} sets within without ts")

        def timestamp(turn: Dict[str, Any]) -> int:
            scope, _, key = ts_key.partition(".")
            if scope != "metadata":
                raise SpecError(f"cep ts must be metadata.<key>, got {ts_key}")
            raw = (turn.get("metadata") or {}).get(key)
            if raw is None:
                raise SpecError(f"turn {turn['turn_id']} lacks metadata.{key} needed by cep pattern {pattern['name']}")
            return int(raw)

        def where(stage: Dict[str, Any], turn: Dict[str, Any]) -> bool:
            cond = stage.get("where") or {}
            needle = cond.get("text_contains")
            return needle is None or needle.lower() in (turn.get("text") or "").lower()

        matched_at = -1
        stage_index = 0
        start_ts = 0
        for i, turn in enumerate(turns):
            if stage_index > 0 and within is not None and timestamp(turn) - start_ts > within:
                stage_index = 0
            stage = stages[stage_index]
            if where(stage, turn):
                if stage_index == 0 and ts_key:
                    start_ts = timestamp(turn)
                stage_index += 1
                if stage_index == len(stages):
                    matched_at = i
                    stage_index = 0
            elif stage_index > 0 and stage.get("contiguity", "next") == "next":
                stage_index = 0
        return matched_at == len(turns) - 1

    def _run_saga(self, conv: Conversation, ctx: "_TurnContext", path: str) -> Dict[str, Any]:
        done: List[Any] = []
        for step in self.saga["steps"]:
            args = step.get("args", {})
            try:
                result = self._invoke(conv, ctx, step["tool"], args)
            except (ToolError, ValidationFailure) as exc:
                cls = "tool" if isinstance(exc, ToolError) else "validation"
                self._append(conv, ctx, "compensation_started", {"failed_step": step.get("name", step["tool"])})
                failed_undos = self._compensate(conv, ctx, done)
                if failed_undos:
                    return self._fail(conv, ctx, path, "fatal", f"compensation failed for {failed_undos} after: {exc}")
                self._append(conv, ctx, "compensation_completed", {"steps": len(done)})
                return self._fail(conv, ctx, path, cls, str(exc))
            done.append((step, args, result))
        return self._complete(conv, ctx, path, f"[{path}] saga completed")

    def _compensate(self, conv: Conversation, ctx: "_TurnContext", done: List[Any]) -> List[str]:
        """Every undo in reverse order, each given what its step did. One that fails is recorded
        and passed over, so the rest still run."""
        failed: List[str] = []
        for completed, args, result in reversed(done):
            undo = completed.get("compensate_with") or self.tools[completed["tool"]].get("compensation")
            if not undo:
                continue
            forward = {"forward_args": args, "forward_result": result}
            try:
                self._invoke(conv, ctx, undo, forward, event="compensation_step", strict=True)
            except (ToolError, ValidationFailure) as exc:
                failed.append(undo)
                last = ctx.tool_calls[-1] if ctx.tool_calls else {"index": None}
                self._append(conv, ctx, "compensation_failed",
                             {"tool": undo, "index": last["index"], "step": completed.get("name", completed["tool"]),
                              "error": str(exc)})
        return failed

    # -- pieces ------------------------------------------------------------

    def _route(self, text: str) -> str:
        low = (text or "").lower()
        if self.router.get("kind", "keyword") == "keyword":
            for path, keywords in (self.router.get("rules") or {}).items():
                if any(k.lower() in low for k in keywords):
                    return path
        default = self.router.get("default")
        if default is None:
            raise SpecError("no rule matched and router.default is not set")
        return default

    def _global_rails(self) -> List[Dict[str, Any]]:
        scoped = {name for path in self.paths.values() for name in path.get("guardrails") or []}
        return [g for g in self.guardrails if g.get("name") not in scoped]

    def _path_rails(self, path: str) -> List[Dict[str, Any]]:
        names = self.paths[path].get("guardrails") or []
        return [g for name in names for g in self.guardrails if g.get("name") == name]

    def _check_guardrails(self, rails: List[Dict[str, Any]], stage: str, text: str) -> Optional[str]:
        for rail in rails:
            applies = rail.get("stage", "input")
            if applies != stage and applies != "both":
                continue
            if rail["kind"] != "regex":
                raise SpecError(f"the reference runtime implements regex guardrails only, got {rail['kind']}")
            for pattern in rail.get("allow", []):
                if not re.search(pattern, text or "", re.IGNORECASE):
                    return rail.get("reason", "denied")
            for pattern in rail.get("deny", []):
                if re.search(pattern, text or "", re.IGNORECASE):
                    return rail.get("reason", "denied")
        return None

    def _verifier_for(self, path: str) -> Dict[str, Any]:
        """primitives.md section 5: the path's verifier, else `agent.verifier`, else `prefix`."""
        path_verifier = self.paths[path].get("verifier")
        return path_verifier if path_verifier is not None else self.verifier

    def _verify(self, path: str, reply: str) -> bool:
        verifier = self._verifier_for(path)
        kind = verifier.get("kind", "prefix")
        if kind == "none":
            return True
        if kind == "prefix":
            return reply.startswith("[")
        if kind == "regex":
            return re.search(verifier["pattern"], reply) is not None
        raise SpecError(f"the reference runtime does not implement the {kind} verifier")

    def _invoke(self, conv: Conversation, ctx: "_TurnContext", tool_id: str,
                args: Dict[str, Any], event: str = None, strict: bool = False) -> Any:
        """`record_or_get` for one tool call (primitives.md section 10). `strict` raises when the
        budget is spent whatever `on_tool_error` says: a saga undo that failed must never read as
        compensated."""
        spec = self.tools.get(tool_id)
        if spec is None:
            raise ValidationFailure(f"unknown tool {tool_id}")
        self._validate_args(tool_id, spec, args)
        turn = ctx.turn
        index = ctx.next_tool_index()
        key = (turn.conversation_id, turn.turn_id, "tool", index)
        effect = spec.get("effect", "idempotent")
        ikey = idempotency_key(turn.conversation_id, turn.turn_id, index)
        kind = event or ("delegated" if spec.get("kind") == "agent" else "tool_called")
        intent = intent_of(tool_id, args)
        entry = self.journal.get(key)
        fresh = entry is None
        if fresh:
            entry = self.journal[key] = {"intent": intent, "effect": effect, "state": "pending",
                                         "attempts": [], "result": None, "error": None}
        elif entry["intent"] != intent:
            raise JournalDivergence(f"call {index} of turn {turn.turn_id} was {entry['intent']}, now {intent}")
        self._append(conv, ctx, "tool_intent", {"tool": tool_id, "index": index, "args": args,
                                                "effect": effect, "idempotency_key": ikey})
        for attempt in entry["attempts"]:
            self._record_failure(conv, ctx, tool_id, index, args, attempt["attempt"], attempt["error"])

        if entry["state"] == "pending" and effect == "non_idempotent" and not fresh and not entry.pop("released", False):
            entry["state"] = "unknown"
        if entry["state"] == "unknown":
            self._append(conv, ctx, "unknown_outcome", {"tool": tool_id, "index": index, "args": args,
                                                        "idempotency_key": ikey, "reason": "no recorded result"})
            raise UnknownOutcome(tool_id, index)
        if entry["state"] == "succeeded":
            return self._record_success(conv, ctx, kind, tool_id, index, args, len(entry["attempts"]) + 1, entry["result"])
        if entry["state"] == "failed":
            return self._give_up(entry["error"], strict)

        retry = self.policies.get("retry", {})
        max_attempts = retry.get("max_attempts", 1) if retry.get("kind", "none") != "none" else 1
        if effect == "non_idempotent":
            max_attempts = 1
        for attempt in range(len(entry["attempts"]) + 1, max_attempts + 1):
            self._hit("call", tool_id, "before")
            try:
                result = self._execute(tool_id, spec, turn.conversation_id, ikey)
            except ToolError as exc:
                if exc.unknown and effect == "non_idempotent":
                    entry["state"] = "unknown"
                    self._append(conv, ctx, "unknown_outcome", {"tool": tool_id, "index": index, "args": args,
                                                                "idempotency_key": ikey, "reason": str(exc)})
                    raise UnknownOutcome(tool_id, index) from exc
                entry["attempts"].append({"attempt": attempt, "error": str(exc)})
                self._record_failure(conv, ctx, tool_id, index, args, attempt, str(exc))
                if attempt >= max_attempts:
                    entry.update(state="failed", error=str(exc))
                    return self._give_up(str(exc), strict)
                continue
            self._hit("call", tool_id, "during")
            entry.update(state="succeeded", result=result)
            self._hit("call", tool_id, "after")
            return self._record_success(conv, ctx, kind, tool_id, index, args, attempt, result)
        entry.update(state="failed", error=entry["attempts"][-1]["error"] if entry["attempts"] else "budget spent")
        return self._give_up(entry["error"], strict)

    def _record_failure(self, conv: Conversation, ctx: "_TurnContext", tool_id: str, index: int,
                        args: Dict[str, Any], attempt: int, error: str) -> None:
        ctx.tool_calls.append({"tool": tool_id, "index": index, "args": args, "error": error, "attempt": attempt})
        self._append(conv, ctx, "tool_failed",
                     {"tool": tool_id, "index": index, "attempt": attempt, "args": args, "error": error})

    def _record_success(self, conv: Conversation, ctx: "_TurnContext", kind: str, tool_id: str, index: int,
                        args: Dict[str, Any], attempt: int, result: Any) -> Any:
        ctx.tool_calls.append({"tool": tool_id, "index": index, "args": args, "result": result, "attempt": attempt})
        self._append(conv, ctx, kind, {"tool": tool_id, "index": index, "attempt": attempt, "args": args, "result": result})
        return result

    def _give_up(self, error: str, strict: bool) -> None:
        if self.policies.get("on_tool_error", "fail") == "continue" and not strict:
            return None
        raise ToolError(error)

    @staticmethod
    def _validate_args(tool_id: str, spec: Dict[str, Any], args: Dict[str, Any]) -> None:
        schema = spec.get("parameters")
        if schema is None:
            return
        from jsonschema import Draft202012Validator
        errors = sorted(Draft202012Validator(schema).iter_errors(args), key=lambda e: list(e.absolute_path))
        if errors:
            raise ValidationFailure(f"tool {tool_id} arguments rejected: {errors[0].message}")

    def _execute(self, tool_id: str, spec: Dict[str, Any], conversation_id: str, ikey: str) -> Any:
        kind = spec.get("kind", "constant")
        if kind == "constant":
            return spec.get("value")
        if kind == "agent":
            return f"[{tool_id}] delegated"
        if kind == "failing":
            budget = spec.get("x-fail-attempts")
            seen = self._tool_attempts.get(tool_id, 0) + 1
            self._tool_attempts[tool_id] = seen
            if budget is None or seen <= budget:
                raise ToolError(f"{tool_id} failed")
            return spec.get("value")
        if kind == "hanging":
            # never answers; the reference has no wall clock, so the limit is reached at once
            raise ToolError(f"{tool_id} timed out after {spec.get('timeout_ms', 10000)} ms", unknown=True)
        if kind == "counting":
            return self._count(tool_id, spec, conversation_id, ikey)
        if kind == "http":
            return self._http(tool_id, spec, ikey)
        raise SpecError(f"the reference runtime does not implement tool kind {kind}")

    def _count(self, tool_id: str, spec: Dict[str, Any], conversation_id: str, ikey: str) -> int:
        """The outside world, in miniature: how many calls landed, or how many distinct keys.
        It answers with that number, so a repeated effect shows in the reply."""
        seen = self._world.setdefault((tool_id, conversation_id), [])
        if spec.get("x-count-by") == "keys":
            if ikey not in seen:
                seen.append(ikey)
        else:
            seen.append(ikey)
        return len(seen)

    def _http(self, tool_id: str, spec: Dict[str, Any], ikey: str) -> Any:
        request = urllib.request.Request(spec["url"], data=b"{}", method="POST", headers={
            "Content-Type": "application/json", "Idempotency-Key": ikey})
        try:
            with urllib.request.urlopen(request, timeout=spec.get("timeout_ms", 10000) / 1000.0) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise ToolError(f"{tool_id} failed: HTTP {exc.code}", unknown=exc.code >= 500) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise ToolError(f"{tool_id} failed: {exc}", unknown=True) from exc
        try:
            return json.loads(raw)
        except ValueError:
            return raw

    # -- bookkeeping -------------------------------------------------------

    def _hit(self, axis: str, name: str, when: str) -> None:
        """The `crash_at` verb: die here if this is the armed point."""
        plan = self._crash
        if plan is None or plan.get(axis) != name or plan["when"] != when:
            return
        plan["seen"] += 1
        if plan["seen"] == plan.get("occurrence", 1):
            raise SimulatedCrash()

    def _append(self, conv: Conversation, ctx: "_TurnContext", kind: str, payload: Dict[str, Any]) -> None:
        """At-most-once per (turn_id, type, index, attempt): what the log kept is reused."""
        kept = ctx.prior.take(kind, payload) if ctx.prior else None
        if kept is not None:
            ctx.events.append(kept)
            return
        self._hit("event", kind, "before")
        event = {"type": kind, "sequence": len(conv.log), "payload": payload, "turn_id": ctx.turn.turn_id,
                 "timestamp": self.clock_ms, "metadata": dict(ctx.turn.metadata)}
        conv.log.append(event)
        ctx.events.append(event)
        self._hit("event", kind, "after")

    def _complete(self, conv: Conversation, ctx: "_TurnContext", path: str, reply: str) -> Dict[str, Any]:
        self._append(conv, ctx, "memory_written",
                     {"messages": [{"role": "user", "text": ctx.turn.text},
                                   {"role": "assistant", "text": reply}]})
        return self._finish(conv, ctx, "completed", path, reply, None,
                            event=("turn_completed", {"reply": reply}))

    def _finish(self, conv: Conversation, ctx: "_TurnContext", status: str, path: Optional[str],
                reply: Optional[str], error: Optional[Dict[str, Any]], event=None) -> Dict[str, Any]:
        if event is not None:
            self._append(conv, ctx, event[0], event[1])
        elif status in ("rejected", "unverified"):
            self._append(conv, ctx, "turn_failed", {"status": status, "error_class": (error or {}).get("class")})
        result = {
            "conversation_id": ctx.turn.conversation_id,
            "turn_id": ctx.turn.turn_id,
            "status": status,
            "path": path,
            "reply": reply,
            "state": reduce_state(conv.log, self.context),
            "tool_calls": ctx.tool_calls,
            "events": [{k: e[k] for k in ("type", "sequence", "payload", "timestamp", "metadata")}
                       for e in ctx.segment()],
            "error": error,
        }
        conv.results[ctx.turn.turn_id] = result
        conv.tool_indices[ctx.turn.turn_id] = ctx._tool_index
        conv.llm_indices[ctx.turn.turn_id] = ctx._llm_index
        return result


class _Prior:
    """What an earlier run of this turn left on the log, by (type, index, attempt)."""

    def __init__(self, events: List[Dict[str, Any]]) -> None:
        self.events = events
        self._kept: Dict[Any, deque] = {}
        self.resumes = [e for e in events if e["type"] == "turn_resumed"]
        for event in events:
            if event["type"] != "turn_resumed":  # each resume is its own: a second is not a repeat
                self._kept.setdefault(self._key(event["type"], event["payload"]), deque()).append(event)

    @staticmethod
    def _key(kind: str, payload: Dict[str, Any]) -> Any:
        return kind, payload.get("index"), payload.get("attempt")

    def __bool__(self) -> bool:
        return bool(self.events)

    def take(self, kind: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        kept = self._kept.get(self._key(kind, payload))
        return kept.popleft() if kept else None


@dataclass
class _TurnContext:
    turn: Turn
    events: List[Dict[str, Any]] = field(default_factory=list)
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    prior: Optional[_Prior] = None
    _tool_index: int = 0
    _llm_index: int = 0

    def next_tool_index(self) -> int:
        index = self._tool_index
        self._tool_index += 1
        return index

    def next_llm_index(self) -> int:
        index = self._llm_index
        self._llm_index += 1
        return index

    def skip_indices_of_timers(self) -> None:
        """A resume does not fire timers again, so the calls they made keep their indices."""
        for event in self.prior.events if self.prior else []:
            if event["type"] == "turn_received":
                return
            if "index" in event["payload"]:
                self._tool_index = max(self._tool_index, event["payload"]["index"] + 1)

    def segment(self) -> List[Dict[str, Any]]:
        """The result's events: the turn's, from its last `turn_resumed`."""
        ordered = sorted(self.events, key=lambda e: e["sequence"])
        starts = [i for i, e in enumerate(ordered) if e["type"] == "turn_resumed"]
        return ordered[starts[-1]:] if starts else ordered

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

import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

_TOKEN = re.compile(r"[a-z0-9]+")
_FNV_OFFSET_32 = 0x811C9DC5
_FNV_PRIME_32 = 0x01000193
_MASK_32 = 0xFFFFFFFF


class ToolError(RuntimeError):
    """A tool raised. Carries no runtime detail so results stay comparable."""


class SpecError(ValueError):
    """The workflow document is not executable by this runtime."""


def fnv1a_32(token: str) -> int:
    h = _FNV_OFFSET_32
    for b in token.encode("utf-8"):
        h ^= b
        h = (h * _FNV_PRIME_32) & _MASK_32
    return h


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
    log: List[Dict[str, Any]] = field(default_factory=list)
    results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    suspended: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    timers: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # pending, by timer id
    watermark_ms: Optional[int] = None


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

    # -- lifecycle ---------------------------------------------------------

    def restart(self) -> None:
        """Drop everything derived and rebuild from the log, exercising replay."""
        for conv in self.conversations.values():
            conv.suspended = {}
            conv.timers = {}
            conv.watermark_ms = None
            for event in conv.log:
                kind, payload = event["type"], event["payload"]
                if kind == "turn_suspended":
                    conv.suspended[payload["turn_id"]] = payload
                elif kind == "turn_resumed":
                    conv.suspended.pop(payload["turn_id"], None)
                elif kind == "timer_scheduled":
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

    def submit(self, turn: Turn) -> Dict[str, Any]:
        conv = self.conversations.setdefault(turn.conversation_id, Conversation())
        idempotent = self.policies.get("idempotency", "turn-id") == "turn-id"

        if turn.signal is not None and turn.turn_id in conv.suspended:
            return self._resume(conv, turn)
        if idempotent and turn.turn_id in conv.results:
            prior = dict(conv.results[turn.turn_id])
            prior["status"] = "duplicate"
            prior["events"] = []
            return prior

        ctx = _TurnContext(turn)
        event_time = turn.event_time_ms()
        if event_time is not None:
            conv.watermark_ms = max(conv.watermark_ms if conv.watermark_ms is not None else event_time, event_time)
        first_turn = not conv.log
        if not first_turn:
            self._fire_due_timers(conv, ctx)
        received: Dict[str, Any] = {"turn_id": turn.turn_id, "text": turn.text}
        if event_time is not None:
            received["event_time_ms"] = event_time
        if turn.metadata:
            received["metadata"] = dict(turn.metadata)
        self._append(conv, ctx, "turn_received", received)
        if first_turn:
            self._schedule_timers(conv, ctx)

        blocked = self._check_guardrails(turn.text)
        if blocked is not None:
            self._append(conv, ctx, "guardrail_rejected", {"reason": blocked})
            return self._finish(conv, ctx, "rejected", None, None, {"class": "guardrail", "message": blocked})

        path = self._route(turn.text)
        self._append(conv, ctx, "routed", {"path": path})
        self._match_patterns(conv, ctx)

        if "x-suspend-until" in self.paths[path]:
            self._append(conv, ctx, "turn_suspended",
                         {"turn_id": turn.turn_id, "path": path, "text": turn.text,
                          "until": self.paths[path]["x-suspend-until"]})
            conv.suspended[turn.turn_id] = {"turn_id": turn.turn_id, "path": path, "text": turn.text}
            return self._finish(conv, ctx, "suspended", path, None, None)

        if self.saga is not None:
            return self._run_saga(conv, ctx, path)
        return self._run_brain(conv, ctx, path)

    def _resume(self, conv: Conversation, turn: Turn) -> Dict[str, Any]:
        pending = conv.suspended.pop(turn.turn_id)
        ctx = _TurnContext(Turn(turn.conversation_id, turn.turn_id, pending["text"]))
        self._append(conv, ctx, "turn_resumed", {"turn_id": turn.turn_id, "signal": turn.signal})
        return self._run_brain(conv, ctx, pending["path"])

    def _run_brain(self, conv: Conversation, ctx: "_TurnContext", path: str) -> Dict[str, Any]:
        attempts = self.policies.get("verification", {}).get("max_attempts", 1)
        self._append(conv, ctx, "brain_started", {"path": path})
        reply = None
        for _ in range(attempts):
            try:
                reply = self._draft(conv, ctx, path)
            except ToolError as exc:
                return self._finish(conv, ctx, "failed", path, None, {"class": "tool", "message": str(exc)},
                                    event=("turn_failed", {"reason": str(exc)}))
            self._append(conv, ctx, "reply_drafted", {"reply": reply})
            if self._verify(path, reply):
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
        for step in script:
            if "tool" in step:
                self._invoke(conv, ctx, step["tool"], dict(step.get("args", {})))
            else:
                return step["text"]
        raise SpecError("llm.script ended without a final text")

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
        done: List[Dict[str, Any]] = []
        for step in self.saga["steps"]:
            try:
                self._invoke(conv, ctx, step["tool"], step.get("args", {}))
            except ToolError as exc:
                self._append(conv, ctx, "compensation_started", {"failed_step": step.get("name", step["tool"])})
                for completed in reversed(done):
                    undo = completed.get("compensate_with") or self.tools[completed["tool"]].get("compensation")
                    if undo:
                        self._invoke(conv, ctx, undo, {}, event="compensation_step")
                self._append(conv, ctx, "compensation_completed", {"steps": len(done)})
                return self._finish(conv, ctx, "failed", path, None, {"class": "tool", "message": str(exc)},
                                    event=("turn_failed", {"reason": str(exc)}))
            done.append(step)
        return self._complete(conv, ctx, path, f"[{path}] saga completed")

    # -- pieces ------------------------------------------------------------

    def _route(self, text: str) -> str:
        low = (text or "").lower()
        for path, keywords in (self.router.get("rules") or {}).items():
            if any(k.lower() in low for k in keywords):
                return path
        default = self.router.get("default")
        if default is None:
            raise SpecError("no rule matched and router.default is not set")
        return default

    def _check_guardrails(self, text: str) -> Optional[str]:
        for rail in self.guardrails:
            if rail.get("stage", "input") == "output":
                continue
            if rail["kind"] != "regex":
                raise SpecError(f"the reference runtime implements regex guardrails only, got {rail['kind']}")
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
                args: Dict[str, Any], event: str = None) -> Any:
        spec = self.tools.get(tool_id)
        if spec is None:
            raise SpecError(f"unknown tool {tool_id}")
        index = ctx.next_tool_index()
        retry = self.policies.get("retry", {})
        max_attempts = retry.get("max_attempts", 1) if retry.get("kind", "none") != "none" else 1
        for attempt in range(1, max_attempts + 1):
            try:
                result = self._execute(tool_id, spec)
            except ToolError as exc:
                ctx.tool_calls.append({"tool": tool_id, "index": index, "args": args,
                                       "error": str(exc), "attempt": attempt})
                self._append(conv, ctx, "tool_failed",
                             {"tool": tool_id, "index": index, "attempt": attempt, "error": str(exc)})
                if attempt == max_attempts:
                    if self.policies.get("on_tool_error", "fail") == "continue":
                        return None
                    raise
                continue
            ctx.tool_calls.append({"tool": tool_id, "index": index, "args": args,
                                   "result": result, "attempt": attempt})
            kind = event or ("delegated" if spec.get("kind") == "agent" else "tool_called")
            self._append(conv, ctx, kind,
                         {"tool": tool_id, "index": index, "attempt": attempt, "args": args})
            return result
        raise AssertionError("unreachable")

    def _execute(self, tool_id: str, spec: Dict[str, Any]) -> Any:
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
        raise SpecError(f"the reference runtime does not implement tool kind {kind}")

    # -- bookkeeping -------------------------------------------------------

    def _append(self, conv: Conversation, ctx: "_TurnContext", kind: str, payload: Dict[str, Any]) -> None:
        event = {"type": kind, "sequence": len(conv.log), "payload": payload}
        conv.log.append(event)
        ctx.events.append(event)

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
            self._append(conv, ctx, "turn_failed", {"status": status})
        result = {
            "conversation_id": ctx.turn.conversation_id,
            "turn_id": ctx.turn.turn_id,
            "status": status,
            "path": path,
            "reply": reply,
            "state": reduce_state(conv.log, self.context),
            "tool_calls": ctx.tool_calls,
            "events": [{"type": e["type"], "sequence": e["sequence"], "payload": e["payload"]}
                       for e in ctx.events],
            "error": error,
        }
        conv.results[ctx.turn.turn_id] = result
        return result


@dataclass
class _TurnContext:
    turn: Turn
    events: List[Dict[str, Any]] = field(default_factory=list)
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    _tool_index: int = 0

    def next_tool_index(self) -> int:
        index = self._tool_index
        self._tool_index += 1
        return index

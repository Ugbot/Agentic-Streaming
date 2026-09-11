#!/usr/bin/env python3
"""The reference runtime for the v1 spec.

It is not a production runtime and never will be: no concurrency, no durability beyond
the in-process log, no models. It exists so the fixtures are executable and so every real
runtime has a golden oracle for routing, ordering, idempotency, retry, replay, saga
compensation, and the normalized result shape defined in `spec/v1/result.schema.json`.

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


@dataclass
class Conversation:
    log: List[Dict[str, Any]] = field(default_factory=list)
    results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    suspended: Dict[str, Dict[str, Any]] = field(default_factory=dict)


def reduce_state(log: List[Dict[str, Any]]) -> Dict[str, Any]:
    """The only definition of conversation state: a fold over the log."""
    state: Dict[str, Any] = {"turn_count": 0, "transcript_length": 0}
    for event in log:
        kind = event["type"]
        if kind == "turn_received":
            state["turn_count"] += 1
        elif kind == "memory_written":
            state["transcript_length"] += len(event["payload"]["messages"])
        elif kind == "retrieved":
            state["last_retrieved_ids"] = list(event["payload"]["ids"])
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
            for event in conv.log:
                if event["type"] == "turn_suspended":
                    conv.suspended[event["payload"]["turn_id"]] = event["payload"]
                elif event["type"] == "turn_resumed":
                    conv.suspended.pop(event["payload"]["turn_id"], None)

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
        self._append(conv, ctx, "turn_received", {"turn_id": turn.turn_id, "text": turn.text})

        blocked = self._check_guardrails(turn.text)
        if blocked is not None:
            self._append(conv, ctx, "guardrail_rejected", {"reason": blocked})
            return self._finish(conv, ctx, "rejected", None, None, {"class": "guardrail", "message": blocked})

        path = self._route(turn.text)
        self._append(conv, ctx, "routed", {"path": path})

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
            if self._verify(reply):
                return self._complete(conv, ctx, path, reply)
            self._append(conv, ctx, "verification_failed", {"reply": reply})
        on_exhausted = self.policies.get("verification", {}).get("on_exhausted", "unverified")
        status = "unverified" if on_exhausted == "unverified" else "failed"
        return self._finish(conv, ctx, status, path, reply,
                            {"class": "verification", "message": "verifier rejected the reply"})

    def _draft(self, conv: Conversation, ctx: "_TurnContext", path: str) -> str:
        spec = self.paths[path]
        if spec.get("brain", "rule") != "rule":
            raise SpecError("the reference runtime implements the rule brain only")
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

    def _verify(self, reply: str) -> bool:
        kind = self.verifier.get("kind", "prefix")
        if kind == "none":
            return True
        if kind == "prefix":
            return reply.startswith("[")
        if kind == "regex":
            return re.search(self.verifier["pattern"], reply) is not None
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
            "state": reduce_state(conv.log),
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

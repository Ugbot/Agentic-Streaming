"""The turn engine: `handle(turn) -> normalized result`, one conversation at a time.

The engine owns no ordering and no threads. A runtime supplies the single writer per
conversation and calls `handle` from it. Everything it knows about a conversation is a fold
over the event log, read on each delivery, so there is nothing to rebuild after a restart.

Every delivery falls in one cell of the resume state machine (`primitives.md`, section 10). A run
of a turn starts from the top every time and re-derives each step: what the log kept is reused by
`_append`, what the journal recorded is reused by `_invoke` and `_ask_model`, and the rest happens
for the first time. A crash therefore needs no recovery code of its own.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from collections import deque
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Callable, Deque, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from jsonschema import Draft202012Validator

from .bindings import Bindings
from .cep import SequencePattern, compile_patterns, event_time_ms, turns_of
from .errors import CorruptLog, LogUnavailable, ToolError, ToolRefused, TransientError, ValidationError
from .events import ChatMessage, Event, EventLog, Turn, reduce_state, transcript
from .journal import (
    LLM,
    READ,
    SUCCEEDED,
    TOOL,
    UNKNOWN,
    Attempt,
    EffectCall,
    EffectJournal,
    Entry,
    InMemoryEffectJournal,
    JournalKey,
    JournalDivergence,
    intent_of,
    model_intent_of,
)
from .limits import Limits, Violation, who
from .opa import Both, from_environment
from .redact import Redactor
from .policy import ASK_A_PERSON, DENY, DocumentToolPolicy, ToolPolicy, ToolRequest, approver_problem, owner_problem
from .llm import DEFAULT_MAX_ITERATIONS, REACT_SYSTEM, ChatClient, make_chat_client
from .retrieval import KnowledgeBase, Passage
from .untrusted import RULE, mark
from .tools import ToolCall, ToolRegistry, ToolSpec, idempotency_key
from .workflow_timers import WorkflowTimers

LOG = logging.getLogger(__name__)

Clock = Callable[[], int]
Sleep = Callable[[float], None]
Probe = Callable[[str, str, str], None]  # (axis, name, when): a seam for `crash_at`, see Engine.probe

_RECORDED_CALL_EVENTS = frozenset({"tool_called", "delegated", "compensation_step"})

# What the fold of a turn's events says about it: the rows of the resume state machine.
ABSENT = "absent"
RUNNING = "running"  # events and no terminal status, which only a crash can leave
RESUMING = "resuming"  # the same, after a turn_resumed
SUSPENDED = "suspended"
TRANSIENT = "transient"  # ended failed with class transient, so the turn_id is not spent
TERMINAL = "terminal"

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


def turn_state(events: Sequence[Event]) -> str:
    """Which row of the resume state machine a turn is in, from its events alone."""
    state = ABSENT
    for event in events:
        if event.type == "turn_suspended":
            state = SUSPENDED
        elif event.type == "turn_resumed":
            state = RESUMING
        elif event.type == "turn_completed":
            state = TERMINAL
        elif event.type == "turn_failed":
            state = TRANSIENT if _is_transient(event) else TERMINAL
        elif state == ABSENT:
            state = RUNNING
    return state


def _tenant_of(log: Sequence[Event]) -> Optional[str]:
    """The tenant a conversation belongs to: the one that started it (`primitives.md` section 11)."""
    first = next((e for e in log if e.type == "turn_received"), None)
    return (first.payload.get("principal") or {}).get("tenant") if first is not None else None


def _owner_of(log: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """The principal that started a conversation, or None when it started with none."""
    first = next((e for e in log if e.type == "turn_received"), None)
    return first.payload.get("principal") if first is not None else None


def _sender(principal: Optional[Mapping[str, Any]]) -> Optional[tuple]:
    """Who sent a signal, for counting refusals: tenant and subject, or None for everyone unauthenticated."""
    return (principal.get("tenant"), principal.get("subject")) if principal else None


def _refused_before(log: Sequence[Event], turn: Turn) -> bool:
    """True when this turn already has a `signal_refused` from this signal's sender."""
    sender = _sender(turn.principal)
    return any(e.type == "signal_refused" and e.turn_id == turn.turn_id
               and _sender(e.payload.get("signal_principal")) == sender for e in log)


def _principal_of(turn: Turn) -> Dict[str, Any]:
    return {"principal": dict(turn.principal)} if turn.principal is not None else {}


def _is_transient(event: Event) -> bool:
    return event.type == "turn_failed" and event.payload.get("error_class") == "transient"


def _reached_non_idempotent(events: Sequence[Event]) -> bool:
    return any(e.type == "tool_intent" and e.payload.get("effect") == "non_idempotent" for e in events)


def _split_runs(events: Sequence[Event]) -> Tuple[Sequence[Event], Sequence[Event]]:
    """The events of runs that are over, and those of the current run. A run that ended `failed`
    with class `transient` is spent: section 10 says it is not replayed, and the next run takes the
    next free indices. Everything after it is one run, which a crash or a suspension may have cut
    short and which replays like any other. A run that reached a `non_idempotent` tool is never
    spent, because that tool must not be called twice: the whole turn replays."""
    if _reached_non_idempotent(events):
        return (), events
    for i in range(len(events) - 1, -1, -1):
        if _is_transient(events[i]):
            return events[:i + 1], events[i + 1:]
    return (), events


def _next_indices(events: Sequence[Event]) -> Tuple[int, int]:
    """The next free tool index and model index: `(turn_id, index)` names one call across runs."""
    tool = llm = 0
    for event in events:
        index = event.payload.get("index")
        if index is None:
            continue
        if event.type == "llm_called":
            llm = max(llm, int(index) + 1)
        else:
            tool = max(tool, int(index) + 1)
    return tool, llm


class _Prior:
    """What the current run of a turn already put on the log, by `(type, index, attempt)`, or by
    occurrence for events with no index. Appending takes from here first, which is what makes it
    at-most-once."""

    def __init__(self, events: Sequence[Event], resuming_now: bool = False) -> None:
        self.resumes = [e for e in events if e.type == "turn_resumed"]
        last_resume = self.resumes[-1].sequence if self.resumes else -1
        if resuming_now:  # the resume about to be appended answers everything on the log so far
            last_resume = events[-1].sequence + 1 if events else 0
        self._kept: Dict[Tuple[str, Any, Any], Deque[Event]] = {}
        for event in events:
            if event.type == "turn_resumed":
                continue  # each resume is its own: a second is not a repeat
            if event.type in ("turn_suspended", "unknown_outcome") and event.sequence < last_resume:
                continue  # answered by that resume, so one at the same index now is a new one
            self._kept.setdefault(self._key(event.type, event.payload), deque()).append(event)

    @staticmethod
    def _key(kind: str, payload: Mapping[str, Any]) -> Tuple[str, Any, Any]:
        return kind, payload.get("index"), payload.get("attempt")

    def take(self, kind: str, payload: Mapping[str, Any]) -> Optional[Event]:
        kept = self._kept.get(self._key(kind, payload))
        return kept.popleft() if kept else None

    def peek(self, kind: str, index: Optional[int] = None) -> Optional[Event]:
        kept = self._kept.get((kind, index, None))
        return kept[0] if kept else None


@dataclass
class _TurnContext:
    turn: Turn
    text: str
    started_ms: int
    events: List[Event] = field(default_factory=list)
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    next_index: int = 0
    next_llm_index: int = 0
    path: Optional[str] = None
    prior: Optional[_Prior] = None
    rerun: bool = False  # an earlier run ended transient, so turn_received is on the log for good
    history: Sequence[Event] = ()  # the log as it stood before this run's first event

    def take_index(self) -> int:
        index = self.next_index
        self.next_index += 1
        return index

    def take_llm_index(self) -> int:
        index = self.next_llm_index
        self.next_llm_index += 1
        return index

    def segment(self) -> List[Event]:
        """A result's events: the turn's, from its last `turn_resumed`."""
        ordered = sorted(self.events, key=lambda e: e.sequence)
        starts = [i for i, e in enumerate(ordered) if e.type == "turn_resumed"]
        return ordered[starts[-1]:] if starts else ordered


class _UnknownOutcome(Exception):
    """A call has no answer and cannot be made again, or needs a person first. The turn suspends
    for a person, `until` saying which."""

    def __init__(self, tool: str, index: int, until: str = "unknown_outcome", intent: Optional[str] = None) -> None:
        super().__init__(f"{tool} waits for a person")
        self.tool = tool
        self.index = index
        self.until = until
        self.intent = intent  # for an approval, the call the approver is shown


class _ToolEffect(EffectCall):
    """One tool call as `record_or_get` drives it: the events around each attempt."""

    def __init__(self, engine: "Engine", ctx: _TurnContext, spec: ToolSpec, index: int,
                 args: Dict[str, Any], key: str) -> None:
        self.engine = engine
        self.ctx = ctx
        self.spec = spec
        self.index = index
        self.args = args
        self.key = key
        self.retry = engine.policies.get("retry") or {}

    def on_intent(self, entry: Entry) -> None:
        self.engine._append(self.ctx, "tool_intent", {"tool": self.spec.id, "index": self.index, "args": self.args,
                                                      "effect": self.spec.effect, "idempotency_key": self.key,
                                                      **_principal_of(self.ctx.turn)})

    def on_failed_attempt(self, attempt: Attempt) -> None:
        self.ctx.tool_calls.append({"tool": self.spec.id, "index": self.index,
                                    "args": self.engine.redactor.apply(self.args),
                                    "error": attempt.error, "attempt": attempt.attempt})
        self.engine._append(self.ctx, "tool_failed", {"tool": self.spec.id, "index": self.index,
                                                      "attempt": attempt.attempt, "args": self.args,
                                                      "error": attempt.error})

    def recorded(self, error: str) -> str:
        return self.engine.redactor.apply(error)

    def budget(self) -> int:
        return int(self.retry.get("max_attempts", 1)) if self.retry.get("kind", "none") != "none" else 1

    def attempt(self, attempt: int) -> Any:
        if attempt > 1:
            delay = retry_delay_ms(self.retry, attempt - 1, self.engine.rng)
            if delay:
                self.engine.sleep(delay / 1000.0)
        self.engine._hit("call", self.spec.id, "before")
        call = ToolCall(self.ctx.turn.conversation_id, self.ctx.turn.turn_id, self.index, self.ctx.text)
        return self.engine.redactor.apply(self.engine.tools.execute(self.spec.id, self.args, call))

    def after_attempt(self) -> None:
        self.engine._hit("call", self.spec.id, "during")

    def after_record(self) -> None:
        self.engine._hit("call", self.spec.id, "after")


class _ModelCall(EffectCall):
    """One model call. It retries on its own, inside `ask`, so `record_or_get` sees one answer."""

    def __init__(self, ask: Callable[[], Mapping[str, Any]]) -> None:
        self.ask = ask

    def attempt(self, attempt: int) -> Any:
        return dict(self.ask())


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
        journal: Optional[EffectJournal] = None,
        policy: Optional[ToolPolicy] = None,
    ) -> None:
        self.doc = doc
        self.tools = tools
        self.log = log
        self.journal = journal if journal is not None else InMemoryEffectJournal()
        self._scope = str((doc.get("agent") or {}).get("id", ""))
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
        self.cep: List[SequencePattern] = compile_patterns(doc.get("cep"))
        self.llm: Mapping[str, Any] = doc.get("llm") or {}
        self.chat_client: Optional[ChatClient] = None
        if self.llm.get("provider", "stub") != "stub" and any(
                p.get("brain") == "llm" and "x-brain" not in p for p in self.paths.values()):
            self.chat_client = make_chat_client(self.llm)
        self.policy: ToolPolicy = policy if policy is not None else DocumentToolPolicy(doc)
        opa = from_environment()
        if opa is not None:
            self.policy = Both(self.policy, opa)
        self.limits = Limits(doc)
        self.redactor = Redactor(doc)
        self.guardrails: Sequence[Mapping[str, Any]] = doc.get("guardrails") or []
        path_scoped = {name for path in self.paths.values() for name in path.get("guardrails") or []}
        self.global_guardrails = [g for g in self.guardrails if g.get("name") not in path_scoped]
        self.named_guardrails = {g["name"]: g for g in self.guardrails if g.get("name")}
        retrieval = doc.get("retrieval") or {}
        dim = (doc.get("embeddings") or {}).get("dim", retrieval.get("dim", 256))
        self.kb = KnowledgeBase(retrieval.get("kb") or [], dim, retrieval.get("top_k", 4))
        self.timers = WorkflowTimers(doc.get("timers") or [], self.clock)
        self._schema_validators: Dict[int, Draft202012Validator] = {}
        # A test seam: called as (`event`, type, before|after) around every append and as
        # (`call`, tool, before|during|after) around every tool attempt. It must not alter anything;
        # only a test has a reason to raise from it, and it raises what nothing here can catch.
        self.probe: Optional[Probe] = None

    # -- lifecycle -----------------------------------------------------------

    def recover(self) -> None:
        """Read every conversation, so one that cannot be trusted is reported now. Nothing is kept:
        a delivery folds the log itself. Runs no tool and appends nothing."""
        for conversation_id in self.log.conversation_ids():
            try:
                self.log.read(conversation_id)
            except CorruptLog as exc:  # one bad conversation must not stop the others
                LOG.error("skipping conversation %s on recovery: %s", conversation_id, exc)

    def state(self, conversation_id: str) -> Dict[str, Any]:
        return reduce_state(self.log.read(conversation_id), self.context)

    def transcript(self, conversation_id: str) -> List[ChatMessage]:
        return transcript(self.log.read(conversation_id), self.context)

    def suspended_turns(self, conversation_id: str) -> List[str]:
        return [turn_id for turn_id, events in self._turns_of(self.log.read(conversation_id))
                if turn_state(events) == SUSPENDED]

    def partial_turns(self) -> Iterator[Turn]:
        """Every turn a crash left with events and no terminal status, as a redelivery of it. A turn
        with no `turn_received` yet, which only a crash among its timers can leave, has no text to
        redeliver and waits for its source."""
        for conversation_id in self.log.conversation_ids():
            try:
                log = self.log.read(conversation_id)
            except CorruptLog as exc:
                LOG.error("skipping conversation %s on recovery: %s", conversation_id, exc)
                continue
            for turn_id, events in self._turns_of(log):
                if turn_state(events) not in (RUNNING, RESUMING):
                    continue
                received = next((e for e in events if e.type == "turn_received"), None)
                if received is None:
                    LOG.warning("turn %s of %s stopped before turn_received; it waits for redelivery",
                                turn_id, conversation_id)
                    continue
                yield self._turn_as_received(conversation_id, turn_id, received)

    @staticmethod
    def _turns_of(log: Sequence[Event]) -> List[Tuple[str, List[Event]]]:
        by_turn: Dict[str, List[Event]] = {}
        for event in log:
            by_turn.setdefault(event.turn_id, []).append(event)
        return list(by_turn.items())

    @staticmethod
    def _turn_as_received(conversation_id: str, turn_id: str, received: Event) -> Turn:
        payload = received.payload
        return Turn(conversation_id, turn_id, str(payload.get("text", "")), str(payload.get("user_id", "anonymous")),
                    None, dict(received.metadata), payload.get("principal"))

    # -- the turn ------------------------------------------------------------

    def handle(self, turn: Turn) -> Dict[str, Any]:
        """Process one turn to a terminal status. Must be called by the conversation's
        single writer.

        This is the turn boundary and it does not raise for anything a turn does: whatever
        escapes the turn's own handling ends it `failed` with class `fatal`. `LogUnavailable`
        alone passes through, since a turn cannot record its end in a log it cannot reach."""
        ctx = _TurnContext(turn, turn.text, self.clock())
        try:
            return self._deliver(turn, ctx)
        except LogUnavailable:
            raise
        except Exception as exc:  # the boundary: a per-turn failure must not reach the caller
            return self._fail_at_boundary(ctx, exc)

    def _fail_at_boundary(self, ctx: _TurnContext, exc: Exception) -> Dict[str, Any]:
        message = str(exc) or type(exc).__name__
        ended = ctx.events and ctx.events[-1].type in ("turn_completed", "turn_failed")
        error = {"class": "fatal", "message": message}
        try:
            return self._finish(ctx, "failed", ctx.path, None, error,
                                event=None if ended else ("turn_failed", {"status": "failed", "reason": message}))
        except CorruptLog:  # nothing can be recorded on this conversation; the caller still gets a result
            return self._finish(ctx, "failed", ctx.path, None, error)

    def _deliver(self, turn: Turn, ctx: _TurnContext) -> Dict[str, Any]:
        """The resume state machine of section 10, one branch per cell."""
        log = self.log.read(turn.conversation_id)
        events = [e for e in log if e.turn_id == turn.turn_id]
        idempotent = self.policies.get("idempotency", "turn-id") == "turn-id"
        state = turn_state(events)
        retryable = state == TRANSIENT and idempotent

        tenant = _tenant_of(log)
        if turn.signal is None:
            foreign = owner_problem(self.doc, turn.conversation_id, _owner_of(log), turn.principal)
            if foreign is not None:
                return self.refused(turn.conversation_id, turn.turn_id, foreign, "policy")
        if turn.signal is not None and state != ABSENT:
            problem = approver_problem(self.doc, turn.principal, tenant)
            if problem is not None:
                return self._refuse_signal(ctx, problem)
        if turn.signal is not None:
            if state == SUSPENDED:
                return self._resume(turn, ctx, log, events)
            if state in (ABSENT, RUNNING):
                return self.refused(turn.conversation_id, turn.turn_id, f"no suspended turn {turn.turn_id!r} to resume")
            if state == RESUMING:
                return self._replay(ctx, log, events, resuming=True)
            if retryable:
                self._adopt(ctx, log, events, resuming_now=True)
                self._append(ctx, "turn_resumed", self._resumed_payload(turn))
                return self._continue(ctx, resuming=True)
            return self._duplicate(turn, log, events)
        if state in (RUNNING, RESUMING):
            return self._replay(ctx, log, events, resuming=state == RESUMING)
        if retryable:
            resumed = _reached_non_idempotent(events) and any(e.type == "turn_resumed" for e in events)
            return self._replay(ctx, log, events, resuming=resumed)
        if state != ABSENT and idempotent:
            return self._duplicate(turn, log, events)
        self._adopt(ctx, log, events, fresh=not idempotent)
        return self._continue(ctx, resuming=False)

    def _refuse_signal(self, ctx: _TurnContext, problem: str) -> Dict[str, Any]:
        """Status `failed`, class `policy`. The turn is untouched, and the attempt is on the log."""
        turn = ctx.turn
        if _refused_before(self.log.read(turn.conversation_id), turn):
            # Once per sender and turn: a sender who keeps trying cannot grow the log.
            return self.refused(turn.conversation_id, turn.turn_id, problem, "policy")
        payload = {"turn_id": turn.turn_id, "signal_sender": turn.user_id, "reason": problem}
        if turn.principal is not None:
            payload["signal_principal"] = dict(turn.principal)
        self._append(ctx, "signal_refused", payload)
        return self._finish(ctx, "failed", None, None, {"class": "policy", "message": problem})

    def refused(self, conversation_id: str, turn_id: str, message: str,
                error_class: str = "validation") -> Dict[str, Any]:
        """Status `failed`, nothing appended: the `turn_id` stays unspent."""
        return {"conversation_id": conversation_id, "turn_id": turn_id, "status": "failed", "path": None,
                "reply": None, "state": self._state_or_empty(conversation_id), "tool_calls": [], "events": [],
                "error": {"class": error_class, "message": message},
                "runtime_detail": {"runtime": "local", "refused": True}}

    def _duplicate(self, turn: Turn, log: Sequence[Event], events: Sequence[Event]) -> Dict[str, Any]:
        """The recorded result, with the conversation's state as it is now."""
        recorded = self._result_from_events(turn.conversation_id, turn.turn_id, events, log)
        duplicate = dict(recorded)
        duplicate["status"] = "duplicate"
        duplicate["events"] = []
        duplicate["state"] = reduce_state(log, self.context)
        duplicate["runtime_detail"] = {"duplicate_of": recorded["runtime_detail"], "re_executed": False}
        return duplicate

    def _adopt(self, ctx: _TurnContext, log: Sequence[Event], events: Sequence[Event], fresh: bool = False,
               resuming_now: bool = False) -> None:
        """Make `ctx` the turn as first received, with what its earlier runs left on the log. The
        user, text and metadata are those of `turn_received`, whoever delivered it this time."""
        received = next((e for e in events if e.type == "turn_received"), None)
        if fresh:  # idempotency: none, so this is a new run under an old id, taking the next indices
            ctx.next_index, ctx.next_llm_index = _next_indices(events)
            ctx.history = log
            return
        if received is not None:
            ctx.turn = self._turn_as_received(ctx.turn.conversation_id, ctx.turn.turn_id, received)
            ctx.text = ctx.turn.text
        spent, run = _split_runs(events)
        ctx.next_index, ctx.next_llm_index = _next_indices(spent)
        ctx.rerun = bool(spent)
        ctx.prior = _Prior(run, resuming_now)
        ctx.path = next((str(e.payload["path"]) for e in reversed(events) if e.type == "routed"), None)
        replayed = {e.sequence for e in run}
        ctx.history = [e for e in log if e.sequence not in replayed]

    def _replay(self, ctx: _TurnContext, log: Sequence[Event], events: Sequence[Event],
                resuming: bool) -> Dict[str, Any]:
        self._adopt(ctx, log, events)
        if resuming and ctx.prior is not None:
            ctx.events.extend(ctx.prior.resumes)
        return self._continue(ctx, resuming)

    @staticmethod
    def _resumed_payload(turn: Turn) -> Dict[str, Any]:
        payload = {"turn_id": turn.turn_id, "signal": dict(turn.signal or {}), "signal_sender": turn.user_id}
        if turn.principal is not None:
            payload["signal_principal"] = dict(turn.principal)
        return payload

    def _resume(self, turn: Turn, ctx: _TurnContext, log: Sequence[Event], events: Sequence[Event]) -> Dict[str, Any]:
        suspended = next(e for e in reversed(events) if e.type == "turn_suspended").payload
        if suspended.get("until") == "unknown_outcome":
            problem = self._resolve(turn, suspended, events)
            if problem is not None:
                return self.refused(turn.conversation_id, turn.turn_id, problem)
        resumed = self._resumed_payload(turn)
        if suspended.get("until") == "tool_approval":
            approved = (turn.signal or {}).get("approved")
            if not isinstance(approved, bool):
                return self.refused(turn.conversation_id, turn.turn_id,
                                    f"signal needs approved true or false, got {approved!r}")
            resumed["approval"] = {"tool": suspended["tool"], "index": suspended["index"], "approved": approved}
            if suspended.get("intent") is not None:
                resumed["approval_intent"] = suspended["intent"]
        self._adopt(ctx, log, events, resuming_now=True)
        self._append(ctx, "turn_resumed", resumed)
        return self._continue(ctx, resuming=True)

    def _resolve(self, turn: Turn, suspended: Mapping[str, Any], events: Sequence[Event]) -> Optional[str]:
        """A person says what happened to a call. Returns why a signal is refused, if it is. A
        redelivered signal finds the entry already answered, and the first answer stands."""
        signal = turn.signal or {}
        resolution = signal.get("resolution")
        if resolution not in ("applied", "not_applied", "failed"):
            return f"signal needs a resolution of applied, not_applied or failed, got {resolution!r}"
        tool, index = str(suspended["tool"]), int(suspended["index"])
        key = JournalKey(turn.conversation_id, turn.turn_id, TOOL, index)
        entry = self.journal.get(key)
        if entry is None:  # a journal that did not outlive the log: the log still says what was asked
            intent = next(e.payload for e in events if e.type == "tool_intent" and e.payload.get("index") == index)
            entry = Entry.pending(key, intent_of(tool, intent["args"]), str(intent["effect"])).unknown("journal lost")
        if entry.state != UNKNOWN:
            return None
        if resolution == "applied":
            self.journal.put(entry.succeeded(signal.get("result")))
        elif resolution == "not_applied":
            self.journal.put(entry.release())
        else:
            error = f"{tool} resolved as failed"
            self.journal.put(entry.with_attempt(len(entry.attempts) + 1, error).failed(error))
        return None

    def _continue(self, ctx: _TurnContext, resuming: bool) -> Dict[str, Any]:
        """One run of the turn, from the top. An unknown outcome anywhere in it suspends the turn."""
        try:
            return self._run(ctx, resuming)
        except _UnknownOutcome as exc:
            payload = {"turn_id": ctx.turn.turn_id, "path": ctx.path, "text": ctx.turn.text,
                       "user_id": ctx.turn.user_id, "until": exc.until, "tool": exc.tool, "index": exc.index}
            if exc.intent is not None:
                payload["intent"] = exc.intent  # the approval is for this call and no other
            self._append(ctx, "turn_suspended", payload)
            return self._finish(ctx, "suspended", ctx.path, None, None)
        except ValidationError as exc:
            return self._fail(ctx, ctx.path, exc.error_class, str(exc))

    def _run(self, ctx: _TurnContext, resuming: bool) -> Dict[str, Any]:
        turn = ctx.turn
        prior = ctx.prior
        if resuming and ctx.rerun and (prior is None or prior.peek("routed") is None):
            return self._after_routing(ctx, str(ctx.path))  # a redelivered signal's run starts past the router

        def append(kind: str, payload: Mapping[str, Any]) -> Event:
            return self._append(ctx, kind, payload)

        received = prior.peek("turn_received") if prior is not None else None
        recorded_ms = received.payload.get("processing_time_ms") if received is not None else None
        try:
            self.timers.fire_due(turn, ctx.history, append, lambda tool_id, args: self._invoke(ctx, tool_id, args, origin="timer"),
                                 recorded_ms)
        except ToolError as exc:
            return self._fail(ctx, None, "tool", str(exc))
        if not ctx.rerun:
            append("turn_received", self.timers.received_payload(turn, self._received_payload(turn)))
        self.timers.schedule(turn, ctx.history, append)
        if (not ctx.rerun and not resuming) or self.limits.stopped():
            self._enforce(ctx, self.limits.admit_turn(who(turn), f"{turn.conversation_id}\0{turn.turn_id}", self.clock()))

        blocked = self._check_guardrails(self.global_guardrails, "input", turn.text)
        if blocked is not None:
            append("guardrail_rejected", {"reason": blocked, "stage": "input"})
            return self._finish(ctx, "rejected", None, None, {"class": "guardrail", "message": blocked})

        path = self._route(turn.text)
        ctx.path = path
        append("routed", {"path": path})
        failed = self._match_patterns(ctx, path)
        if failed is not None:
            return failed
        spec = self.paths[path]

        blocked = self._check_guardrails(self._path_guardrails(spec), "input", turn.text)
        if blocked is not None:
            append("guardrail_rejected", {"reason": blocked, "stage": "input", "path": path})
            return self._finish(ctx, "rejected", path, None, {"class": "guardrail", "message": blocked})

        if "x-suspend-until" in spec and not resuming:
            append("turn_suspended", {"turn_id": turn.turn_id, "path": path, "text": turn.text,
                                      "user_id": turn.user_id, "until": spec["x-suspend-until"]})
            return self._finish(ctx, "suspended", path, None, None)
        return self._after_routing(ctx, path)

    def _after_routing(self, ctx: _TurnContext, path: str) -> Dict[str, Any]:
        if self.saga is not None:
            return self._run_saga(ctx, path)
        return self._run_brain(ctx, path)

    def _fail(self, ctx: _TurnContext, path: Optional[str], error_class: str, message: str) -> Dict[str, Any]:
        return self._finish(ctx, "failed", path, None, {"class": error_class, "message": message},
                            event=("turn_failed", {"status": "failed", "reason": message}))

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
                return self._fail(ctx, path, "tool", str(exc))
            except TransientError as exc:
                return self._fail(ctx, path, "transient", str(exc))
            self._append(ctx, "reply_drafted", {"reply": reply})
            if self._verify(spec, reply):
                blocked = self._check_guardrails(list(self.global_guardrails) + self._path_guardrails(spec),
                                                 "output", reply)
                if blocked is not None:
                    self._append(ctx, "guardrail_rejected", {"reason": blocked, "stage": "output", "path": path})
                    return self._finish(ctx, "rejected", path, reply, {"class": "guardrail", "message": blocked})
                return self._complete(ctx, path, reply)
            self._append(ctx, "verification_failed", {"reply": reply})
        status = "unverified" if verification.get("on_exhausted", "unverified") == "unverified" else "failed"
        return self._finish(ctx, status, path, reply,
                            {"class": "verification", "message": "verifier rejected the reply"})

    def _match_patterns(self, ctx: _TurnContext, path: str) -> Optional[Dict[str, Any]]:
        """Sequence CEP over this conversation's turns up to this one; a pattern completing on this
        turn invokes its tool (recorded on this turn). A turn replayed after others arrived sees what
        it saw the first time. Returns the failed result when that tool fails, else None."""
        if not self.cep:
            return None
        log = self.log.read(ctx.turn.conversation_id)
        last = next((e.sequence for e in log if e.turn_id == ctx.turn.turn_id and e.type == "turn_received"),
                    len(log))
        turns = turns_of(log[:last + 1])
        for pattern in self.cep:
            if pattern.completes_on(turns):
                try:
                    self._invoke(ctx, pattern.tool, pattern.match_args(ctx.turn.conversation_id), origin="cep")
                except ToolError as exc:
                    return self._fail(ctx, path, "tool", str(exc))
        return None

    def _received_payload(self, turn: Turn) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"turn_id": turn.turn_id, "text": turn.text, "user_id": turn.user_id}
        if turn.principal is not None:
            payload["principal"] = dict(turn.principal)
        event_time = event_time_ms(turn.metadata)
        if event_time is not None:
            payload["event_time_ms"] = event_time
        return payload

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
        if self.chat_client is not None:
            return self._draft_react(ctx, path, self.chat_client)
        for i, step in enumerate(self.llm.get("script") or []):
            answer = self._ask_model(ctx, self.paths[path].get("prompt", ""), partial(self._scripted_answer, i, step))
            if "tool" in answer:
                self._invoke(ctx, answer["tool"], answer.get("args") or {})
            else:
                return str(answer["text"])
        raise ValidationError("the llm script ended without a final text", "/llm/script")

    def _draft_react(self, ctx: _TurnContext, path: str, client: ChatClient) -> str:
        """The bounded loop a real provider drives: the model picks a tool or answers. A tool outside
        the policy is refused at the tool boundary and ends the turn. The model's view is the log as it stood before this run, which a re-run sees the same. An observation goes back
        as a user message, because the `tool` role needs a tool_calls id this protocol does not have."""
        spec = self.paths[path]
        declared = DocumentToolPolicy(self.doc).tools_of(path)
        tools = [{"name": s.id, "description": s.description} for s in self.tools.ids().values()
                 if s.id in declared]
        listing = ", ".join(f"{t['name']}: {t['description']}" for t in tools)
        system = f"{spec.get('prompt', '')}\n{REACT_SYSTEM}{listing}\n{RULE}".strip()
        messages = [{"role": "system", "content": system}]
        messages += [{"role": m.role, "content": m.text} for m in transcript(ctx.history, self.context)]
        messages.append({"role": "user", "content": ctx.text})
        limit = max(1, int(spec.get("max_iterations", DEFAULT_MAX_ITERATIONS)))
        for _ in range(limit):
            answer = self._ask_model(ctx, system, partial(self._chat, client, messages, tools))
            if "tool" not in answer:
                return f"[{path}] {answer.get('text', '')}"
            tool_id, args = str(answer["tool"]), answer.get("args") or {}
            messages.append({"role": "assistant", "content": json.dumps({"tool": tool_id, "args": args})})
            result = self._invoke(ctx, tool_id, args)
            observed = mark("tool:" + tool_id, str(result))
            messages.append({"role": "user", "content": f"Result of {tool_id}: {observed}"})
        return f"[{path}] (stopped after {limit} steps)"

    def _chat(self, client: ChatClient, messages: List[Dict[str, str]], tools: List[Dict[str, str]]) -> Dict[str, Any]:
        """One model call under `policies.retry`. Only the call is retried, so a tool the model
        already called is not called again. A refusal is not retried."""
        retry = self.policies.get("retry") or {}
        max_attempts = retry.get("max_attempts", 1) if retry.get("kind", "none") != "none" else 1
        for attempt in range(1, max_attempts + 1):
            try:
                return client.chat(list(messages), tools)
            except TransientError:
                if attempt >= max_attempts:
                    raise
                delay = retry_delay_ms(retry, attempt, self.rng)
                if delay:
                    self.sleep(delay / 1000.0)
        raise AssertionError("unreachable")

    def _ask_model(self, ctx: _TurnContext, request: str, ask: Callable[[], Mapping[str, Any]]) -> Mapping[str, Any]:
        """One model call, journaled by its own index in the turn: a recorded reply is reused, so a
        re-run takes the same path. A reply recorded for a different `request` is a divergence."""
        index = ctx.take_llm_index()
        key = JournalKey(ctx.turn.conversation_id, ctx.turn.turn_id, LLM, index)
        intent = model_intent_of(self._scope, self.redactor.apply(f"{request}\0{ctx.text}"))
        fresh = self.journal.get(key) is None
        if fresh:
            self._enforce(ctx, self.limits.admit_model(who(ctx.turn), self.clock()))
        live: List[Mapping[str, Any]] = []

        def recorded() -> Mapping[str, Any]:  # this run acts on the answer; the journal keeps it redacted
            live.append(ask())
            return self.redactor.apply(live[0])

        entry = self.journal.record_or_get(key, intent, READ, _ModelCall(recorded))
        answer: Mapping[str, Any] = live[0] if live else entry.result
        if fresh:
            self.limits.charge_model(who(ctx.turn), self.clock(), len(request) + len(ctx.text) + len(str(answer)))
        model = {"model": self.llm["model"]} if self.llm.get("model") else {}
        self._append(ctx, "llm_called", {"index": index, **model, **answer})
        return answer

    def _scripted_answer(self, index: int, step: Mapping[str, Any]) -> Dict[str, Any]:
        """The `stub` provider's answer. `x-count-calls` adds how many real calls the step has seen."""
        self._call_model(index, step)
        answer = {k: step[k] for k in ("tool", "args", "text") if k in step}
        if step.get("x-count-calls"):
            world = self.tools.world
            with world.guard:
                world.model_calls[index] = seen = world.model_calls.get(index, 0) + 1
            answer["text"] = f"{answer['text']} #{seen}"
        return answer

    def _call_model(self, index: int, step: Mapping[str, Any]) -> None:
        """One scripted model call under `policies.retry`. Only the call is retried, so a tool
        the model already called is not called again."""
        retry = self.policies.get("retry") or {}
        max_attempts = retry.get("max_attempts", 1) if retry.get("kind", "none") != "none" else 1
        for attempt in range(1, max_attempts + 1):
            world = self.tools.world
            with world.guard:
                world.model_attempts[index] = seen = world.model_attempts.get(index, 0) + 1
            if seen > step.get("x-fail-attempts", 0):
                return
            if attempt < max_attempts:
                delay = retry_delay_ms(retry, attempt, self.rng)
                if delay:
                    self.sleep(delay / 1000.0)
        raise TransientError("stub model unavailable")

    def _run_saga(self, ctx: _TurnContext, path: str) -> Dict[str, Any]:
        """Any failure of a step compensates, whatever its class. An unknown outcome, forward or
        backward, suspends the turn, and the saga carries on from the journal when it is answered."""
        assert self.saga is not None
        done: List[Tuple[Mapping[str, Any], Dict[str, Any], Any]] = []
        for step in self.saga["steps"]:
            args = dict(step.get("args") or {})
            try:
                result = self._invoke(ctx, step["tool"], args, origin="saga")
            except (ToolError, ValidationError) as exc:
                self._append(ctx, "compensation_started", {"failed_step": step.get("name", step["tool"])})
                compensated, failed_undos = self._compensate(ctx, done)
                if failed_undos:
                    return self._fail(ctx, path, "fatal", f"compensation failed for {failed_undos} after: {exc}")
                self._append(ctx, "compensation_completed", {"steps": len(done), "compensated": compensated})
                return self._fail(ctx, path, exc.error_class, str(exc))
            done.append((step, args, result))
        return self._complete(ctx, path, f"[{path}] saga completed")

    def _compensate(self, ctx: _TurnContext,
                    done: Sequence[Tuple[Mapping[str, Any], Dict[str, Any], Any]]) -> Tuple[int, List[str]]:
        """Every undo in reverse order, each given what its step did. One that fails is recorded and
        passed over, so the rest still run. Returns how many ran and the ids of those that failed."""
        compensated = 0
        failed: List[str] = []
        for step, args, result in reversed(done):
            undo = step.get("compensate_with") or self.tools.spec(step["tool"]).compensation
            if not undo:
                continue
            forward = {"forward_args": args, "forward_result": result}
            try:
                self._invoke(ctx, undo, forward, event="compensation_step", strict=True, origin="compensation")
                compensated += 1
            except (ToolError, ValidationError) as exc:
                failed.append(undo)
                index = ctx.tool_calls[-1]["index"] if isinstance(exc, ToolError) else None
                self._append(ctx, "compensation_failed", {"tool": undo, "index": index,
                                                          "step": step.get("name", step["tool"]), "error": str(exc)})
        return compensated, failed

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

    def _already_begun(self, ctx: _TurnContext, tool_id: str, args: Mapping[str, Any]) -> bool:
        """This very call, the same tool and arguments at the next index, was allowed when it began.
        Another call at that index was not: a re-run whose journal is gone asks the model again."""
        turn = ctx.turn
        intent = intent_of(tool_id, self.redactor.apply(dict(args)), self._scope)
        begun = ctx.prior.peek("tool_intent", ctx.next_index) if ctx.prior is not None else None
        if begun is not None and intent == intent_of(str(begun.payload.get("tool")),
                                                     begun.payload.get("args") or {}, self._scope):
            return True
        entry = self.journal.get(JournalKey(turn.conversation_id, turn.turn_id, TOOL, ctx.next_index))
        return entry is not None and entry.intent == intent

    def _authorise(self, ctx: _TurnContext, tool_id: str, args: Mapping[str, Any], origin: str) -> None:
        """The one check every tool call passes (`primitives.md` section 11), whatever asked for it.
        A refusal is on the log before the turn ends, and the turn does end: nothing offers it back."""
        # A call already begun was allowed then: a re-run takes the path the first run took.
        begun = self._already_begun(ctx, tool_id, args)
        if not (getattr(self.policy, "outside_this_process", False) and begun):
            self._decide(ctx, tool_id, args, origin)
        begun = self.limits.has_quota(tool_id) and begun
        self._enforce(ctx, self.limits.admit_tool(who(ctx.turn), self.clock(), tool_id, begun))

    def _enforce(self, ctx: _TurnContext, violation: Optional[Violation]) -> None:
        """A limit that was exceeded is on the log, and the turn ends."""
        if violation is None:
            return
        limit, detail = violation
        self._append(ctx, "limit_exceeded", {"limit": limit, "who": who(ctx.turn), **detail})
        raise ToolRefused(f"{limit} exceeded for {who(ctx.turn)}")

    def _decide(self, ctx: _TurnContext, tool_id: str, args: Mapping[str, Any], origin: str) -> None:
        known = self.tools.ids().get(tool_id)
        effect = known.effect if known is not None else None
        decision = self.policy.decide(ToolRequest(tool_id, origin, ctx.path, effect, args, ctx.turn.principal))
        if decision.verdict == DENY:
            self._refuse(ctx, tool_id, origin, decision.reason)
        if decision.verdict == ASK_A_PERSON:
            self._gate(ctx, tool_id, origin, args)

    def _refuse(self, ctx: _TurnContext, tool_id: str, origin: str, reason: str) -> None:
        self._append(ctx, "tool_refused", {"tool": tool_id, "origin": origin, "path": ctx.path,
                                           "reason": reason, **_principal_of(ctx.turn)})
        raise ToolRefused(reason)

    def _gate(self, ctx: _TurnContext, tool_id: str, origin: str, args: Mapping[str, Any]) -> None:
        """A tool that needs a person waits for one. A timer or a pattern has nobody to ask. An answer
        about another call at this index, other arguments after a re-run, is no answer."""
        if origin in ("timer", "cep"):
            self._refuse(ctx, tool_id, origin, f"tool {tool_id} needs a person and {origin} has nobody to ask")
        index = ctx.next_index
        intent = intent_of(tool_id, self.redactor.apply(dict(args)), self._scope)
        answers = [e.payload["approval"] for e in self.log.read(ctx.turn.conversation_id)
                   if e.type == "turn_resumed" and e.turn_id == ctx.turn.turn_id
                   and (e.payload.get("approval") or {}).get("index") == index
                   and e.payload["approval"]["tool"] == tool_id
                   and e.payload.get("approval_intent") in (None, intent)]
        if not answers:
            raise _UnknownOutcome(tool_id, index, "tool_approval", intent)
        if not answers[-1]["approved"]:
            self._refuse(ctx, tool_id, origin, f"an approver refused tool {tool_id}")

    def _invoke(self, ctx: _TurnContext, tool_id: str, args: Mapping[str, Any],
                event: Optional[str] = None, strict: bool = False, origin: str = "brain") -> Any:
        """`record_or_get` for one tool call. Arguments are checked before the call is journaled or
        given an index. `strict` raises when the budget is spent whatever `on_tool_error` says: a saga
        undo that failed must never read as compensated."""
        self._authorise(ctx, tool_id, args, origin)
        spec: ToolSpec = self.tools.spec(tool_id)
        self.tools.validate_args(tool_id, args)
        args = dict(args)
        turn = ctx.turn
        index = ctx.take_index()
        key = JournalKey(turn.conversation_id, turn.turn_id, TOOL, index)
        ikey = idempotency_key(turn.conversation_id, turn.turn_id, index)
        kept = self.redactor.apply(args)
        intent = intent_of(tool_id, kept, self._scope)
        if ctx.prior is not None and ctx.prior.peek("tool_intent", index) is not None:
            # The log says this call was begun, so a journal that lost it must not read as a first call.
            self.journal.put_if_absent(Entry.pending(key, intent, spec.effect))
        entry = self.journal.record_or_get(key, intent, spec.effect, _ToolEffect(self, ctx, spec, index, args, ikey))
        if entry.state == UNKNOWN:
            self._append(ctx, "unknown_outcome", {"tool": tool_id, "index": index, "args": args,
                                                  "idempotency_key": ikey,
                                                  "reason": entry.error or "no recorded result"})
            raise _UnknownOutcome(tool_id, index)
        if entry.state == SUCCEEDED:
            attempt = len(entry.attempts) + 1
            ctx.tool_calls.append({"tool": tool_id, "index": index, "args": kept,
                                   "result": entry.result, "attempt": attempt})
            kind = event or ("delegated" if spec.kind == "agent" else "tool_called")
            self._append(ctx, kind, {"tool": tool_id, "index": index, "attempt": attempt,
                                     "args": args, "result": entry.result})
            return entry.result
        if self.policies.get("on_tool_error", "fail") == "continue" and not strict:
            return None
        raise ToolError(entry.error or f"{tool_id} failed")

    # -- bookkeeping ---------------------------------------------------------

    def _hit(self, axis: str, name: str, when: str) -> None:
        if self.probe is not None:
            self.probe(axis, name, when)

    def _append(self, ctx: _TurnContext, kind: str, payload: Mapping[str, Any]) -> Event:
        """At-most-once per `(turn_id, type, index, attempt)`: what the log kept is reused."""
        kept = ctx.prior.take(kind, payload) if ctx.prior is not None else None
        if kept is not None:
            ctx.events.append(kept)
            return kept
        self._hit("event", kind, "before")
        event = self.log.append(ctx.turn.conversation_id, ctx.turn.turn_id, kind, self.redactor.apply(payload),
                                self.clock(), ctx.turn.metadata)
        ctx.events.append(event)
        self._hit("event", kind, "after")
        return event

    def _complete(self, ctx: _TurnContext, path: str, reply: str) -> Dict[str, Any]:
        turn = ctx.turn
        ahead = self.journal.get(JournalKey(turn.conversation_id, turn.turn_id, TOOL, ctx.next_index))
        if ahead is not None:  # a recorded effect must not vanish behind a success
            raise JournalDivergence(ahead.key, ahead.intent, None)
        self._append(ctx, "memory_written", {"messages": [{"role": "user", "text": ctx.text},
                                                          {"role": "assistant", "text": reply}]})
        return self._finish(ctx, "completed", path, reply, None, event=("turn_completed", {"reply": reply}))

    def _finish(self, ctx: _TurnContext, status: str, path: Optional[str], reply: Optional[str],
                error: Optional[Dict[str, Any]],
                event: Optional[Tuple[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        if event is not None:
            payload = dict(event[1])
            if error is not None:
                payload.update(error=error, error_class=error["class"])
            self._append(ctx, event[0], payload)
        elif status in ("rejected", "unverified"):
            self._append(ctx, "turn_failed", {"status": status, "error": error,
                                              "error_class": (error or {}).get("class")})
        return {
            "conversation_id": ctx.turn.conversation_id,
            "turn_id": ctx.turn.turn_id,
            "status": status,
            "path": path,
            "reply": reply,
            "state": self._state_or_empty(ctx.turn.conversation_id),
            "tool_calls": list(ctx.tool_calls),
            "events": [e.normalized() for e in ctx.segment()],
            # The result leaves the process, to a caller and a results topic: redacted as the log is.
            "error": self.redactor.apply(error),
            "runtime_detail": {"runtime": "local", "started_ms": ctx.started_ms,
                               "elapsed_ms": self.clock() - ctx.started_ms},
        }

    def _state_or_empty(self, conversation_id: str) -> Dict[str, Any]:
        """A corrupt log still gets a result."""
        try:
            return self.state(conversation_id)
        except CorruptLog:
            return {}

    def _result_from_events(self, conversation_id: str, turn_id: str, events: Sequence[Event],
                            log: Sequence[Event]) -> Dict[str, Any]:
        """A recorded result, rebuilt from the turn's events alone: those of its last run, which is
        the run that answered."""
        end = len(events) - 1 if _is_transient(events[-1]) else len(events)
        run = list(_split_runs(events[:end])[1]) + list(events[end:])
        path = next((str(e.payload["path"]) for e in reversed(events) if e.type == "routed"), None)
        status = "suspended"
        reply: Optional[str] = None
        error: Optional[Dict[str, Any]] = None
        tool_calls: List[Dict[str, Any]] = []
        for event in run:
            p = event.payload
            if event.type in ("reply_drafted", "turn_completed"):
                reply = str(p["reply"])
                if event.type == "turn_completed":
                    error = None  # a later run of the turn finished it
                    status = "completed"
            elif event.type == "turn_failed":
                status = str(p.get("status", "failed"))
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
        return {
            "conversation_id": conversation_id,
            "turn_id": turn_id,
            "status": status,
            "path": path,
            "reply": reply if status in ("completed", "unverified", "rejected") else None,
            "state": reduce_state(log, self.context),
            "tool_calls": tool_calls,
            "events": [e.normalized() for e in run],
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
        return self._engine._invoke(self._ctx, tool_id, args)

    def retrieve(self, query: str, k: int = 0) -> List[Passage]:
        return self._engine._retrieve(self._ctx, query, k)

"""Portable, keyed NFA matcher over an event stream — the cross-engine equivalent of
Flink CEP (the Python port of ``org.jagentic.core.cep``).

Feed events per key with :meth:`CepMatcher.match`; it advances partial matches and emits
completed :class:`Match` es. ``within`` is enforced by expiring partials whose first event
is older than the bound (also exposed via :meth:`CepMatcher.flush_expired` for timer-driven
expiry).

Semantics per event (deterministic relaxed/strict, no ``followedByAny`` non-determinism):
existing partials advance one stage if the next stage's condition matches; on a non-match a
``NEXT`` (strict) partial is dropped and a ``FOLLOWED_BY`` (relaxed) partial waits; every
event may also start a new partial at stage 0. A completed partial is emitted and not reused.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .core import Event

# A CEP stage predicate: ``(event, matched_so_far) -> bool``. Simple conditions ignore
# ``matched_so_far``; iterative conditions inspect the events already matched in this partial
# (the portable form of Flink's SimpleCondition / IterativeCondition).
Condition = Callable[[Event, List[Event]], bool]


def simple(pred: Callable[[Event], bool]) -> Condition:
    """Wrap an event-only predicate ``(event) -> bool`` as a :data:`Condition`."""
    return lambda event, matched_so_far: pred(event)


def any_() -> Condition:
    """A condition that always matches."""
    return lambda event, matched_so_far: True


class Contiguity(Enum):
    """Contiguity on the transition INTO a stage. ``NEXT`` = strict (the very next event must
    match, else the partial is dropped); ``FOLLOWED_BY`` = relaxed (non-matching events are
    skipped, the partial waits). ``BEGIN`` marks the first stage."""

    BEGIN = "BEGIN"
    NEXT = "NEXT"
    FOLLOWED_BY = "FOLLOWED_BY"


@dataclass(frozen=True)
class Stage:
    """One named stage with a contiguity and a condition."""

    name: str
    contiguity: Contiguity
    condition: Condition


class Pattern:
    """A CEP pattern: an ordered list of named stages with a contiguity each, plus an optional
    ``within`` time bound on the whole match. The portable counterpart of Flink's
    ``Pattern.begin(..).next(..).followed_by(..).within(..)``.

    Example::

        p = (Pattern.begin("first", any_())
             .followed_by("second", any_())
             .followed_by("third", any_())
             .within(5 * 60 * 1000))
    """

    def __init__(self) -> None:
        self.stages: List[Stage] = []
        self.within_millis: int = 0  # 0 = unbounded

    @classmethod
    def begin(cls, name: str, condition: Condition) -> "Pattern":
        p = cls()
        p.stages.append(Stage(name, Contiguity.BEGIN, condition))
        return p

    def next(self, name: str, condition: Condition) -> "Pattern":
        """Strict contiguity: the immediately-next event must satisfy ``condition``."""
        self.stages.append(Stage(name, Contiguity.NEXT, condition))
        return self

    def followed_by(self, name: str, condition: Condition) -> "Pattern":
        """Relaxed contiguity: skip non-matching events until one satisfies ``condition``."""
        self.stages.append(Stage(name, Contiguity.FOLLOWED_BY, condition))
        return self

    def within(self, millis: int) -> "Pattern":
        """Bound the whole match to ``millis`` from the first matched event (0 = unbounded)."""
        self.within_millis = millis
        return self


@dataclass
class Match:
    """A completed pattern match: the matched events in order, plus a name->event map (stage
    name to the event that satisfied it, in order)."""

    events: List[Event]
    named: Dict[str, Event]


@dataclass
class _Partial:
    events: List[Event]
    stage: int
    start_ts: int


class CepMatcher:
    """A portable keyed NFA matcher. Mirrors the Java ``CepMatcher`` semantics exactly."""

    def __init__(self, pattern: Pattern) -> None:
        self._pattern = pattern
        self._by_key: Dict[str, List[_Partial]] = {}

    def match(self, key: str, ts: int, event: Event) -> List[Match]:
        """Feed one event for ``key`` at logical time ``ts``; return any completed matches."""
        stages = self._pattern.stages
        within = self._pattern.within_millis
        last = len(stages) - 1

        partials = self._by_key.setdefault(key, [])
        if within > 0:
            partials = [p for p in partials if ts - p.start_ts <= within]

        completed: List[Match] = []
        survivors: List[_Partial] = []

        for p in partials:
            next_stage = p.stage + 1
            stage = stages[next_stage]
            if stage.condition(event, p.events):
                advanced = p.events + [event]
                if next_stage == last:
                    completed.append(self._to_match(advanced, stages))
                else:
                    survivors.append(_Partial(advanced, next_stage, p.start_ts))
            elif stage.contiguity == Contiguity.FOLLOWED_BY:
                survivors.append(p)  # relaxed: skip this event, keep waiting
            # NEXT (strict) + non-match -> drop p

        first = stages[0]
        if first.condition(event, []):
            ev = [event]
            if last == 0:
                completed.append(self._to_match(ev, stages))
            else:
                survivors.append(_Partial(ev, 0, ts))

        self._by_key[key] = survivors
        return completed

    def flush_expired(self, key: str, now: int) -> List[List[Event]]:
        """Remove and return the matched-events of partials that have exceeded ``within`` as of
        ``now`` (the portable form of Flink's timed-out partial matches). Empty if unbounded."""
        within = self._pattern.within_millis
        out: List[List[Event]] = []
        if within <= 0:
            return out
        partials = self._by_key.get(key)
        if partials is None:
            return out
        survivors: List[_Partial] = []
        for p in partials:
            if now - p.start_ts > within:
                out.append(p.events)
            else:
                survivors.append(p)
        self._by_key[key] = survivors
        return out

    @staticmethod
    def _to_match(events: List[Event], stages: List[Stage]) -> Match:
        named: Dict[str, Event] = {}
        for i in range(min(len(events), len(stages))):
            named[stages[i].name] = events[i]
        return Match(list(events), named)


class CepObserver:
    """Bridges a :class:`CepMatcher` onto the event stream: an ``EventObserver`` (a callable
    taking an event) that keys + timestamps each event, feeds the matcher, and invokes
    ``on_match`` for every completed match — the portable equivalent of routing Flink CEP
    matches to a ``PatternProcessFunction``."""

    def __init__(
        self,
        pattern: Pattern,
        key_fn: Callable[[Event], str],
        ts_fn: Callable[[Event], int],
        on_match: Callable[[Match], None],
    ) -> None:
        self._matcher = CepMatcher(pattern)
        self._key_fn = key_fn
        self._ts_fn = ts_fn
        self._on_match = on_match

    def __call__(self, event: Event) -> None:
        for match in self._matcher.match(self._key_fn(event), self._ts_fn(event), event):
            self._on_match(match)


# ---------------------------------------------------------------------------
# Declarative CEP spec compiler — the Python port of org.jagentic.core.cep.CepSpec
# + CepWiring. Compiles a pipeline's ``cep:`` section into runnable wirings that the
# loader feeds inbound events to (one match -> one action), mirroring Java exactly.
# ---------------------------------------------------------------------------

# Metadata flag marking an event injected by a CEP action (so CEP does not re-match it).
DERIVED = "__cep_derived__"

# A compiled CEP action: ``(match, key, runtime, tools) -> None``. A ``submit`` action
# injects a derived Event through the inner runtime; a ``tool`` action calls a tool.
Action = Callable[[Match, str, Any, Any], None]


class CepWiring:
    """One declarative CEP rule: a :class:`CepMatcher` plus key/timestamp extractors and
    the action to fire on a completed match. The Python port of the Java ``CepWiring``.

    :meth:`on_event` feeds an inbound event to the matcher and fires the action for each
    completed match. Events the action itself produced are tagged (:data:`DERIVED`) and
    skipped, so a ``submit`` action cannot recurse.
    """

    def __init__(
        self,
        name: str,
        matcher: CepMatcher,
        key_fn: Callable[[Event], str],
        ts_fn: Callable[[Event], int],
        action: Action,
    ) -> None:
        self.name = name
        self._matcher = matcher
        self._key_fn = key_fn
        self._ts_fn = ts_fn
        self._action = action

    def on_event(self, event: Event, runtime: Any, tools: Any) -> None:
        """Feed one inbound event; fire the action for every completed match."""
        if event.metadata and DERIVED in event.metadata:
            return  # don't re-match events a CEP action produced
        key = self._key_fn(event)
        ts = self._ts_fn(event)
        for match in self._matcher.match(key, ts, event):
            self._action(match, key, runtime, tools)


def compile_cep(specs: Optional[List[Dict[str, Any]]]) -> List[CepWiring]:
    """Compile a pipeline's ``cep:`` section into runnable :class:`CepWiring` s.

    Each spec::

        {name, key, ts, within, pattern: [{stage, where, contiguity}], on_match}

    ``where`` mini-language -> :data:`Condition`:
      * ``any`` (or ``None``) -> always matches
      * ``{text_contains: s | [..]}`` -> ``event.text`` contains any needle
      * ``{metadata_equals: {k: v}}`` -> all ``metadata[k] == str(v)``
      * ``{metadata_gt: {k: n}}`` -> ``float(metadata[k]) > n``
    """
    wirings: List[CepWiring] = []
    for s in specs or []:
        name = str(s.get("name", "cep"))
        pattern = _build_pattern(s.get("pattern"), int(s.get("within", 0) or 0))
        key_fn = _key_fn(str(s.get("key", "conversation_id")))
        ts_fn = _ts_fn(s.get("ts"))
        action = _action(s.get("on_match"))
        wirings.append(CepWiring(name, CepMatcher(pattern), key_fn, ts_fn, action))
    return wirings


def _build_pattern(stages: Optional[List[Dict[str, Any]]], within: int) -> Pattern:
    if not stages:
        raise ValueError("cep pattern needs at least one stage")
    pattern: Optional[Pattern] = None
    for st in stages:
        stage = str(st.get("stage", "s"))
        cond = _condition(st.get("where"))
        if pattern is None:
            pattern = Pattern.begin(stage, cond)
        elif str(st.get("contiguity", "followedBy")).lower() == "next":
            pattern = pattern.next(stage, cond)
        else:
            pattern = pattern.followed_by(stage, cond)
    return pattern.within(within)


def _condition(where: Any) -> Condition:
    """Compile a ``where`` clause into a :data:`Condition` (mirrors Java ``CepSpec.condition``)."""
    if where is None or where == "any":
        return any_()
    if isinstance(where, dict):
        if "text_contains" in where:
            needles = _as_list(where["text_contains"])
            return simple(lambda e: e.text is not None and any(n in e.text for n in needles))
        if "metadata_equals" in where:
            kv = where["metadata_equals"] or {}
            return simple(lambda e: all(str(v) == _meta(e, str(k)) for k, v in kv.items()))
        if "metadata_gt" in where:
            kv = where["metadata_gt"] or {}
            return simple(lambda e: all(_meta_gt(e, str(k), v) for k, v in kv.items()))
    raise ValueError(f"unknown cep where: {where!r}")


def _meta_gt(event: Event, field: str, bound: Any) -> bool:
    raw = _meta(event, field)
    try:
        return raw is not None and float(raw) > float(bound)
    except (TypeError, ValueError):
        return False


def _key_fn(key: str) -> Callable[[Event], str]:
    if key.startswith("metadata."):
        f = key[len("metadata."):]
        return lambda e: _meta(e, f)
    # conversation_id / conversationId / default
    return lambda e: e.conversation_id


def _ts_fn(ts: Optional[Any]) -> Callable[[Event], int]:
    if ts is not None and str(ts).startswith("metadata."):
        f = str(ts)[len("metadata."):]

        def from_meta(e: Event) -> int:
            v = _meta(e, f)
            try:
                return int(v) if v is not None else 0
            except (TypeError, ValueError):
                return 0

        return from_meta
    counter = itertools.count()  # no ts -> monotonic arrival order
    return lambda e: next(counter)


def _action(on_match: Optional[Dict[str, Any]]) -> Action:
    if on_match is None:
        return lambda match, key, runtime, tools: None  # detect-only
    kind = str(on_match.get("kind", "submit"))
    if kind == "tool":
        tool_id = str(on_match.get("tool"))
        args = on_match.get("args") or {}
        return lambda match, key, runtime, tools: tools.execute(tool_id, args)
    if kind == "submit":
        text = str(on_match.get("text", "cep match"))

        def submit(match: Match, key: str, runtime: Any, tools: Any) -> None:
            body = text.replace("{key}", key or "")
            runtime.submit(Event(
                conversation_id=key,
                text=body,
                user_id="cep",
                metadata={DERIVED: "true"},
            ))

        return submit
    raise ValueError(f"unknown cep on_match kind: {kind!r}")


def _meta(event: Event, field: str) -> Optional[str]:
    if not event.metadata:
        return None
    return event.metadata.get(field)


def _as_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if value is not None:
        return [str(value)]
    return []

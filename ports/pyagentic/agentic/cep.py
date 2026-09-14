"""Sequence patterns (the ``cep:`` section of an ``agentic/v1`` workflow) as a pure fold over
the conversation log.

A pattern is evaluated on every turn after ``routed`` and before the brain, on the list of
``turn_received`` events of the conversation (the current turn last). Nothing about a partial
match is stored anywhere: replaying the log reproduces every decision. This mirrors the
reference runtime's ``_matches_on_last_turn``:

* stages match in declaration order against the turn text (``where.text_contains``,
  case-insensitive); a stage without ``where`` matches every turn;
* ``contiguity: next`` (the default) requires the turn right after the previous stage's turn;
  a non-matching turn drops the partial match and is consumed; ``followedBy`` skips it;
* ``within`` bounds the event-time span from the first matched turn, read from the metadata
  key named by ``ts``; a turn outside the window drops the partial match and may itself start
  a new one;
* a completed match consumes its turns, so matches never overlap.

The event time of a turn is read in exactly one place, :func:`event_time_ms`, so it can be
re-pointed at a shared logical clock later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .errors import ValidationError
from .events import Event

EVENT_TIME_KEY = "event_time_ms"
TOOL_KIND = "tool"


def event_time_ms(metadata: Optional[Mapping[str, Any]]) -> Optional[int]:
    """A turn's event time from its metadata (``event_time_ms``, a decimal string), or None."""
    raw = None if metadata is None else metadata.get(EVENT_TIME_KEY)
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except ValueError as exc:
        raise ValidationError(f"metadata.{EVENT_TIME_KEY} is not an integer: {raw!r}") from exc


@dataclass(frozen=True)
class Turn:
    """What the fold sees of one turn: the ``turn_received`` payload plus its metadata."""

    turn_id: str
    text: str
    metadata: Mapping[str, str] = field(default_factory=dict)

    @staticmethod
    def from_event(event: Event) -> "Turn":
        payload = event.payload
        metadata: Dict[str, str] = {str(k): str(v) for k, v in dict(event.metadata or {}).items()}
        for k, v in dict(payload.get("metadata") or {}).items():
            metadata.setdefault(str(k), str(v))
        text = payload.get("text")
        return Turn(str(payload.get("turn_id")), "" if text is None else str(text), metadata)


@dataclass(frozen=True)
class Stage:
    name: str
    text_contains: Optional[str]
    contiguity: str  # "next" | "followedBy"

    def matches(self, text: str) -> bool:
        return self.text_contains is None or self.text_contains.lower() in (text or "").lower()


@dataclass(frozen=True)
class SequencePattern:
    name: str
    stages: Tuple[Stage, ...]
    tool: str
    ts_key: Optional[str] = None
    within_ms: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.stages:
            raise ValidationError(f"cep pattern {self.name} needs at least one stage", f"/cep/{self.name}/pattern")
        if self.within_ms is not None and self.ts_key is None:
            raise ValidationError(f"cep pattern {self.name} sets within without ts", f"/cep/{self.name}/within")

    @staticmethod
    def from_spec(spec: Mapping[str, Any]) -> "SequencePattern":
        name = str(spec.get("name", "cep"))
        key = spec.get("key", "conversation_id")
        if key != "conversation_id":
            raise ValidationError(f"cep pattern {name} is keyed by {key!r}; sequence patterns are keyed by "
                                  f"conversation_id", f"/cep/{name}/key")
        ts_key: Optional[str] = None
        if spec.get("ts") is not None:
            scope, _, ts_key = str(spec["ts"]).partition(".")
            if scope != "metadata" or not ts_key:
                raise ValidationError(f"cep ts must be metadata.<key>, got {spec['ts']}", f"/cep/{name}/ts")
        stages = tuple(
            Stage(str(st.get("stage", f"s{i}")),
                  None if (st.get("where") or {}).get("text_contains") is None
                  else str(st["where"]["text_contains"]),
                  str(st.get("contiguity", "next")))
            for i, st in enumerate(spec.get("pattern") or []))
        on_match = spec.get("on_match") or {}
        if "tool" not in on_match:
            raise ValidationError(f"cep pattern {name} has on_match.kind tool without a tool id",
                                  f"/cep/{name}/on_match/tool")
        within = spec.get("within")
        return SequencePattern(name, stages, str(on_match["tool"]), ts_key,
                               None if within is None else int(within))

    def timestamp(self, turn: Turn) -> int:
        """The event time of ``turn`` under this pattern's ``ts``."""
        assert self.ts_key is not None
        raw = turn.metadata.get(self.ts_key)
        if raw is None:
            raise ValidationError(f"turn {turn.turn_id} lacks metadata.{self.ts_key} needed by cep pattern "
                                  f"{self.name}")
        try:
            return int(str(raw).strip())
        except ValueError as exc:
            raise ValidationError(f"turn {turn.turn_id} metadata.{self.ts_key} is not an integer: {raw!r}") from exc

    def completes_on(self, turns: Sequence[Turn]) -> bool:
        """The fold: whether a match completes on the last of ``turns`` (log order, current last)."""
        matched_at = -1
        stage_index = 0
        start_ts = 0
        for i, turn in enumerate(turns):
            if stage_index > 0 and self.within_ms is not None and self.timestamp(turn) - start_ts > self.within_ms:
                stage_index = 0
            stage = self.stages[stage_index]
            if stage.matches(turn.text):
                if stage_index == 0 and self.ts_key is not None:
                    start_ts = self.timestamp(turn)
                stage_index += 1
                if stage_index == len(self.stages):
                    matched_at = i
                    stage_index = 0
            elif stage_index > 0 and stage.contiguity == "next":
                stage_index = 0
        return bool(turns) and matched_at == len(turns) - 1

    def match_args(self, conversation_id: str) -> Dict[str, Any]:
        """The ``on_match.kind: tool`` arguments: the pattern name and the conversation key."""
        return {"pattern": self.name, "key": conversation_id}


def _has_tool_action(spec: Mapping[str, Any]) -> bool:
    return (spec.get("on_match") or {}).get("kind") == TOOL_KIND


def compile_patterns(specs: Optional[Sequence[Mapping[str, Any]]]) -> List[SequencePattern]:
    """The ``cep:`` entries evaluated in-turn: those with ``on_match.kind: tool``."""
    return [SequencePattern.from_spec(s) for s in specs or [] if _has_tool_action(s)]


def without_tool_actions(specs: Optional[Sequence[Mapping[str, Any]]]) -> List[Mapping[str, Any]]:
    """The ``cep:`` entries :func:`compile_patterns` leaves to stream-level wiring."""
    return [s for s in specs or [] if not _has_tool_action(s)]


def turns_of(log: Sequence[Event]) -> List[Turn]:
    """The conversation's turns in log order, as the fold sees them."""
    return [Turn.from_event(e) for e in log if e.type == "turn_received"]

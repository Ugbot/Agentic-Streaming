"""The effect journal (`spec/v1/primitives.md`, section 10): the record of every tool and model
call, kept outside the log so that what already happened is reused rather than done again.

`record_or_get` is the only way a tool or a model is reached. A store supplies a durable keyed
upsert and nothing else: an entry is written whole, and the last write for a key wins.

A journal gives reuse of recorded outcomes. It does not make an outside system transactional: a
`non_idempotent` call that left no answer is `unknown`, and only a person can say what happened.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Mapping, NamedTuple, Optional, Tuple

from .errors import AgenticError, LogUnavailable, OutcomeUnknown, ToolError
from .events import _acquire_writer_lock, _sync_directory

LOG = logging.getLogger(__name__)

TOOL = "tool"
LLM = "llm"

PENDING = "pending"
SUCCEEDED = "succeeded"
FAILED = "failed"
UNKNOWN = "unknown"

READ = "read"
IDEMPOTENT = "idempotent"
NON_IDEMPOTENT = "non_idempotent"


class JournalKey(NamedTuple):
    """The identity of one call, derived from recorded input alone so a re-run regenerates it."""

    conversation_id: str
    turn_id: str
    kind: str
    index: int


@dataclass(frozen=True)
class Attempt:
    """One failed attempt at a call."""

    attempt: int
    error: str


@dataclass(frozen=True)
class Entry:
    """One call. `intent` is what the call was, as `intent_of` and `model_intent_of` write it.
    `released` means a person said an unknown call did not land and may be made once more. `error`
    is the final error of a failed call, and why the outcome is not known of an unknown one."""

    key: JournalKey
    intent: str
    effect: str
    state: str = PENDING
    attempts: Tuple[Attempt, ...] = ()
    result: Any = None
    error: Optional[str] = None
    released: bool = False

    @classmethod
    def pending(cls, key: JournalKey, intent: str, effect: str) -> "Entry":
        return cls(key, intent, effect)

    def succeeded(self, result: Any) -> "Entry":
        return dataclasses.replace(self, state=SUCCEEDED, result=result, error=None, released=False)

    def failed(self, error: str) -> "Entry":
        return dataclasses.replace(self, state=FAILED, result=None, error=error, released=False)

    def unknown(self, reason: str) -> "Entry":
        return dataclasses.replace(self, state=UNKNOWN, result=None, error=reason, released=False)

    def release(self) -> "Entry":
        """A person's decision that the call may be made again, under the same key."""
        return dataclasses.replace(self, state=PENDING, result=None, error=None, released=True)

    def with_attempt(self, attempt: int, error: str) -> "Entry":
        return dataclasses.replace(self, attempts=self.attempts + (Attempt(attempt, error),))

    @property
    def is_final(self) -> bool:
        return self.state in (SUCCEEDED, FAILED)


class JournalDivergence(AgenticError):
    """A re-run asked for a different call under a recorded key: the turn did not replay, for
    instance because the workflow changed under it. Nothing is called. Error class `fatal`."""

    def __init__(self, key: JournalKey, recorded: str, asked: Optional[str]) -> None:
        super().__init__(f"call {key.index} of turn {key.turn_id} was recorded as {recorded} but this "
                         + (f"run asked for {asked}" if asked else "run ended before it"))


class EffectCall(ABC):
    """One way of reaching the outside world, driven by `record_or_get`."""

    def on_intent(self, entry: Entry) -> None:
        """The durable intent exists. Runs before any attempt, on every run of the turn."""

    def on_failed_attempt(self, attempt: Attempt) -> None:
        """A failed attempt, either replayed from the journal or just made."""

    def budget(self) -> int:
        """The number of attempts the call may use in all, counting those already recorded."""
        return 1

    @abstractmethod
    def attempt(self, attempt: int) -> Any:
        """Make attempt number `attempt`. The retry back-off is the caller's business."""

    def after_attempt(self) -> None:
        """After an attempt returned and before its outcome is journaled."""

    def after_record(self) -> None:
        """After the outcome is durable and before it is logged."""


class EffectJournal(ABC):
    """A keyed store of entries. `put` and `put_if_absent` must not return before the write is
    durable, and a store that cannot be reached raises `LogUnavailable`, which stops the call."""

    @abstractmethod
    def get(self, key: JournalKey) -> Optional[Entry]: ...

    @abstractmethod
    def put(self, entry: Entry) -> None:
        """Durable upsert of the whole entry."""

    @abstractmethod
    def put_if_absent(self, entry: Entry) -> Optional[Entry]:
        """Durably write `entry` unless the key is taken, and return what was there, if anything."""

    def close(self) -> None:
        """Release whatever the store holds. It must not be used afterwards."""

    def record_or_get(self, key: JournalKey, intent: str, effect: str, call: EffectCall) -> Entry:
        """Section 10. The returned entry is `succeeded`, `failed` or `unknown`. Nothing is called
        when an outcome is recorded. A `pending` entry means the previous run stopped without an
        answer: `read` and `idempotent` calls carry on at their next attempt under the same key, and
        a `non_idempotent` call becomes `unknown`, unless a person released it.

        A model call is recorded when it answers, with no `pending`, since asking again costs money
        but changes nothing."""
        if key.kind == LLM:
            return self._answer_or_get(key, intent, effect, call)
        fresh = Entry.pending(key, intent, effect)
        existing = self.put_if_absent(fresh)
        entry = existing if existing is not None else fresh
        if entry.intent != intent:
            raise JournalDivergence(key, entry.intent, intent)
        call.on_intent(entry)
        for failed in entry.attempts:
            call.on_failed_attempt(failed)
        if entry.is_final or entry.state == UNKNOWN:
            return entry
        first = len(entry.attempts) + 1
        budget = call.budget()
        once = NON_IDEMPOTENT in (effect, entry.effect)  # what a run recorded outranks a later declaration
        if once:
            budget = first
            if existing is not None and not entry.released:
                entry = entry.unknown("no recorded result")
                self.put(entry)
                return entry
            if entry.released:  # one release buys one call, so it is spent before the call is made
                entry = dataclasses.replace(entry, released=False)
                self.put(entry)
        for attempt in range(first, budget + 1):
            try:
                result = call.attempt(attempt)
            except ToolError as exc:
                if isinstance(exc, OutcomeUnknown) and once:
                    entry = entry.unknown(str(exc))
                    self.put(entry)
                    return entry
                entry = entry.with_attempt(attempt, str(exc))
                if attempt >= budget:
                    entry = entry.failed(str(exc))
                self.put(entry)
                call.on_failed_attempt(entry.attempts[-1])
                continue
            call.after_attempt()
            entry = entry.succeeded(result)
            self.put(entry)
            call.after_record()
            return entry
        if not entry.is_final:  # the budget shrank under a recorded call
            entry = entry.failed(entry.attempts[-1].error if entry.attempts else "no attempts left")
            self.put(entry)
        return entry

    def _answer_or_get(self, key: JournalKey, intent: str, effect: str, call: EffectCall) -> Entry:
        entry = self.get(key)
        if entry is None:
            entry = Entry.pending(key, intent, effect).succeeded(call.attempt(1))
            self.put(entry)
        elif entry.intent != intent:
            raise JournalDivergence(key, entry.intent, intent)
        return entry


def _digest(scope: str, body: str) -> str:
    return hashlib.sha256(f"{scope}\0{body}".encode("utf-8")).hexdigest()


def intent_of(tool_id: str, args: Mapping[str, Any], scope: str = "") -> str:
    """The tool, then a hash of the workflow and its arguments: what a re-run must ask for again
    under the same key. `scope` is the agent id, so a store two workflows share cannot answer one
    with the other's call."""
    return f"{tool_id}:{_digest(scope, json.dumps(args, sort_keys=True, default=str))}"


def model_intent_of(scope: str, request: str) -> str:
    """As `intent_of` for a model call. `request` is what identifies the call across runs, the
    brain's instructions and the turn's text. The transcript is not part of it, since a log that
    kept part of the turn shows more of it than the first run saw."""
    return f"llm:{_digest(scope, request)}"


class InMemoryEffectJournal(EffectJournal):
    """Process-lifetime entries. Like `InMemoryEventLog` it outlives a runtime restart in the same
    process, which is what the fixtures need, but it is not durability across a crash."""

    def __init__(self) -> None:
        self._entries: Dict[JournalKey, Entry] = {}
        self._guard = threading.Lock()

    def get(self, key: JournalKey) -> Optional[Entry]:
        with self._guard:
            return self._entries.get(key)

    def put(self, entry: Entry) -> None:
        with self._guard:
            self._entries[entry.key] = entry

    def put_if_absent(self, entry: Entry) -> Optional[Entry]:
        with self._guard:
            existing = self._entries.get(entry.key)
            if existing is None:
                self._entries[entry.key] = entry
            return existing


class FileEffectJournal(EffectJournal):
    """Whole-entry JSON lines in `directory`, one file, the last line for a key winning. Survives
    the process.

    A write syncs before it returns, so an acknowledged entry is on disk. A directory has one
    writer, as for `FileEventLog`: an OS lock on `.writer.lock` from construction to `close()`,
    dropped by the OS if the writer dies. Loading tolerates what a crash can leave, a last line with
    no newline, which was never acknowledged. Anything else that does not read back raises
    `LogUnavailable`: a journal that cannot be trusted must stop calls rather than repeat them.
    """

    FILE = "effects.jsonl"
    LOCK_FILE = ".writer.lock"

    def __init__(self, directory: str) -> None:
        self.directory = directory
        self._path = os.path.join(directory, self.FILE)
        try:
            os.makedirs(directory, exist_ok=True)
        except OSError as exc:
            raise LogUnavailable(f"cannot create {directory}: {exc}") from exc
        self._guard = threading.Lock()
        self._lock_fd: Optional[int] = _acquire_writer_lock(os.path.join(directory, self.LOCK_FILE))
        try:
            self._entries = self._load()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        with self._guard:
            if self._lock_fd is not None:
                os.close(self._lock_fd)  # closing the descriptor releases the lock
                self._lock_fd = None

    def _load(self) -> Dict[JournalKey, Entry]:
        try:
            with open(self._path, "rb") as fh:
                data = fh.read()
        except FileNotFoundError:
            return {}
        except OSError as exc:
            raise LogUnavailable(f"cannot read {self._path}: {exc}") from exc
        entries: Dict[JournalKey, Entry] = {}
        position = 0
        while position < len(data):
            newline = data.find(b"\n", position)
            if newline == -1:
                LOG.warning("%s: dropping a torn last line of %d bytes", self._path, len(data) - position)
                os.truncate(self._path, position)
                break
            line = data[position:newline]
            position = newline + 1
            if line.strip():
                entry = self._parse(line)
                entries[entry.key] = entry
        return entries

    def _parse(self, line: bytes) -> Entry:
        try:
            rec = json.loads(line)
            return Entry(JournalKey(rec["conversation_id"], rec["turn_id"], rec["kind"], rec["index"]),
                         rec["intent"], rec["effect"], rec["state"],
                         tuple(Attempt(a["attempt"], a["error"]) for a in rec["attempts"]),
                         rec.get("result"), rec.get("error"), bool(rec.get("released", False)))
        except (ValueError, KeyError, TypeError) as exc:
            raise LogUnavailable(f"unreadable record in {self._path}: {exc}") from exc

    @staticmethod
    def _record(entry: Entry) -> str:
        return json.dumps({**entry.key._asdict(), "intent": entry.intent, "effect": entry.effect,
                           "state": entry.state, "result": entry.result, "error": entry.error,
                           "attempts": [{"attempt": a.attempt, "error": a.error} for a in entry.attempts],
                           "released": entry.released}, sort_keys=True) + "\n"

    def _write(self, entry: Entry) -> None:
        line = self._record(entry).encode("utf-8")
        if self._lock_fd is None:
            raise LogUnavailable(f"{self.directory}: this journal is closed and no longer holds the writer lock")
        created = not os.path.exists(self._path)
        try:
            with open(self._path, "ab") as fh:
                start = fh.tell()
                try:
                    fh.write(line)
                    fh.flush()
                    os.fsync(fh.fileno())
                except OSError:
                    fh.truncate(start)  # never leave half a record for the next write to extend
                    raise
            if created:
                _sync_directory(self.directory)
        except OSError as exc:
            raise LogUnavailable(f"cannot write to {self._path}: {exc}") from exc
        self._entries[entry.key] = entry

    def get(self, key: JournalKey) -> Optional[Entry]:
        with self._guard:
            return self._entries.get(key)

    def put(self, entry: Entry) -> None:
        with self._guard:
            self._write(entry)

    def put_if_absent(self, entry: Entry) -> Optional[Entry]:
        with self._guard:
            existing = self._entries.get(entry.key)
            if existing is None:
                self._write(entry)
            return existing

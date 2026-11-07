"""The effect journal of `spec/v1/primitives.md` section 10, on its own: what `record_or_get`
promises and what each store keeps."""

from __future__ import annotations

import json
import os
from typing import Any, Callable, List, Optional

import pytest

from agentic.errors import LogUnavailable, OutcomeUnknown, ToolError
from agentic.journal import (
    FAILED,
    LLM,
    PENDING,
    SUCCEEDED,
    TOOL,
    UNKNOWN,
    Attempt,
    EffectCall,
    EffectJournal,
    Entry,
    FileEffectJournal,
    InMemoryEffectJournal,
    JournalDivergence,
    JournalKey,
    intent_of,
)
from agentic.tools import ToolCall, idempotency_key

KEY = JournalKey("c1", "t1", TOOL, 0)


class Crash(BaseException):
    pass


class Scripted(EffectCall):
    """Answers each attempt from `outcomes`: an exception is raised, anything else returned."""

    def __init__(self, *outcomes: Any, budget: int = 1, look: Optional[Callable[[], None]] = None) -> None:
        self.outcomes = list(outcomes)
        self.allowed = budget
        self.look = look
        self.made: List[int] = []
        self.intents = 0
        self.failures: List[Attempt] = []

    def on_intent(self, entry: Entry) -> None:
        self.intents += 1

    def on_failed_attempt(self, attempt: Attempt) -> None:
        self.failures.append(attempt)

    def budget(self) -> int:
        return self.allowed

    def attempt(self, attempt: int) -> Any:
        self.made.append(attempt)
        if self.look is not None:
            self.look()
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@pytest.fixture(params=["memory", "file"])
def journal(request: Any, tmp_path: Any) -> Any:
    store: EffectJournal = InMemoryEffectJournal() if request.param == "memory" else FileEffectJournal(str(tmp_path))
    yield store
    store.close()


def test_the_key_is_the_one_the_spec_defines():
    assert idempotency_key("c1", "t1", 0) == "4dce4871000d3c792626309090b13fe70a301367eb3686841299da15556a6340"
    assert ToolCall("c1", "t1", 0).idempotency_key == idempotency_key("c1", "t1", 0)
    assert idempotency_key("c1", "t1", 1) != idempotency_key("c1", "t1", 0)


def test_pending_is_on_the_store_before_the_call(journal):
    seen: List[str] = []
    call = Scripted("done", look=lambda: seen.append(journal.get(KEY).state))
    entry = journal.record_or_get(KEY, "pay:x", "idempotent", call)
    assert seen == [PENDING]
    assert (entry.state, entry.result, call.intents) == (SUCCEEDED, "done", 1)
    assert journal.get(KEY).state == SUCCEEDED


def test_a_final_state_is_reused_and_nothing_is_called(journal):
    journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted("done"))
    again = Scripted()
    entry = journal.record_or_get(KEY, "pay:x", "non_idempotent", again)
    assert (entry.state, entry.result, again.made, again.intents) == (SUCCEEDED, "done", [], 1)


def test_a_failed_outcome_is_final_and_its_attempts_replay(journal):
    first = Scripted(ToolError("a"), ToolError("b"), budget=2)
    entry = journal.record_or_get(KEY, "pay:x", "idempotent", first)
    assert (entry.state, entry.error, first.made) == (FAILED, "b", [1, 2])
    again = Scripted(budget=2)
    assert journal.record_or_get(KEY, "pay:x", "idempotent", again).state == FAILED
    assert (again.made, again.failures) == ([], [Attempt(1, "a"), Attempt(2, "b")])


def test_pending_non_idempotent_is_unknown_and_never_called(journal):
    with pytest.raises(Crash):
        journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted(Crash()))
    again = Scripted("must not run")
    entry = journal.record_or_get(KEY, "pay:x", "non_idempotent", again)
    assert (entry.state, again.made) == (UNKNOWN, [])
    assert journal.get(KEY).state == UNKNOWN
    third = Scripted("must not run")
    assert journal.record_or_get(KEY, "pay:x", "non_idempotent", third).state == UNKNOWN
    assert third.made == []


def test_a_release_buys_one_call_and_no_more(journal):
    with pytest.raises(Crash):
        journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted(Crash()))
    journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted())
    journal.put(journal.get(KEY).release())
    with pytest.raises(Crash):  # the call a person approved dies too
        journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted(Crash()))
    again = Scripted("must not run")
    assert journal.record_or_get(KEY, "pay:x", "non_idempotent", again).state == UNKNOWN
    assert again.made == []


def test_a_released_call_runs_under_the_same_key(journal):
    with pytest.raises(Crash):
        journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted(Crash()))
    journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted())
    journal.put(journal.get(KEY).release())
    entry = journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted("landed"))
    assert (entry.state, entry.result) == (SUCCEEDED, "landed")


@pytest.mark.parametrize("effect", ["read", "idempotent"])
def test_pending_read_or_idempotent_carries_on_at_the_next_attempt(journal, effect):
    with pytest.raises(Crash):
        journal.record_or_get(KEY, "get:x", effect, Scripted(ToolError("first"), Crash(), budget=3))
    again = Scripted("fine", budget=3)
    entry = journal.record_or_get(KEY, "get:x", effect, again)
    assert again.made == [2]
    assert again.failures == [Attempt(1, "first")]
    assert (entry.state, entry.result) == (SUCCEEDED, "fine")


def test_non_idempotent_gets_one_attempt_whatever_the_budget(journal):
    call = Scripted(ToolError("no"), "must not run", budget=5)
    entry = journal.record_or_get(KEY, "pay:x", "non_idempotent", call)
    assert (entry.state, call.made) == (FAILED, [1])


def test_an_outcome_without_an_answer_is_unknown_only_for_non_idempotent(journal):
    entry = journal.record_or_get(KEY, "pay:x", "non_idempotent", Scripted(OutcomeUnknown("timed out")))
    assert (entry.state, entry.error) == (UNKNOWN, "timed out")
    other = JournalKey("c1", "t1", TOOL, 1)
    call = Scripted(OutcomeUnknown("timed out"), "fine", budget=2)
    assert journal.record_or_get(other, "get:x", "idempotent", call).state == SUCCEEDED
    assert call.failures == [Attempt(1, "timed out")]


def test_a_different_intent_under_a_recorded_key_diverges_and_calls_nothing(journal):
    journal.record_or_get(KEY, "pay:x", "idempotent", Scripted("done"))
    again = Scripted("must not run")
    with pytest.raises(JournalDivergence) as raised:
        journal.record_or_get(KEY, "pay:y", "idempotent", again)
    assert raised.value.error_class == "fatal"
    assert (again.made, again.intents) == ([], 0)


def test_a_model_answer_is_recorded_once_and_no_pending_is_written(journal):
    key = JournalKey("c1", "t1", LLM, 0)
    states: List[Any] = []
    call = Scripted({"text": "hi"}, look=lambda: states.append(journal.get(key)))
    entry = journal.record_or_get(key, "", "read", call)
    assert states == [None]
    assert (entry.state, entry.result) == (SUCCEEDED, {"text": "hi"})
    again = Scripted()
    assert journal.record_or_get(key, "", "read", again).result == {"text": "hi"}
    assert again.made == []


def test_intent_is_stable_across_key_order():
    assert intent_of("pay", {"a": 1, "b": 2}) == intent_of("pay", {"b": 2, "a": 1})
    assert intent_of("pay", {"a": 1}) != intent_of("pay", {"a": 2})
    assert intent_of("pay", {"a": 1}).startswith("pay:")


# -- the file store ----------------------------------------------------------------

def entry_of(state_result: str, index: int = 0) -> Entry:
    return Entry.pending(JournalKey("c1", "t1", TOOL, index), "pay:x", "idempotent").succeeded(state_result)


def journal_file(directory: Any) -> str:
    return os.path.join(str(directory), FileEffectJournal.FILE)


def test_file_entries_survive_a_reopen_and_the_last_per_key_wins(tmp_path):
    store = FileEffectJournal(str(tmp_path))
    store.put(Entry.pending(KEY, "pay:x", "idempotent").with_attempt(1, "no"))
    store.put(store.get(KEY).succeeded({"id": 7}))
    store.close()
    with open(journal_file(tmp_path), encoding="utf-8") as fh:
        assert len(fh.readlines()) == 2  # whole-entry records, appended
    store = FileEffectJournal(str(tmp_path))
    entry = store.get(KEY)
    store.close()
    assert (entry.state, entry.result, entry.attempts) == (SUCCEEDED, {"id": 7}, (Attempt(1, "no"),))
    assert entry.key == KEY


def test_file_put_syncs_before_it_returns(tmp_path, monkeypatch):
    store = FileEffectJournal(str(tmp_path))
    synced: List[int] = []
    real = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (synced.append(fd), real(fd))[1])
    store.put(entry_of("x"))
    store.close()
    assert synced


def test_file_drops_a_torn_tail_and_keeps_what_was_acknowledged(tmp_path):
    store = FileEffectJournal(str(tmp_path))
    store.put(entry_of("kept"))
    store.close()
    with open(journal_file(tmp_path), "ab") as fh:
        fh.write(b'{"conversation_id": "c1", "tur')
    store = FileEffectJournal(str(tmp_path))
    assert store.get(KEY).result == "kept"
    store.put(entry_of("after", 1))
    store.close()
    store = FileEffectJournal(str(tmp_path))
    assert store.get(JournalKey("c1", "t1", TOOL, 1)).result == "after"
    store.close()
    with open(journal_file(tmp_path), encoding="utf-8") as fh:
        assert all(json.loads(line) for line in fh)


def test_file_refuses_a_record_a_crash_could_not_have_left(tmp_path):
    store = FileEffectJournal(str(tmp_path))
    store.put(entry_of("kept"))
    store.close()
    with open(journal_file(tmp_path), "ab") as fh:
        fh.write(b"not json\n")
    with pytest.raises(LogUnavailable):
        FileEffectJournal(str(tmp_path))


def test_file_has_one_writer_and_the_lock_goes_with_it(tmp_path):
    first = FileEffectJournal(str(tmp_path))
    with pytest.raises(LogUnavailable):
        FileEffectJournal(str(tmp_path))
    first.close()
    FileEffectJournal(str(tmp_path)).close()


def test_a_store_that_cannot_be_written_stops_the_call(tmp_path):
    store = FileEffectJournal(str(tmp_path))
    store.close()
    call = Scripted("must not run")
    with pytest.raises(LogUnavailable):
        store.record_or_get(KEY, "pay:x", "idempotent", call)
    assert call.made == []


def test_a_failing_disk_is_log_unavailable_and_leaves_no_half_record(tmp_path, monkeypatch):
    store = FileEffectJournal(str(tmp_path))
    store.put(entry_of("kept"))
    size = os.path.getsize(journal_file(tmp_path))

    def broken(fd: int) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "fsync", broken)
    call = Scripted("must not run")
    with pytest.raises(LogUnavailable):
        store.record_or_get(JournalKey("c1", "t1", TOOL, 1), "pay:x", "idempotent", call)
    monkeypatch.undo()
    assert call.made == []
    assert os.path.getsize(journal_file(tmp_path)) == size
    assert store.get(JournalKey("c1", "t1", TOOL, 1)) is None
    store.close()

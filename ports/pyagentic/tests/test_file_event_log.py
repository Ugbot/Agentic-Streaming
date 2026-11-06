"""FileEventLog durability: a torn tail, density, and one writer per directory (review H12)."""

from __future__ import annotations

import json
import multiprocessing
import os
import uuid

import pytest

from agentic.errors import CorruptLog, LogUnavailable
from agentic.events import Event, FileEventLog, Turn, reduce_state
from agentic.runtime import LocalRuntime

DOC = {
    "spec_version": "agentic/v1",
    "agent": {
        "id": "a",
        "router": {"kind": "static", "default": "p"},
        "verifier": {"kind": "none"},
        "paths": {"p": {"brain": "rule", "tool_triggers": {"pay": "charge"}}},
    },
    "tools": [{"id": "charge", "kind": "constant", "value": "ok"}],
}


def _lines(directory, conversation="c"):
    with open(os.path.join(directory, f"{conversation}.jsonl"), encoding="utf-8") as fh:
        return fh.readlines()


def _write_turn(directory, conversation="c", turn="t1"):
    runtime = LocalRuntime(store_dir=str(directory))
    runtime.deploy(DOC)
    result = runtime.submit(Turn(conversation, turn, "pay"))
    runtime.close()
    return result


def _tear_last_line(directory, conversation="c"):
    path = os.path.join(directory, f"{conversation}.jsonl")
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text[:-20])


def test_a_torn_last_line_is_dropped_and_the_runtime_deploys(tmp_path):
    _write_turn(tmp_path)
    complete = len(_lines(tmp_path))
    _tear_last_line(tmp_path)

    runtime = LocalRuntime(store_dir=str(tmp_path))
    runtime.deploy(DOC)
    assert runtime.state("c")["turn_count"] == 1
    assert len(_lines(tmp_path)) == complete - 1, "the torn bytes are gone from the file"
    runtime.close()


def test_appends_after_a_torn_tail_stay_dense_and_parse(tmp_path):
    _write_turn(tmp_path)
    _tear_last_line(tmp_path)
    runtime = LocalRuntime(store_dir=str(tmp_path))
    runtime.deploy(DOC)
    runtime.submit(Turn("c", "t2", "pay"))
    runtime.close()
    sequences = [json.loads(line)["sequence"] for line in _lines(tmp_path)]
    assert sequences == list(range(len(sequences)))


def test_a_record_cut_exactly_at_its_newline_was_never_acknowledged(tmp_path):
    _write_turn(tmp_path)
    path = tmp_path / "c.jsonl"
    data = path.read_bytes()
    path.write_bytes(data[:-1])
    log = FileEventLog(str(tmp_path))
    assert len(log.read("c")) == len(data.splitlines()) - 1
    log.close()


def test_a_bad_line_inside_the_file_is_corruption_not_a_torn_tail(tmp_path):
    _write_turn(tmp_path)
    lines = _lines(tmp_path)
    lines[1] = "{not json\n"
    (tmp_path / "c.jsonl").write_text("".join(lines), encoding="utf-8")
    log = FileEventLog(str(tmp_path))
    with pytest.raises(CorruptLog):
        log.read("c")
    log.close()


def test_a_gap_in_sequence_is_refused_on_load(tmp_path):
    _write_turn(tmp_path)
    lines = _lines(tmp_path)
    del lines[2]
    (tmp_path / "c.jsonl").write_text("".join(lines), encoding="utf-8")
    log = FileEventLog(str(tmp_path))
    with pytest.raises(CorruptLog, match="not dense"):
        log.read("c")
    log.close()


def test_the_fold_refuses_a_log_that_is_not_dense():
    events = [Event("c", "t", 0, "turn_received"), Event("c", "t", 2, "turn_completed")]
    with pytest.raises(CorruptLog, match="not dense"):
        reduce_state(events)


def test_one_corrupt_conversation_does_not_stop_the_others(tmp_path):
    _write_turn(tmp_path, "good")
    _write_turn(tmp_path, "bad")
    lines = _lines(tmp_path, "bad")
    lines[0] = "{not json\n"
    (tmp_path / "bad.jsonl").write_text("".join(lines), encoding="utf-8")

    runtime = LocalRuntime(store_dir=str(tmp_path))
    runtime.deploy(DOC)
    assert runtime.submit(Turn("good", "t1", "pay"))["status"] == "duplicate"
    failed = runtime.submit(Turn("bad", f"t-{uuid.uuid4()}", "pay"))
    assert failed["status"] == "failed"
    assert failed["error"]["class"] == "fatal"
    runtime.close()


def test_a_second_writer_on_a_directory_is_refused(tmp_path):
    first = FileEventLog(str(tmp_path))
    with pytest.raises(LogUnavailable, match="another writer"):
        FileEventLog(str(tmp_path))
    first.close()
    FileEventLog(str(tmp_path)).close()


def _hold_lock(directory, ready, release):
    log = FileEventLog(directory)
    ready.set()
    release.wait(30)
    log.close()


def test_a_writer_in_another_process_is_refused_and_a_dead_one_is_not(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    ready, release = ctx.Event(), ctx.Event()
    holder = ctx.Process(target=_hold_lock, args=(str(tmp_path), ready, release))
    holder.start()
    try:
        assert ready.wait(30)
        with pytest.raises(LogUnavailable, match="another writer"):
            FileEventLog(str(tmp_path))
    finally:
        holder.kill()
        holder.join(30)
    FileEventLog(str(tmp_path)).close()


def test_a_second_runtime_on_a_directory_cannot_interleave(tmp_path):
    """The review's p6.py: two instances wrote sequences ...10,11,6,7,8,9,10,11."""
    first = LocalRuntime(store_dir=str(tmp_path))
    first.deploy(DOC)
    first.submit(Turn("c", "t1", "pay"))
    second = LocalRuntime(store_dir=str(tmp_path))
    with pytest.raises(LogUnavailable):
        second.deploy(DOC)
    first.submit(Turn("c", "t2", "pay"))
    first.close()
    sequences = [json.loads(line)["sequence"] for line in _lines(tmp_path)]
    assert sequences == list(range(len(sequences)))

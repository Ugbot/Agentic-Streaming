"""A classifier guardrail in the core: the lexicon classifier, offline and deterministic. A turn whose top
label is blocked at or above the threshold is rejected like any guardrail."""

from __future__ import annotations

from typing import Any, Dict

import pytest

from agentic import Turn
from agentic.classifier import classify_lexicon
from agentic.errors import ValidationError
from agentic.runtime import LocalRuntime

LEXICON = {"toxic": ["idiot", "stupid", "hate", "moron"], "ok": ["please", "thanks", "help", "balance", "card"]}


def document(**rail: Any) -> Dict[str, Any]:
    guardrail = {"kind": "classifier", "classifier": "lexicon", "blocked": ["toxic"], "threshold": 0.3,
                 "reason": "abusive language", "lexicon": LEXICON, **rail}
    return {"agent": {"id": "a", "router": {"kind": "static", "default": "main"},
                      "paths": {"main": {"brain": "rule"}}},
            "guardrails": [guardrail]}


def submit(doc: Dict[str, Any], text: str) -> Dict[str, Any]:
    rt = LocalRuntime()
    rt.deploy(doc)
    return rt.submit(Turn("c1", "t1", text))


def test_the_lexicon_scores_the_share_of_hits():
    assert classify_lexicon(LEXICON, "you stupid idiot") == ("toxic", 1.0)
    label, score = classify_lexicon(LEXICON, "please help, you idiot")
    assert (label, round(score, 2)) == ("ok", 0.67)
    assert classify_lexicon(LEXICON, "nothing here") == (None, 0.0)


def test_abuse_is_rejected_and_on_the_log():
    result = submit(document(), "you stupid idiot")
    assert (result["status"], result["error"]["class"], result["error"]["message"]) == (
        "rejected", "guardrail", "abusive language")
    assert "guardrail_rejected" in [e["type"] for e in result["events"]]


def test_a_clean_turn_passes():
    assert submit(document(), "what is my balance please?")["status"] == "completed"


def test_below_the_threshold_passes():
    assert submit(document(threshold=0.9), "please help, you idiot")["status"] == "completed"


def test_an_output_stage_checks_the_reply():
    result = submit(document(stage="output"), "say stupid idiot")
    assert (result["status"], result["error"]["class"]) == ("rejected", "guardrail")


@pytest.mark.parametrize("rail", [{"classifier": "embedding"}, {"lexicon": None}])
def test_a_classifier_this_runtime_lacks_is_refused_at_deploy(rail):
    doc = document(**rail)
    if doc["guardrails"][0]["lexicon"] is None:
        del doc["guardrails"][0]["lexicon"]
    with pytest.raises(ValidationError, match="classifier"):
        LocalRuntime().deploy(doc)

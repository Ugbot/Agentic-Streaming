"""Tests for the engine adapters' portable logic — runnable WITHOUT any engine
installed (engine-specific wiring is import-guarded; the agent logic is the tested
pyagentic core). Where an engine is present (e.g. Dask in the dev venv), the Dask
adapter exercises the real engine; otherwise it uses the sequential fallback.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
for sub in ("pyagentic", "dask", "airflow", "faust", "ray"):
    sys.path.insert(0, str(_ROOT / sub))


def test_airflow_simulate_routes_correctly():
    import agentic_banking_dag as af

    assert af.simulate("what card types do you offer?")["path"] == "cards"
    assert af.simulate("dispute a charge")["path"] == "payments"
    assert af.simulate("hello there")["path"] == "general"
    assert af.classify("crypto cash-back") == "path_cards"


def test_dask_ingest_eval_and_replay():
    import agentic_dask as dk

    index = dk.ingest_corpus(dk.KB)
    assert index.size() == len(dk.KB)

    cases = [
        dk.EvalCase("what card types are available", "kb_cards_types"),
        dk.EvalCase("how do I dispute a charge", "kb_payments_dispute"),
    ]
    assert dk.eval_recall(index, cases, k=1) >= 0.5

    results = dk.replay_graph([("c0", "card types?"), ("c1", "what is my balance")])
    by_cid = {r["conversation_id"]: r["path"] for r in results}
    assert by_cid["c0"] == "cards" and by_cid["c1"] == "payments"


def test_faust_and_ray_adapters_import_without_engine():
    import agentic_faust as fa
    import agentic_ray as ra

    # Engine-guarded: importable for inspection even when the engine is absent.
    assert hasattr(fa, "FaustTableConversationStore")
    assert hasattr(ra, "RayRuntime")
    # When the engine isn't installed the guard nulls the handle (no crash on import).
    assert fa.faust is None or hasattr(fa.faust, "App")
    assert ra.ray is None or hasattr(ra.ray, "remote")

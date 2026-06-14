"""Tests for the engine adapters' portable logic — runnable WITHOUT any engine
installed (engine-specific wiring is import-guarded; the agent logic is the tested
pyagentic core). Where an engine is present (e.g. Dask in the dev venv), the Dask
adapter exercises the real engine; otherwise it uses the sequential fallback.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
for sub in ("pyagentic", "pyagentic/tests", "dask", "airflow", "faust", "ray", "celery", "nats"):
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
    # Injectable: a custom graph/tools can be configured into the Faust agent.
    assert callable(fa.configure)


def test_airflow_injectable_extended_graph():
    """An extended core graph (new fraud path + tool) flows through the Airflow DAG
    logic via configure() — proving the adapter is no longer hardcoded to banking."""
    import sys

    sys.path.insert(0, str(_ROOT / "airflow"))
    import agentic_banking_dag as af
    from test_extensibility import extended_graph, extended_tools

    af.configure(graph=extended_graph(), tools=extended_tools())
    try:
        res = af.simulate("my card was stolen, please freeze it")
        assert res["path"] == "fraud"
        assert "FRZ-" in res["reply"]
        # original routing still works through the same injected graph
        assert af.simulate("what is my balance?")["path"] == "payments"
    finally:
        af.configure()  # restore banking defaults


def test_celery_runtime_runs_banking_on_real_engine():
    """Celery 5.x runs here, so this exercises the real engine in eager mode (the task
    body runs in-process, no broker) — like the Dask test, not just an import guard."""
    import agentic_celery as cl
    from pyagentic.core import Event

    assert cl.Celery is not None, "celery should be installed in the test env"
    rt = cl.CeleryRuntime(eager=True)
    cards = rt.submit(Event("c1", "what card types do you offer?", "demo"))
    pay = rt.submit(Event("c2", "what is my balance?", "demo"))
    general = rt.submit(Event("c3", "where is the nearest branch?", "demo"))
    assert (cards.path, pay.path, general.path) == ("cards", "payments", "general")
    assert "get_balance" in pay.tool_calls
    # C2 seam: a conversation maps to a stable queue.
    assert cl.conversation_queue("c1") == cl.conversation_queue("c1")


def test_celery_propagates_an_extended_core_graph():
    """The decisive extensibility check at the adapter level: take an EXTENDED core
    graph (a new 'fraud' path + a new 'freeze_card' tool, built only from the public
    pyagentic abstractions), inject it, and run it through the *real Celery task seam*.
    The new path routes and the new tool fires — proving a core addition flows through
    the engine with no adapter change."""
    import agentic_celery as cl
    from pyagentic.core import Event
    from test_extensibility import extended_graph, extended_tools  # the shared extension

    cl.configure(graph=extended_graph(), tools=extended_tools(), retriever=None)
    try:
        rt = cl.CeleryRuntime(eager=True)
        res = rt.submit(Event("c-fraud", "my card was stolen, please freeze it", "alice"))
        assert res.path == "fraud"
        assert "freeze_card" in res.tool_calls
        assert "FRZ-alice" in res.reply
        # original paths still routable through the same injected graph
        assert rt.submit(Event("c-ok", "what is my balance?", "alice")).path == "payments"
    finally:
        cl.configure()  # restore the default banking deps for other tests


def test_nats_adapter_imports_without_engine():
    import agentic_nats as na

    assert hasattr(na, "NatsRuntime")
    # Engine-guarded: importable for inspection even when nats-py is absent.
    assert na.nats is None or hasattr(na.nats, "connect")


def test_nats_jetstream_kv_roundtrip_and_extension():
    """Runs against a live JetStream server when one is reachable (skips otherwise).
    Exercises the durable-KV state path AND an extended core graph through the same
    NATS seam — proving a core addition flows through with no adapter change.

    Everything runs in ONE event loop: a nats connection is bound to the loop it was
    created on, so connect + all ops must share a single ``asyncio.run``.
    """
    import asyncio
    import uuid

    import agentic_nats as na
    from pyagentic.core import Event
    from test_extensibility import extended_graph, extended_tools

    if na.nats is None:
        pytest.skip("nats-py not installed")

    # Unique ids per run — the JetStream KV bucket is durable, so fixed ids would
    # accumulate transcript across runs.
    suffix = uuid.uuid4().hex[:8]
    cid1, cid2, cidf = f"t1-{suffix}", f"t2-{suffix}", f"tf-{suffix}"

    async def _scenario():
        rt = na.NatsRuntime()
        try:
            await asyncio.wait_for(rt.connect(), timeout=2)
        except Exception as exc:  # no server on AGENTIC_NATS_URL
            return {"skip": f"no NATS JetStream server reachable: {exc}"}

        out = {}
        # 1) default banking essence: routing + durable KV state across turns
        r1 = await rt.submit(Event(cid1, "what card types do you offer?", "demo"))
        await rt.submit(Event(cid1, "tell me about crypto cash-back", "demo"))
        r2 = await rt.submit(Event(cid2, "what is my balance?", "demo"))
        store, _o, _rev = await rt._load(cid1)
        out["r1_path"] = r1.path
        out["r2_path"], out["r2_tools"] = r2.path, r2.tool_calls
        out["t1_count"] = store.message_count(cid1)
        await rt.close()

        # 2) extended core graph (new path + tool) through the same NATS seam
        rt2 = na.NatsRuntime(graph=extended_graph(), tools=extended_tools())
        await rt2.connect()
        res = await rt2.submit(Event(cidf, "my card was stolen, please freeze it", "alice"))
        out["fraud_path"], out["fraud_reply"], out["fraud_tools"] = res.path, res.reply, res.tool_calls
        await rt2.close()
        return out

    out = asyncio.run(_scenario())
    if "skip" in out:
        pytest.skip(out["skip"])

    assert out["r1_path"] == "cards"
    assert out["r2_path"] == "payments" and "get_balance" in out["r2_tools"]
    assert out["t1_count"] == 4  # two turns (user+assistant x2) durable in JetStream KV
    assert out["fraud_path"] == "fraud"
    assert "freeze_card" in out["fraud_tools"] and "FRZ-alice" in out["fraud_reply"]

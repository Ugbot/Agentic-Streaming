"""Cross-core parity guard (Python side).

The cores must behave identically: same FNV-1a hashing, same embedder vectors, same retrieval ranking,
same routing. The asserted constants are the shared golden values; the Java `ParityTest` asserts the
same numbers, so a divergence in either core is caught.
"""

from __future__ import annotations

import math
from pathlib import Path

import yaml

from agentic import Turn
from agentic.retrieval import KnowledgeBase, fnv1a_32, hashing_embed
from agentic.runtime import LocalRuntime

REPO = Path(__file__).resolve().parents[3]
PIPELINES = REPO / "examples" / "pipelines"

# Golden FNV-1a 32-bit hashes (unsigned), identical in every core.
GOLDEN_FNV = {"crypto": 1712156752, "balance": 2560266987, "card": 2284280159, "dispute": 3025431163}
# Buckets at the banking embedding dimension (256).
GOLDEN_BUCKET_256 = {"crypto": 80, "balance": 235, "card": 95, "dispute": 123}
# Routing is a pure keyword function, identical across cores.
GOLDEN_ROUTES = [
    ("what card types do you offer?", "cards"),
    ("what is my balance?", "payments"),
    ("how do I dispute a charge?", "payments"),
    ("hello there", "general"),
    ("tell me about crypto cash-back", "cards"),
]


def test_fnv1a_golden():
    for tok, want in GOLDEN_FNV.items():
        assert fnv1a_32(tok) == want, tok
    for tok, want in GOLDEN_BUCKET_256.items():
        assert fnv1a_32(tok) % 256 == want, tok


def test_embed_golden_vector():
    # "crypto" and "cash" land in distinct buckets; at dim 8 they are buckets 0 and 2.
    v = hashing_embed("crypto cash", 8)
    nonzero = sorted(i for i, x in enumerate(v) if x != 0.0)
    assert nonzero == [0, 2]
    for i in nonzero:
        assert abs(v[i] - 1.0 / math.sqrt(2)) < 1e-6


def test_retrieval_ranks_crypto_first():
    kb = yaml.safe_load((PIPELINES / "banking-rag.yaml").read_text())["retrieval"]["kb"]
    hits = KnowledgeBase(kb, 256, 4).retrieve("tell me about crypto cash-back redemption")
    assert hits[0].id == "kb_cards_crypto", [h.id for h in hits]


def test_routing_parity():
    rt = LocalRuntime()
    rt.deploy(yaml.safe_load((PIPELINES / "banking.yaml").read_text()))
    for i, (text, want) in enumerate(GOLDEN_ROUTES):
        assert rt.submit(Turn(f"c{i}", "t", text))["path"] == want, text

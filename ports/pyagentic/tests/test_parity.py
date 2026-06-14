"""Cross-core parity guard (Python side).

The Python, Java, and Go cores must behave identically: same FNV-1a hashing, same
embedder vectors, same retrieval ranking, same routing. The asserted constants here are
the shared "golden" values — the Java ``ParityTest`` and Go ``parity_test.go`` assert
the exact same numbers, so a divergence in any core is caught.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.banking import KB, build_banking_graph, default_tools, seed_kb  # noqa: E402
from pyagentic.core import Event  # noqa: E402
from pyagentic.retrieval import (  # noqa: E402
    InMemoryHotVectorIndex,
    TwoTierRetriever,
    fnv1a_32,
    hashing_embedder,
)
from pyagentic.runtime import LocalRuntime  # noqa: E402

# Golden FNV-1a 32-bit hashes (unsigned) — identical in all three cores.
GOLDEN_FNV = {"crypto": 1712156752, "balance": 2560266987, "card": 2284280159, "dispute": 3025431163}
# Buckets at the banking embedding dimension (256).
GOLDEN_BUCKET_256 = {"crypto": 80, "balance": 235, "card": 95, "dispute": 123}
# Routing is a pure keyword function — identical across cores.
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
    v = hashing_embedder(8)("crypto cash")
    nonzero = sorted(i for i, x in enumerate(v) if x != 0.0)
    assert nonzero == [0, 2]
    for i in nonzero:
        assert abs(v[i] - 1.0 / math.sqrt(2)) < 1e-6


def test_retrieval_ranks_crypto_first():
    embed = hashing_embedder(256)
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retr = TwoTierRetriever(hot, None, 4, 4)
    hits = retr.retrieve(embed("tell me about crypto cash-back redemption"), 4)
    assert hits[0].id == "kb_cards_crypto", [h.id for h in hits]
    assert hits[0].text == KB["kb_cards_crypto"]


def test_routing_parity():
    rt = LocalRuntime(build_banking_graph(), tools=default_tools())
    for text, want in GOLDEN_ROUTES:
        res = rt.submit(Event("c-" + text[:4], text, "demo"))
        assert res.path == want, (text, res.path)

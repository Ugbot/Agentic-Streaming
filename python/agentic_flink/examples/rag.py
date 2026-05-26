"""Sequential Python RAG: embed a small corpus, search, ask the LLM.

This example sidesteps Flink streaming entirely and uses the framework's
classes directly from Python. It seeds an HNSW corpus in-process, embeds
two questions, retrieves the top-k passages, and asks the LLM to answer
with citations.

For the full streaming version see ``examples.live_research``.

Prerequisites:

* ``mvn -DskipTests package`` (or ``AGENTIC_FLINK_JAR``).
* Ollama running with ``qwen2.5:3b`` pulled.
* A DJL native binary on the classpath (``pytorch-native-cpu`` is sufficient
  for the embedder and reranker used here). Pass it via ``extra_jars=`` to
  ``start_jvm`` or stage it onto the Maven build classpath.
"""

from __future__ import annotations

import os
import sys
from typing import Sequence

import agentic_flink as af


def main() -> int:
    af.start_jvm()

    from agentic_flink import (
        Agent,
        ChatSetup,
        langchain4j_ollama,
        chat,
        ChatMessage,
    )
    from agentic_flink.embedding import djl_embedding, EmbeddingSetup
    from agentic_flink.memory import flink_state_hnsw

    # ── 1. Build an embedder + a vector-memory spec ──────────────────────
    embeddings = djl_embedding(
        "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2"
    )
    vec_spec = flink_state_hnsw(dimension=384)

    # We can't run Flink-keyed-state operators standalone, so we exercise the
    # raw embedder + Java in-process classes for this demo and skip the
    # corpus wrapper. The recipe for the full job graph lives in
    # examples/live_research.py.
    embedder = embeddings.bind(None)
    setup = EmbeddingSetup(model="MiniLM-L6-v2", dimension=384)._to_java()

    corpus = _build_brute_force_kb(embedder, setup)

    # ── 2. Ask two questions ────────────────────────────────────────────
    questions: Sequence[str] = (
        "How does Flink guarantee exactly-once state?",
        "What is a guardrail?",
    )

    chat_setup = ChatSetup(model="qwen2.5:3b", temperature=0.2)
    chat_conn = langchain4j_ollama()

    for q in questions:
        q_vec = embedder.embed(q, setup)
        hits = _top_k(q_vec, corpus, k=3)
        context = "\n".join(f"[{i + 1}] {h['text']}" for i, h in enumerate(hits))
        response = chat(
            chat_conn,
            [
                ChatMessage(
                    role="system",
                    content=(
                        "Answer using ONLY the numbered sources below. "
                        "Cite them inline as [1], [2], etc."
                    ),
                ),
                ChatMessage(role="user", content=f"Sources:\n{context}\nQuestion: {q}"),
            ],
            chat_setup,
        )
        print(f"\nQ: {q}\nA: {response['text']}")
        print(f"   tokens={response['tokens_used']}, finish={response['finish_reason']}")
    return 0


def _build_brute_force_kb(embedder, setup):
    """Embed a tiny in-process knowledge base and store it as a Python list
    of ``{id, text, vec}`` records — brute-force KNN for the demo."""
    import numpy as np  # noqa: F401  -- not required; left for users extending the demo

    docs = [
        ("flink-1", "Apache Flink is a stream-processing framework with exactly-once state guarantees."),
        ("flink-2", "Flink's RocksDB state backend supports incremental checkpoints to S3 or HDFS."),
        ("flink-3", "Flink CEP matches event patterns within a DataStream and supports time windows."),
        ("ag-1", "ReAct agents alternate between thought, action, and observation steps."),
        ("ag-2", "Guardrails are pre/post-LLM classifiers that can block or rewrite a call."),
    ]
    kb = []
    for doc_id, text in docs:
        kb.append({"id": doc_id, "text": text, "vec": embedder.embed(text, setup)})
    return kb


def _top_k(query_vec, kb, k: int = 3):
    """Brute-force cosine similarity over the in-process KB."""

    def dot(a, b):
        return sum(float(a[i]) * float(b[i]) for i in range(len(a)))

    def norm(a):
        return (sum(float(x) * float(x) for x in a)) ** 0.5

    qn = norm(query_vec)
    scored = []
    for entry in kb:
        en = norm(entry["vec"])
        cos = 0.0 if qn == 0 or en == 0 else dot(query_vec, entry["vec"]) / (qn * en)
        scored.append({"id": entry["id"], "text": entry["text"], "score": cos})
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:k]


if __name__ == "__main__":
    sys.exit(main())

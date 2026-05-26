"""PyFlink-native live-research demo.

Mirrors the Java ``RagResearchExample`` end-to-end: an agent that classifies
incoming research queries by topic, looks up a (stub) corpus, and emits a
summarized answer object. All decision-making is Python; the chat connection,
embedder, and corpus references resolve to the framework's Java SPIs by FQN.

Run::

    flink run -py path/to/pyflink_research.py
"""

from __future__ import annotations

from agentic_flink.pyflink import (
    Agent,
    ResourceRef,
    action,
    environment,
    tool,
)


def _stub_corpus_lookup(topic: str, query: str) -> list[dict]:
    """Stand-in for a real ``Corpus`` call — the production version would
    inject a corpus ResourceRef and the operator would expose it on ``ctx``."""
    samples = {
        "ml": [
            {"doc": "BERT-style encoders dominate retrieval relevance benchmarks."},
            {"doc": "HNSW gives sub-millisecond KNN over up to ~10^5 vectors/key."},
        ],
        "systems": [
            {"doc": "Flink keyed state replicates with the operator; checkpoints capture it."},
        ],
        "general": [{"doc": "Search the index; rank by recency * relevance."}],
    }
    return samples.get(topic, samples["general"])


class ResearchAgent(Agent):
    agent_id = "research-bot"
    system_prompt = (
        "You answer research questions. Cite each document you draw from."
    )
    chat_setup = {"model": "qwen2.5:3b", "temperature": "0.4"}
    chat_connection = ResourceRef(
        "org.agentic.flink.llm.langchain4j.LangChain4jChatConnection",
        {"provider": "OLLAMA", "base_url": "http://localhost:11434"},
    )
    # Embedder reference — resolved by Java; Python sees only the dict on ctx.
    resources = {
        "embedder": ResourceRef(
            "org.agentic.flink.inference.djl.DjlEmbeddingConnection",
            {"model_uri": "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2"},
        ),
    }

    @tool
    def classify_topic(self, query: str) -> str:
        low = query.lower()
        if any(k in low for k in ("model", "encoder", "embed", "rerank")):
            return "ml"
        if any(k in low for k in ("flink", "checkpoint", "state", "operator")):
            return "systems"
        return "general"

    @action("research-query")
    def answer(self, event, ctx):
        topic = self.classify_topic(event["query"])
        hits = _stub_corpus_lookup(topic, event["query"])
        return {
            "id": event["id"],
            "topic": topic,
            "n_hits": len(hits),
            "summary": " | ".join(h["doc"] for h in hits),
            "agent": ctx.get("agent_id"),
        }


def main():
    from pyflink.datastream import StreamExecutionEnvironment

    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)

    queries = s_env.from_collection(
        [
            {"type": "research-query", "id": "q-1", "query": "BERT encoder relevance"},
            {"type": "research-query", "id": "q-2", "query": "Flink keyed state checkpoint"},
            {"type": "research-query", "id": "q-3", "query": "What is RAG?"},
        ]
    )

    ae = environment(s_env)
    out = (
        ae.from_datastream(queries, key_selector=lambda q: q["id"])
        .apply(ResearchAgent())
        .to_datastream()
    )
    out.print()
    s_env.execute("research-bot")


if __name__ == "__main__":
    main()

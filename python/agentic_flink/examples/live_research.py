"""Live research + RAG, PyFlink edition.

This example demonstrates the **PyFlink integration path** — same job
graph as the Java :class:`org.agentic.flink.example.research.LiveResearchExample`,
but the assembly happens in Python and the Java operators run inside the
PyFlink JVM. The framework's spec classes and pipeline builders ship in the
job graph; Python is the driver.

Prerequisites:

* ``mvn -DskipTests package`` (or ``AGENTIC_FLINK_JAR``).
* Ollama with ``qwen2.5:3b`` pulled.
* A DJL PyTorch native binary on the runtime classpath.
* ``pip install apache-flink>=1.20``.

Run::

    python -m agentic_flink.examples.live_research

This example is intentionally light on Python-side state: every transform
is a Java operator. Adapt the channels list (add Kafka, webhook, etc.) to
drive ingest from external producers.
"""

from __future__ import annotations

import sys

import agentic_flink as af


def main() -> int:
    af.start_jvm()

    # PyFlink's environment uses the same JVM JPype started.
    from agentic_flink.channel import (
        static_seed,
        tool_invocation_side_output,
    )
    from agentic_flink.corpus import broadcast
    from agentic_flink.embedding import djl_embedding
    from agentic_flink.ingest import recursive_chunker, pipeline_from as ingest_from
    from agentic_flink.inference import djl_classification, InferenceSetup
    from agentic_flink.memory import flink_state_hnsw
    from agentic_flink.retrieve import pipeline_from as retrieve_from
    from agentic_flink.web import crawler_core, options, url_request
    from agentic_flink.llm import langchain4j_ollama, ChatSetup

    StreamEnv = af.jclass(
        "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment"
    )
    UrlRequest = af.jclass("org.agentic.flink.web.UrlRequest")

    env = StreamEnv.getExecutionEnvironment()
    env.setParallelism(1)

    # ── Channels ───────────────────────────────────────────────────────
    seeds = static_seed(
        [
            url_request("https://en.wikipedia.org/wiki/Apache_Flink", "seed"),
            url_request("https://en.wikipedia.org/wiki/Vector_database", "seed"),
        ],
        java_type=UrlRequest,
    )
    agent_crawl = tool_invocation_side_output(
        "crawl-url",
        UrlRequest,
        lambda params: url_request(params["url"], "agent"),
    )
    queries = af.jclass("org.agentic.flink.channel.StaticSeedChannel")(
        _java_list(
            [
                "How does Flink guarantee exactly-once state?",
                "How do vector databases differ from traditional databases?",
            ]
        ),
        af.jclass("org.apache.flink.api.common.typeinfo.TypeInformation").of(
            af.jclass("java.lang.String")
        ),
    )

    # ── Corpus + connections ───────────────────────────────────────────
    corpus = broadcast("research-kb", flink_state_hnsw(dimension=384))
    embedder = djl_embedding(
        "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2"
    )
    reranker = djl_classification(
        "djl://ai.djl.huggingface.pytorch/cross-encoder/ms-marco-MiniLM-L-6-v2"
    )
    reranker_setup = InferenceSetup(
        model="ms-marco-MiniLM-L-6-v2", model_uri=reranker.getDefaultModelUri()
    )
    chat = langchain4j_ollama()
    chat_setup = ChatSetup(model="qwen2.5:3b", temperature=0.2)

    # ── Ingest pipeline ────────────────────────────────────────────────
    pages = crawler_core(seeds, agent_crawl, opts=options(max_depth=1)).open(env)
    ingest_from(pages).chunk(recursive_chunker(512)).embed(embedder).into(corpus).print().name("ingest")

    # ── Retrieve pipeline ──────────────────────────────────────────────
    retrieve_from(queries.open(env)) \
        .embed(embedder) \
        .search(corpus, 6) \
        .rerank(reranker, reranker_setup._to_java()) \
        .answer(chat, chat_setup._to_java()) \
        .print().name("answers")

    env.execute("live-research-python")
    return 0


def _java_list(items):
    JL = af.jclass("java.util.ArrayList")
    out = JL()
    for i in items:
        out.add(i)
    return out


if __name__ == "__main__":
    sys.exit(main())

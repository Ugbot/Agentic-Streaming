package org.agentic.flink.example.research;

import org.agentic.flink.channel.Channel;
import org.agentic.flink.channel.StaticSeedChannel;
import org.agentic.flink.channel.ToolInvocationChannel;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.corpus.BroadcastCorpus;
import org.agentic.flink.corpus.CorpusSpec;
import org.agentic.flink.embedding.djl.DjlEmbeddingConnection;
import org.agentic.flink.ingest.IngestionPipeline;
import org.agentic.flink.ingest.RecursiveTextChunker;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.djl.DjlInferenceConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import org.agentic.flink.memory.vector.FlinkStateHnswVectorMemory;
import org.agentic.flink.memory.vector.VectorMemorySpec;
import org.agentic.flink.retrieve.RetrievalPipeline;
import org.agentic.flink.web.CrawledPage;
import org.agentic.flink.web.CrawlerCore;
import org.agentic.flink.web.UrlRequest;
import org.agentic.flink.web.WebToolkitOptions;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Live research + RAG agent in a single Flink job.
 *
 * <p>Two inputs in one job:
 *
 * <ul>
 *   <li>A <b>URL channel</b> (seeds + LLM-driven {@code crawl-url} tool invocations) drives the
 *       {@link CrawlerCore} ingest pipeline: fetch → Tika/Jsoup extract → chunk → embed → upsert
 *       into the shared corpus.
 *   <li>A <b>query channel</b> drives the {@link RetrievalPipeline}: embed → search →
 *       (optional rerank) → LLM answer with citations.
 * </ul>
 *
 * <p>The corpus is a {@link BroadcastCorpus} over {@link FlinkStateHnswVectorMemory} — the HNSW
 * graph is rebuilt on operator restart from the underlying Flink {@code MapState}. No external
 * vector DB required to run the demo; swap to {@code ExternalCorpus.spec("pgvector", …)} for
 * production.
 *
 * <p>Every primitive on the right-hand side of every assignment is a framework class. This file
 * is the user-space glue: which channels feed which corpus, which corpus feeds which query
 * pipeline.
 *
 * <p><b>Prerequisites:</b>
 * <pre>
 *   docker compose up -d ollama
 *   docker compose exec ollama ollama pull qwen2.5:3b
 *
 *   # And add DJL's native PyTorch binary to your local pom:
 *   ai.djl.pytorch:pytorch-native-cpu:0.30.0
 * </pre>
 *
 * <p><b>To run:</b>
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.research.LiveResearchExample"
 * </pre>
 */
public class LiveResearchExample {

  public static void main(String[] args) throws Exception {
    String ollamaUrl = ConfigKeys.DEFAULT_OLLAMA_BASE_URL;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // deterministic demo output

    // ── 1. Channels ─────────────────────────────────────────────────────────────────────
    //
    // The crawler frontier is multi-source by design. We wire two channels:
    //   - seedChannel: static list of URLs we want indexed up front.
    //   - agentCrawlChannel: ToolInvocationChannel that the LLM can call via "crawl-url".
    //
    // Both feed into CrawlerCore.builder().frontier(...). External producers could be wired
    // in too by adding a KafkaChannel<UrlRequest> or RedisPubSubChannel without touching the
    // crawler — it doesn't own its inputs.

    Channel<UrlRequest> seedChannel =
        new StaticSeedChannel<>(
            java.util.List.of(
                new UrlRequest("https://en.wikipedia.org/wiki/Apache_Flink", "seed"),
                new UrlRequest("https://en.wikipedia.org/wiki/Vector_database", "seed")),
            TypeInformation.of(UrlRequest.class));

    ToolInvocationChannel<UrlRequest> agentCrawlChannel =
        ToolInvocationChannel.sideOutput(
            "crawl-url",
            UrlRequest.class,
            params -> new UrlRequest((String) params.get("url"), "agent", 0));
    //   ↑ For cross-job consumption: ToolInvocationChannel.via(
    //                                     "crawl-url", UrlRequest.class, mapper,
    //                                     new KafkaChannel<>(brokers, topic, group, UrlRequest.class),
    //                                     kafkaProducer::send)

    Channel<String> queries =
        new StaticSeedChannel<>(
            java.util.List.of(
                "What is Apache Flink and what guarantees does it give?",
                "How do vector databases differ from traditional databases?"),
            TypeInformation.of(String.class));

    // ── 2. Corpus ───────────────────────────────────────────────────────────────────────
    //
    // BroadcastCorpus = ingest in one operator, reads via per-replica copies. The vector
    // memory itself is HNSW-over-Flink-state — vectors checkpoint with the job; the graph
    // rebuilds on restart.

    VectorMemorySpec vectorSpec = FlinkStateHnswVectorMemory.spec(384);
    CorpusSpec corpus = BroadcastCorpus.spec("research-kb", vectorSpec);

    // ── 3. Connections (chat, embed, rerank) ────────────────────────────────────────────

    LangChain4jChatConnection chat = LangChain4jChatConnection.ollama(ollamaUrl);
    ChatSetup chatSetup =
        ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.2).build();

    DjlEmbeddingConnection embeddings =
        DjlEmbeddingConnection.of(
            "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2");

    DjlInferenceConnection reranker =
        DjlInferenceConnection.classification(
            "djl://ai.djl.huggingface.pytorch/cross-encoder/ms-marco-MiniLM-L-6-v2");
    InferenceSetup rerankerSetup =
        InferenceSetup.builder()
            .withModelName("ms-marco-MiniLM-L-6-v2")
            .withModelUri(reranker.getDefaultModelUri())
            .build();

    // ── 4. Ingest pipeline: URLs → crawler → chunk → embed → corpus ─────────────────────

    DataStream<CrawledPage> pages =
        CrawlerCore.builder()
            .frontier(seedChannel, agentCrawlChannel)
            .options(WebToolkitOptions.defaults().withMaxDepth(1))
            .open(env);

    IngestionPipeline.from(pages)
        .chunk(new RecursiveTextChunker(512))
        .embed(embeddings)
        .into(corpus)
        .map(ack -> "ingested " + ack.getChunkId() + " into " + ack.getCorpusName())
        .print()
        .name("ingest-acks");

    // ── 5. Retrieve pipeline: queries → embed → search → rerank → LLM answer ────────────

    RetrievalPipeline.from(queries.open(env))
        .embed(embeddings)
        .search(corpus, 6)
        .rerank(reranker, rerankerSetup)
        .answer(chat, chatSetup)
        .print()
        .name("answers");

    env.execute("live-research");

    // What's *not* in this file:
    //   - any custom operator class
    //   - any tool wiring beyond `ToolInvocationChannel.sideOutput(...)`
    //   - any embedding / search / rerank / answer code
    // Everything heavy lives in the framework. This main() is the sentence the framework's
    // verbs let you write.
    @SuppressWarnings("unused")
    Object unused = Map.of(); // suppress unused-import warning
  }
}

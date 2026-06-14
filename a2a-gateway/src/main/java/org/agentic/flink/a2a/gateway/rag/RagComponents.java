package org.agentic.flink.a2a.gateway.rag;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.Map;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.retrieve.HotVectorIndex;
import org.agentic.flink.retrieve.InMemoryHotVectorIndex;
import org.agentic.flink.retrieve.RedisHotVectorIndex;
import org.agentic.flink.storage.VectorStore;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI wiring for the RAG proxy: the {@link Embedder} (deterministic default), the {@link
 * HotVectorIndex hot} tier (in-JVM by default, Redis when {@code rag.hot=redis}), and the durable
 * {@link VectorStore cold} tier (in-memory here; swap in pgvector/Qdrant/Fluss for production).
 */
@ApplicationScoped
public class RagComponents {

  private static final Logger LOG = LoggerFactory.getLogger(RagComponents.class);

  private final AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();
  private final int dimension =
      Integer.parseInt(config.get("rag.embedding.dimension", "64"));

  @Produces
  @ApplicationScoped
  public Embedder embedder() {
    LOG.info("RAG embedder: HashingEmbedder(dim={})", dimension);
    return new HashingEmbedder(dimension);
  }

  @Produces
  @ApplicationScoped
  public HotVectorIndex hotIndex() {
    String backend = config.get("rag.hot", "memory");
    String name = config.get("rag.hot.name", "rag-hot");
    if ("redis".equalsIgnoreCase(backend)) {
      String host = config.get("redis.host", "localhost");
      int port = Integer.parseInt(config.get("redis.port", "6379"));
      LOG.info("RAG hot tier: Redis {}:{} name={}", host, port, name);
      return new RedisHotVectorIndex(name, host, port);
    }
    LOG.info("RAG hot tier: in-memory name={}", name);
    return new InMemoryHotVectorIndex(name);
  }

  @Produces
  @ApplicationScoped
  public VectorStore coldStore() {
    InMemoryVectorStore store = new InMemoryVectorStore();
    try {
      store.initialize(Map.of("dimension", String.valueOf(dimension), "similarity", "cosine"));
    } catch (Exception e) {
      throw new IllegalStateException("failed to init RAG cold store", e);
    }
    LOG.info("RAG cold tier: in-memory vector store (dim={})", dimension);
    return store;
  }
}

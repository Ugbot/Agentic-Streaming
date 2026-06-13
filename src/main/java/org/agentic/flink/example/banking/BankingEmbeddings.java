package org.agentic.flink.example.banking;

import java.util.Locale;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.GeminiEmbeddingConnection;
import org.agentic.flink.embedding.HashEmbeddingConnection;
import org.agentic.flink.embedding.OllamaEmbeddingConnection;
import org.agentic.flink.embedding.djl.DjlEmbeddingConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chooses the KB embedding model from the environment, so the same agent runs on a local, keyless
 * embedder in development and swaps to the hackathon-mandated {@code gemini-embedding-001} for
 * marked runs with no code change — the embedding analogue of {@link BankingModel}.
 *
 * <ul>
 *   <li>{@code EMBED_PROVIDER} — {@code djl} (default, local inbuilt) | {@code gemini} | {@code
 *       ollama} | {@code hash} | {@code keyword} (handled by the caller — no embeddings).
 *   <li>{@code EMBED_MODEL} / {@code EMBED_DIM} — override the model name / dimension.
 *   <li>{@code GOOGLE_API_KEY} (gemini), {@code OLLAMA_BASE_URL} (ollama).
 * </ul>
 */
public final class BankingEmbeddings {
  private static final Logger LOG = LoggerFactory.getLogger(BankingEmbeddings.class);

  /** Local DJL sentence-transformers model (all-MiniLM-L6-v2, 384-dim) — the inbuilt default. */
  public static final String DJL_MINILM =
      "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2";

  private final EmbeddingConnection connection;
  private final EmbeddingSetup setup;

  private BankingEmbeddings(EmbeddingConnection connection, EmbeddingSetup setup) {
    this.connection = connection;
    this.setup = setup;
  }

  public EmbeddingConnection connection() {
    return connection;
  }

  public EmbeddingSetup setup() {
    return setup;
  }

  /** True when the configured provider is {@code keyword} (use {@link KbSearchTool}, not vectors). */
  public static boolean isKeyword() {
    return "keyword".equalsIgnoreCase(env("EMBED_PROVIDER", "djl"));
  }

  /** Resolve the embedder from environment variables (see class doc). */
  public static BankingEmbeddings fromEnv() {
    String provider = env("EMBED_PROVIDER", "djl").toLowerCase(Locale.ROOT);
    String model = System.getenv("EMBED_MODEL");
    Integer dimOverride = intOrNull(System.getenv("EMBED_DIM"));

    EmbeddingConnection connection;
    String modelName;
    int dim;
    switch (provider) {
      case "gemini":
        connection = new GeminiEmbeddingConnection(require("GOOGLE_API_KEY"));
        modelName = model == null ? "gemini-embedding-001" : model;
        dim = dimOverride == null ? 768 : dimOverride;
        break;
      case "ollama":
        connection = new OllamaEmbeddingConnection(env("OLLAMA_BASE_URL", "http://localhost:11434"));
        modelName = model == null ? "nomic-embed-text" : model;
        dim = dimOverride == null ? 768 : dimOverride;
        break;
      case "hash":
        connection = new HashEmbeddingConnection();
        modelName = model == null ? "hash" : model;
        dim = dimOverride == null ? 256 : dimOverride;
        break;
      case "djl":
      default:
        connection = DjlEmbeddingConnection.of(model == null ? DJL_MINILM : model);
        modelName = model == null ? "all-MiniLM-L6-v2" : model;
        dim = dimOverride == null ? 384 : dimOverride;
        break;
    }
    LOG.info("Banking embeddings: provider={}, model={}, dim={}", provider, modelName, dim);
    return new BankingEmbeddings(connection, EmbeddingSetup.of(modelName, dim, true));
  }

  private static String env(String key, String def) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? def : v;
  }

  private static Integer intOrNull(String v) {
    try {
      return v == null || v.isBlank() ? null : Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String require(String key) {
    String v = System.getenv(key);
    if (v == null || v.isBlank()) {
      throw new IllegalStateException(key + " is required for the selected embedding provider");
    }
    return v;
  }
}

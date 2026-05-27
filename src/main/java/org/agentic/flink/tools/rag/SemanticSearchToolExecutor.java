package org.agentic.flink.tools.rag;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.OllamaEmbeddingConnection;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.VectorStore;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.agentic.flink.tools.AbstractToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Semantic Search Tool Executor. Searches the vector store for semantically similar documents.
 *
 * <p>Migrated off the legacy {@code langchain/model} + {@code langchain/store} packages onto the
 * framework embedding/vector SPIs.
 */
public class SemanticSearchToolExecutor extends AbstractToolExecutor {

  private final EmbeddingConnection embeddingConnection;
  private final VectorStore vectorStore;
  private final EmbeddingSetup embeddingSetup;
  private final Map<String, String> config;

  public SemanticSearchToolExecutor(Map<String, String> config) {
    super("semantic_search", "Search knowledge base using semantic similarity");
    this.config = config != null ? config : new HashMap<>();
    String baseUrl = this.config.getOrDefault("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    String modelName = this.config.getOrDefault("modelName", "nomic-embed-text");
    int dimension = Integer.parseInt(this.config.getOrDefault("dimension", "768"));
    this.embeddingConnection = new OllamaEmbeddingConnection(baseUrl);
    this.embeddingSetup = EmbeddingSetup.of(modelName, dimension);
    this.vectorStore = buildVectorStore(this.config);
  }

  public SemanticSearchToolExecutor(
      EmbeddingConnection embeddingConnection, VectorStore vectorStore) {
    this(null, embeddingConnection, vectorStore);
  }

  public SemanticSearchToolExecutor(
      Map<String, String> config,
      EmbeddingConnection embeddingConnection,
      VectorStore vectorStore) {
    super("semantic_search", "Search knowledge base using semantic similarity");
    this.config = config != null ? config : new HashMap<>();
    String modelName = this.config.getOrDefault("modelName", "nomic-embed-text");
    int dimension = Integer.parseInt(this.config.getOrDefault("dimension", "768"));
    this.embeddingConnection = embeddingConnection;
    this.vectorStore = vectorStore;
    this.embeddingSetup = EmbeddingSetup.of(modelName, dimension);
  }

  private static VectorStore buildVectorStore(Map<String, String> config) {
    String backend = config.get("vector.backend");
    try {
      if (backend != null && !backend.isBlank()) {
        return StorageFactory.createVectorStore(backend, config);
      }
      InMemoryVectorStore store = new InMemoryVectorStore();
      store.initialize(config);
      return store;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create vector store: " + e.getMessage(), e);
    }
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    logExecution(parameters);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String query = getRequiredParameter(parameters, "query", String.class);
            Integer maxResults =
                getOptionalParameter(parameters, "max_results", Integer.class, 10);
            Double minScore = getOptionalParameter(parameters, "min_score", Double.class, 0.7);

            // Create query embedding.
            EmbeddingClient client = embeddingConnection.bind(null);
            float[] queryEmbedding = client.embed(query, embeddingSetup);

            // Search the vector store.
            List<VectorStore.VectorSearchResult> matches =
                vectorStore.searchSimilar(queryEmbedding, maxResults);

            List<Map<String, Object>> formatted = formatResults(matches, minScore);

            LOG.info(
                "Semantic search found {} results for query: {}", formatted.size(), query);

            Map<String, Object> result = new HashMap<>();
            result.put("query", query);
            result.put("result_count", formatted.size());
            result.put("results", formatted);

            return result;

          } catch (Exception e) {
            LOG.error("Semantic search execution failed", e);
            throw new RuntimeException("Semantic search execution failed: " + e.getMessage(), e);
          }
        });
  }

  private List<Map<String, Object>> formatResults(
      List<VectorStore.VectorSearchResult> matches, double minScore) {
    List<Map<String, Object>> out = new ArrayList<>();
    for (VectorStore.VectorSearchResult match : matches) {
      if (match.getScore() < minScore) {
        continue;
      }
      Map<String, Object> resultItem = new HashMap<>();
      Object text = match.getMetadata() != null ? match.getMetadata().get("text") : null;
      resultItem.put("text", text != null ? text : "");
      resultItem.put("score", (double) match.getScore());
      resultItem.put("embedding_id", match.getId());
      out.add(resultItem);
    }
    return out;
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    if (!super.validateParameters(parameters)) {
      return false;
    }
    return parameters.containsKey("query") && parameters.get("query") instanceof String;
  }
}

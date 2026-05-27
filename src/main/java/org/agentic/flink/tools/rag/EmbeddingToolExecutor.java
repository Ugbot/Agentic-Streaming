package org.agentic.flink.tools.rag;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.OllamaEmbeddingConnection;
import org.agentic.flink.tools.AbstractToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Embedding Tool Executor Converts text to vector embeddings for similarity search
 */
public class EmbeddingToolExecutor extends AbstractToolExecutor {

  private final EmbeddingConnection embeddingConnection;
  private final EmbeddingSetup embeddingSetup;

  public EmbeddingToolExecutor(Map<String, String> config) {
    super("embedding", "Convert text to vector embeddings");
    Map<String, String> cfg = config != null ? config : new HashMap<>();
    String baseUrl = cfg.getOrDefault("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    String modelName = cfg.getOrDefault("modelName", "nomic-embed-text");
    int dimension = Integer.parseInt(cfg.getOrDefault("dimension", "768"));
    this.embeddingConnection = new OllamaEmbeddingConnection(baseUrl);
    this.embeddingSetup = EmbeddingSetup.of(modelName, dimension);
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    logExecution(parameters);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String text = getRequiredParameter(parameters, "text", String.class);
            Boolean returnVector =
                getOptionalParameter(parameters, "return_vector", Boolean.class, false);

            EmbeddingClient client = embeddingConnection.bind(null);
            float[] vector = client.embed(text, embeddingSetup);

            LOG.info("Created embedding for text of length: {}", text.length());

            Map<String, Object> result = new HashMap<>();
            result.put("text", text);
            result.put("dimension", vector.length);

            if (returnVector) {
              result.put("vector", vector);
            }

            return result;

          } catch (Exception e) {
            LOG.error("Embedding execution failed", e);
            throw new RuntimeException("Embedding execution failed: " + e.getMessage(), e);
          }
        });
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    if (!super.validateParameters(parameters)) {
      return false;
    }
    return parameters.containsKey("text") && parameters.get("text") instanceof String;
  }
}

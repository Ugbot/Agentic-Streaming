package org.agentic.flink.tools.rag;

import org.agentic.flink.tools.AbstractToolExecutor;
import org.agentic.flink.langchain.model.embedding.LangChainEmbeddingModel;
import org.agentic.flink.langchain.model.embedding.OllamaEmbeddingModel;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Embedding Tool Executor Converts text to vector embeddings for similarity search
 */
public class EmbeddingToolExecutor extends AbstractToolExecutor {

  private final LangChainEmbeddingModel embeddingModelProvider;
  private final Map<String, String> config;

  public EmbeddingToolExecutor(Map<String, String> config) {
    super("embedding", "Convert text to vector embeddings");
    this.config = config != null ? config : new HashMap<>();
    this.embeddingModelProvider = new OllamaEmbeddingModel();
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

            EmbeddingModel embeddingModel = embeddingModelProvider.getModel(config);
            Response<Embedding> response = embeddingModel.embed(text);
            Embedding embedding = response.content();

            LOG.info("Created embedding for text of length: {}", text.length());

            Map<String, Object> result = new HashMap<>();
            result.put("text", text);
            result.put("dimension", embedding.dimension());

            if (returnVector) {
              result.put("vector", embedding.vector());
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

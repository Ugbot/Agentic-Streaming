package org.agentic.flink.embedding;

import java.io.Serializable;
import java.util.Objects;

/**
 * Per-use embedding configuration.
 *
 * <p>The connection/setup split mirrors {@link org.agentic.flink.llm.ChatConnection}:
 * one long-lived {@link EmbeddingConnection} (Ollama service, OpenAI account) serves many call
 * sites with different model names and dimensions.
 */
public final class EmbeddingSetup implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String modelName;
  private final int dimension;
  private final boolean normalize;

  private EmbeddingSetup(String modelName, int dimension, boolean normalize) {
    this.modelName = Objects.requireNonNull(modelName, "modelName");
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be positive, got " + dimension);
    }
    this.dimension = dimension;
    this.normalize = normalize;
  }

  public static EmbeddingSetup of(String modelName, int dimension) {
    return new EmbeddingSetup(modelName, dimension, true);
  }

  public static EmbeddingSetup of(String modelName, int dimension, boolean normalize) {
    return new EmbeddingSetup(modelName, dimension, normalize);
  }

  public String getModelName() {
    return modelName;
  }

  public int getDimension() {
    return dimension;
  }

  public boolean shouldNormalize() {
    return normalize;
  }
}

package org.agentic.flink.embedding.djl;

import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.inference.djl.DjlInferenceConnection;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * {@link EmbeddingConnection} that routes through DJL.
 *
 * <p>Thin adapter — the actual model loading and inference live in {@link
 * DjlInferenceConnection}. This class exists so embedder-only users can wire a DL embedder
 * (e.g. {@code sentence-transformers/all-MiniLM-L6-v2}) directly into
 * {@code AgentBuilder.withEmbeddingConnection(...)} without thinking about the inference SPI.
 */
public final class DjlEmbeddingConnection implements EmbeddingConnection {
  private static final long serialVersionUID = 1L;

  private final String modelUri;

  /** No-arg constructor for {@link java.util.ServiceLoader}; embedding URI must be set later. */
  public DjlEmbeddingConnection() {
    this(null);
  }

  public DjlEmbeddingConnection(String modelUri) {
    this.modelUri = modelUri;
  }

  /** Factory matching the rest of the framework's style. */
  public static DjlEmbeddingConnection of(String modelUri) {
    return new DjlEmbeddingConnection(modelUri);
  }

  public String getModelUri() {
    return modelUri;
  }

  @Override
  public EmbeddingClient bind(RuntimeContext runtimeContext) throws Exception {
    DjlInferenceConnection inner = DjlInferenceConnection.embedding(modelUri);
    return inner.bind(runtimeContext).asEmbedder();
  }

  @Override
  public String providerName() {
    return "djl:embedding";
  }
}

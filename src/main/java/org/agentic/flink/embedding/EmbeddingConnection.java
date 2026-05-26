package org.agentic.flink.embedding;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Serializable spec for an embedding-model transport.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}. Default implementation is
 * {@code OllamaEmbeddingConnection} pointing at the local Ollama service.
 */
public interface EmbeddingConnection extends Serializable {

  /** Construct the operator-scoped client. Called once per task in {@code RichFunction.open()}. */
  EmbeddingClient bind(RuntimeContext runtimeContext) throws Exception;

  /** Human-readable provider name for logging. */
  default String providerName() {
    return getClass().getSimpleName();
  }
}

package org.agentic.flink.inference;

import org.agentic.flink.embedding.EmbeddingClient;

/**
 * Runtime handle for one or more loaded inference models, returned by
 * {@link InferenceConnection#bind}.
 *
 * <p>Mirrors {@link org.agentic.flink.llm.ChatClient}: live, not serialized, held by an
 * operator for the duration of a Flink task. The client exposes typed task views via
 * {@code as*()} accessors; backends that don't support a given task throw
 * {@link UnsupportedOperationException} from the corresponding view. Callers can probe with
 * {@link #supports(TaskKind)} first.
 */
public interface InferenceClient extends AutoCloseable {

  /** The task surfaces a backend may implement. */
  enum TaskKind {
    CLASSIFIER,
    SCORER,
    EMBEDDER,
    GENERIC
  }

  /** Whether this client implements the given task surface. */
  boolean supports(TaskKind kind);

  /** Typed view as a {@link Classifier}. */
  Classifier asClassifier();

  /** Typed view as a {@link Scorer}. */
  Scorer asScorer();

  /**
   * Typed view as an {@link EmbeddingClient} — the existing embedding SPI, not a separate
   * hierarchy. A DL-backed embedder fits naturally here.
   */
  EmbeddingClient asEmbedder();

  /** Typed view as a {@link GenericInferenceModel}. */
  GenericInferenceModel asGeneric();

  /** Human-readable provider name for logging and metrics. */
  String providerName();

  /** Release model resources. */
  @Override
  default void close() {
    // no-op
  }
}

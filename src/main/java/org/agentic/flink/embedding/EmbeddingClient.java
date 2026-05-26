package org.agentic.flink.embedding;

import java.util.ArrayList;
import java.util.List;

/** Runtime handle for an embedding model, returned by {@link EmbeddingConnection#bind}. */
public interface EmbeddingClient extends AutoCloseable {

  /** Embed a single string under the given setup. */
  float[] embed(String text, EmbeddingSetup setup);

  /**
   * Embed a batch. Default implementation calls {@link #embed} per item; providers that support
   * native batching should override.
   */
  default List<float[]> embedBatch(List<String> texts, EmbeddingSetup setup) {
    List<float[]> out = new ArrayList<>(texts.size());
    for (String t : texts) {
      out.add(embed(t, setup));
    }
    return out;
  }

  /** Human-readable provider name for logging and metrics. */
  String providerName();

  @Override
  default void close() {
    // no-op
  }
}

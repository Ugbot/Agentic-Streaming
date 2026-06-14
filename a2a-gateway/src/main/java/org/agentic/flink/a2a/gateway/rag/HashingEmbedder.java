package org.agentic.flink.a2a.gateway.rag;

import java.util.Locale;

/**
 * A deterministic, dependency-free bag-of-words hashing embedder: each lowercased token is hashed
 * into one of {@code dimension} buckets and accumulated, then the vector is L2-normalized. Documents
 * that share vocabulary land near each other in cosine space, so "ingest a passage, query with its
 * words, retrieve it" works without any external model — enough to exercise the live hot+cold RAG
 * proxy end-to-end. Swap in a model-backed {@link Embedder} for semantic quality in production.
 */
public final class HashingEmbedder implements Embedder {

  private final int dimension;

  public HashingEmbedder(int dimension) {
    this.dimension = Math.max(8, dimension);
  }

  @Override
  public float[] embed(String text) {
    float[] v = new float[dimension];
    if (text == null || text.isEmpty()) {
      return v;
    }
    for (String tok : text.toLowerCase(Locale.ROOT).split("[^a-z0-9]+")) {
      if (tok.isEmpty()) {
        continue;
      }
      int h = tok.hashCode();
      int bucket = Math.floorMod(h, dimension);
      // Sign from a second hash so distinct tokens don't all add positively.
      v[bucket] += ((h >>> 31) == 0) ? 1.0f : -1.0f;
    }
    double norm = 0;
    for (float x : v) {
      norm += (double) x * x;
    }
    norm = Math.sqrt(norm);
    if (norm > 0) {
      for (int i = 0; i < dimension; i++) {
        v[i] = (float) (v[i] / norm);
      }
    }
    return v;
  }

  @Override
  public int dimension() {
    return dimension;
  }
}

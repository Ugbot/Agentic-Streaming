package org.jagentic.core.embedding;

import org.jagentic.core.Retrieval;

/** Deterministic FNV bag-of-words embedder (the default). */
public final class HashingEmbedder implements Embedder {
  private final int dim;

  public HashingEmbedder(int dim) {
    this.dim = dim <= 0 ? 256 : dim;
  }

  @Override
  public float[] embed(String text) {
    return Retrieval.embed(text, dim);
  }

  @Override
  public int dim() {
    return dim;
  }
}

package org.agentic.flink.memory.vector;

import java.io.Serializable;

/**
 * Graph-construction parameters for an HNSW-backed {@link VectorMemory}.
 *
 * <p>Defaults mirror the values JVector and Lucene's HNSW use in published benchmarks for
 * embedding-style workloads at d≈384.
 */
public final class HnswBuildConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Number of neighbors per node in the graph. Higher = better recall, more memory. */
  private final int m;

  /** Beam width / efConstruction during graph build. Higher = better recall, slower builds. */
  private final int beamWidth;

  /** efSearch during query time. Higher = better recall, slower queries. */
  private final int searchBeam;

  /** Alpha multiplier for the neighbor-selection heuristic (JVector terminology). */
  private final float alpha;

  /** Similarity function. */
  private final VectorMemorySpec.Similarity similarity;

  public HnswBuildConfig(
      int m,
      int beamWidth,
      int searchBeam,
      float alpha,
      VectorMemorySpec.Similarity similarity) {
    if (m <= 0) throw new IllegalArgumentException("m must be positive");
    if (beamWidth <= 0) throw new IllegalArgumentException("beamWidth must be positive");
    if (searchBeam <= 0) throw new IllegalArgumentException("searchBeam must be positive");
    this.m = m;
    this.beamWidth = beamWidth;
    this.searchBeam = searchBeam;
    this.alpha = alpha;
    this.similarity =
        similarity == null ? VectorMemorySpec.Similarity.COSINE : similarity;
  }

  public static HnswBuildConfig defaults() {
    return new HnswBuildConfig(16, 100, 50, 1.2f, VectorMemorySpec.Similarity.COSINE);
  }

  public int getM() {
    return m;
  }

  public int getBeamWidth() {
    return beamWidth;
  }

  public int getSearchBeam() {
    return searchBeam;
  }

  public float getAlpha() {
    return alpha;
  }

  public VectorMemorySpec.Similarity getSimilarity() {
    return similarity;
  }
}

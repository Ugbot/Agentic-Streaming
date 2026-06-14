package org.jagentic.core;

import java.util.List;

/** In-process HNSW vector store — a real approximate-nearest-neighbour index with no
 * external dependency. Drop-in cold tier; recall approaches brute force without scanning
 * every vector. */
public final class HnswVectorStore implements VectorStore {
  private final HnswIndex index;

  public HnswVectorStore(int m, int efConstruction, int efSearch, long seed) {
    this.index = new HnswIndex(m, efConstruction, efSearch, seed);
  }

  public HnswVectorStore() {
    this.index = new HnswIndex();
  }

  @Override
  public void upsert(String docId, float[] embedding, String text) {
    index.add(docId, embedding, text);
  }

  @Override
  public List<Retrieval.Scored> search(float[] query, int k) {
    return index.search(query, k);
  }
}

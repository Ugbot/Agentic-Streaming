package org.jagentic.core;

import java.util.List;

/** Vector store SPI — the cold tier of the two-tier retriever, behind an interface.
 * {@link InMemoryVectorStore} is the default; the {@code store} package adds a real
 * Qdrant impl. */
public interface VectorStore {
  void upsert(String docId, float[] embedding, String text);

  List<Retrieval.Scored> search(float[] query, int k);

  /** Adapt to the TwoTierRetriever cold-tier signature. */
  default Retrieval.ColdSearch coldSearch() {
    return (q, k) -> search(q, k);
  }
}

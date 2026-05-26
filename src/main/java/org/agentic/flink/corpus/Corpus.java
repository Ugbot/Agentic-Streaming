package org.agentic.flink.corpus;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.memory.vector.ScoredItem;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Named, possibly-shared knowledge base — vectors plus metadata, addressed by name.
 *
 * <p>A corpus is the framework abstraction for "an index of embedded {@link ContextItem}s that
 * multiple operators may read from and/or write to." Three flavours ship in-box:
 *
 * <ul>
 *   <li>{@link SingleOperatorCorpus} — both reads and writes happen in one operator. Smallest,
 *       fastest, simplest. The natural fit for the {@code KeyedCoProcessFunction} pattern.
 *   <li>{@link BroadcastCorpus} — ingest is one operator; reads happen in any number of read
 *       operators that each hold a replica via Flink broadcast state. Documented pattern; users
 *       wire the broadcast stream in their job graph.
 *   <li>{@link ExternalCorpus} — vectors live in a {@code VectorStore} (pgvector / Qdrant).
 *       Operators are stateless reads/writes against the shared external store.
 * </ul>
 *
 * <p>All three implement the same {@link Corpus} contract so a user can swap the flavour without
 * changing the agent code that consumes the corpus.
 */
public interface Corpus {

  /** Upsert a vectorized item. */
  CompletableFuture<Void> upsert(String id, float[] embedding, ContextItem item);

  /** Search the top-k most similar items to {@code query}. Higher score = more similar. */
  CompletableFuture<List<ScoredItem>> search(float[] query, int k);

  /** Snapshot of size + dimension + provider. */
  CorpusStats stats();

  /** Free runtime resources. Default: no-op. */
  default void close() throws Exception {
    // no-op
  }
}

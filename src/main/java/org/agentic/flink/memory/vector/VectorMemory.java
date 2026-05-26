package org.agentic.flink.memory.vector;

import java.util.List;

/**
 * Per-operator vector memory abstraction backed by Flink state.
 *
 * <p>Like {@link org.agentic.flink.memory.ShortTermMemory}, this lives inside a Flink
 * {@code RichFunction} — instances are constructed in {@code open()} from a serializable
 * {@link VectorMemorySpec}. The current operator key supplies the scope: every method operates on
 * the vectors belonging to the currently-keyed conversation (or, when scope is {@code
 * PER_OPERATOR}, on a single shared graph for the whole task slot).
 *
 * <p>The default implementation ({@link FlinkStateVectorMemory}) is exact brute-force KNN. That
 * is intentional: for the typical "conversation-local semantic recall" workload — hundreds to low
 * thousands of vectors per key — brute-force at d=768 takes well under a millisecond and is
 * provably correct. Users with larger graphs register an HNSW-backed spec via {@code
 * ServiceLoader}.
 */
public interface VectorMemory {

  /** Insert or replace a vector entry. */
  void put(String id, float[] embedding, org.agentic.flink.context.core.ContextItem item)
      throws Exception;

  /** Insert or replace a vector entry from a prebuilt {@link VectorEntry}. */
  void put(VectorEntry entry) throws Exception;

  /** Remove an entry by id; no-op if absent. */
  void remove(String id) throws Exception;

  /** Return the top-k entries by similarity to {@code query}. Higher score = more similar. */
  List<ScoredItem> search(float[] query, int k) throws Exception;

  /** Number of entries currently stored for the active scope. */
  int size() throws Exception;

  /** Remove every entry from the active scope. */
  void clear() throws Exception;
}

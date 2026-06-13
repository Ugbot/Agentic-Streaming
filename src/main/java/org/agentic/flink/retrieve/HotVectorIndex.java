package org.agentic.flink.retrieve;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.agentic.flink.memory.vector.ScoredItem;

/**
 * The <b>hot</b> tier of a live RAG retrieval stack: a low-latency, recency-bounded vector index over
 * just-ingested documents, queried in parallel with the durable <b>cold</b> tier (a {@link
 * org.agentic.flink.corpus.Corpus} / {@link org.agentic.flink.storage.vector.VectorStore}) and merged
 * by {@link TwoTierRetriever}.
 *
 * <p>The contract is deliberately small so it can sit on different backends with the same semantics:
 *
 * <ul>
 *   <li><b>in-JVM</b> ({@link InMemoryHotVectorIndex}) — a process-wide shared, capacity-bounded
 *       brute-force index; the correct default for the embedded single-JVM deployment where the
 *       ingest operator and the query operator share a JVM.
 *   <li><b>Redis</b> ({@code RedisHotVectorIndex}) — a capped, TTL'd recent window in Redis, for a
 *       distributed cluster; the user's preferred hot backend.
 *   <li><b>Fluss</b> — a PK table window, reusing the framework's Fluss client wiring.
 * </ul>
 *
 * <p>Implementations are {@link Serializable} (the config ships in the Flink job graph); the live
 * index/connection is built lazily on the task side. A bounded capacity (and/or TTL) evicts the
 * oldest entries so the hot tier stays small and fast — it is a moving window of fresh data, not a
 * second copy of the whole corpus.
 */
public interface HotVectorIndex extends Serializable {

  /**
   * Insert or replace a document in the hot window. {@code embedding} is the document vector,
   * {@code text} the passage content, {@code metadata} optional (e.g. {@code source_url}); both may
   * be null/empty. Re-using an existing {@code id} replaces it (and refreshes its recency).
   */
  void upsert(String id, float[] embedding, String text, Map<String, String> metadata);

  /** Top-{@code k} nearest documents to {@code query} by cosine similarity (highest score first). */
  List<ScoredItem> search(float[] query, int k);

  /** Current number of documents held in the hot window. */
  int size();

  /** Drop everything (e.g. between tests or on reset). */
  void clear();
}

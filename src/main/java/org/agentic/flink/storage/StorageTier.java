package org.agentic.flink.storage;

/**
 * Storage tier classification based on latency and access patterns.
 *
 * <p>Storage tiers define the performance characteristics and use cases for different storage
 * backends. Each tier has different latency expectations and persistence guarantees.
 *
 * <p>Performance Characteristics:
 *
 * <ul>
 *   <li>HOT: Sub-millisecond latency, in-memory or local cache (Caffeine, Redis, Hazelcast)
 *   <li>WARM: 1-10ms latency, external fast storage for recent data (Redis, DynamoDB, Cassandra)
 *   <li>COLD: 10-100ms latency, long-term persistence (PostgreSQL, S3, ClickHouse)
 *   <li>VECTOR: Variable latency, specialized for embedding search (Qdrant, Pinecone, Weaviate)
 *   <li>CHECKPOINT: Flink-managed for fault tolerance (RocksDB, HashMapStateBackend)
 * </ul>
 *
 * @author Agentic Flink Team
 */
public enum StorageTier {
  /**
   * Hot tier storage.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Latency: &lt;1ms
   *   <li>Scope: Active conversation state
   *   <li>Backends: Caffeine cache, Redis, Hazelcast IMDG
   *   <li>TTL: Minutes to hours
   * </ul>
   */
  HOT,

  /**
   * Warm tier storage.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Latency: 1-10ms
   *   <li>Scope: Recent conversations for resumption
   *   <li>Backends: Redis, DynamoDB, Cassandra, MongoDB
   *   <li>TTL: Hours to days
   * </ul>
   */
  WARM,

  /**
   * Cold tier storage.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Latency: 10-100ms
   *   <li>Scope: Historical data and analytics
   *   <li>Backends: PostgreSQL, S3, ClickHouse
   *   <li>TTL: Days to months
   * </ul>
   */
  COLD,

  /**
   * Vector tier storage.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Latency: Variable (5-50ms depending on index size)
   *   <li>Scope: Semantic search over embeddings
   *   <li>Backends: Qdrant, Pinecone, Weaviate, pgvector
   *   <li>TTL: Indefinite or application-specific
   * </ul>
   */
  VECTOR,

  /**
   * Checkpoint tier storage.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Latency: &lt;1ms (local) to 1-5ms (remote)
   *   <li>Scope: Flink-managed fault tolerance state
   *   <li>Backends: RocksDB, HashMapStateBackend, S3 checkpoint storage
   *   <li>TTL: Checkpoint retention policy (typically hours)
   * </ul>
   */
  CHECKPOINT
}

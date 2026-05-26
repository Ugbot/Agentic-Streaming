package org.agentic.flink.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * Base interface for all storage providers in the multi-tier storage architecture.
 *
 * <p>This interface defines the contract for pluggable storage backends at every tier (HOT, WARM,
 * COLD, VECTOR, CHECKPOINT). All concrete storage implementations must implement this interface.
 *
 * <p>Implementations must be Serializable for use in Flink distributed environments. Transient
 * fields should be used for non-serializable resources (connections, clients) and reinitialized in
 * open() or initialize().
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Configuration
 * Map<String, String> config = new HashMap<>();
 * config.put("redis.host", "localhost");
 * config.put("redis.port", "6379");
 *
 * // Initialize storage provider
 * StorageProvider<String, AgentContext> store = new RedisShortTermStore();
 * store.initialize(config);
 *
 * // Store data
 * store.put("flow-001", context);
 *
 * // Retrieve data
 * Optional<AgentContext> loaded = store.get("flow-001");
 * }</pre>
 *
 * @param <K> Key type (typically String for flowId)
 * @param <V> Value type (AgentContext, ContextItem, etc.)
 * @author Agentic Flink Team
 */
public interface StorageProvider<K, V> extends Serializable {

  /**
   * Initialize the storage provider with configuration.
   *
   * <p>This method is called once during setup to configure the storage backend. Implementations
   * should establish connections, initialize clients, and validate configuration.
   *
   * <p>Configuration keys vary by backend:
   *
   * <ul>
   *   <li>Redis: redis.host, redis.port, redis.password, redis.ttl.seconds
   *   <li>DynamoDB: dynamodb.table, dynamodb.region, dynamodb.ttl.days
   *   <li>PostgreSQL: postgresql.jdbc.url, postgresql.username, postgresql.password
   * </ul>
   *
   * @param config Configuration map with backend-specific parameters
   * @throws Exception if initialization fails (connection error, invalid config, etc.)
   */
  void initialize(Map<String, String> config) throws Exception;

  /**
   * Store a value with the given key.
   *
   * <p>If a value already exists for this key, it will be overwritten. Implementations should
   * handle serialization and any backend-specific encoding.
   *
   * <p>This operation should be atomic at the storage backend level.
   *
   * @param key Key to store under
   * @param value Value to store
   * @throws Exception if storage operation fails
   */
  void put(K key, V value) throws Exception;

  /**
   * Retrieve a value by key.
   *
   * <p>Returns Optional.empty() if the key does not exist or the value has expired (TTL).
   *
   * @param key Key to retrieve
   * @return Optional containing the value, or empty if not found
   * @throws Exception if retrieval operation fails
   */
  Optional<V> get(K key) throws Exception;

  /**
   * Delete a value by key.
   *
   * <p>This operation should be idempotent. Deleting a non-existent key should not throw an error.
   *
   * @param key Key to delete
   * @throws Exception if deletion operation fails
   */
  void delete(K key) throws Exception;

  /**
   * Check if a key exists in storage.
   *
   * <p>This should be more efficient than retrieving the full value when only existence needs to
   * be checked.
   *
   * @param key Key to check
   * @return true if key exists and has not expired, false otherwise
   * @throws Exception if existence check fails
   */
  boolean exists(K key) throws Exception;

  /**
   * Close the storage provider and release resources.
   *
   * <p>Implementations should close connections, shutdown thread pools, and clean up any resources
   * held. This method should be idempotent.
   *
   * @throws Exception if cleanup fails
   */
  void close() throws Exception;

  /**
   * Get the storage tier classification for this provider.
   *
   * <p>This helps the system understand the performance characteristics and select appropriate
   * storage backends for different use cases.
   *
   * @return The storage tier (HOT, WARM, COLD, VECTOR, CHECKPOINT)
   */
  StorageTier getTier();

  /**
   * Get the expected latency in milliseconds for typical operations.
   *
   * <p>This is used for monitoring, alerting, and selecting appropriate storage backends. Values
   * are approximate:
   *
   * <ul>
   *   <li>HOT: 0-1ms
   *   <li>WARM: 1-10ms
   *   <li>COLD: 10-100ms
   *   <li>VECTOR: 5-50ms (varies with index size)
   * </ul>
   *
   * @return Expected operation latency in milliseconds
   */
  long getExpectedLatencyMs();

  /**
   * Get a human-readable name for this storage provider.
   *
   * <p>Used for logging, metrics, and debugging.
   *
   * @return Provider name (e.g., "RedisShortTermStore", "CaffeineMemoryStore")
   */
  default String getProviderName() {
    return this.getClass().getSimpleName();
  }
}

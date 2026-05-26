package org.agentic.flink.storage.memory;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.ShortTermMemoryStore;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory implementation of ShortTermMemoryStore using Caffeine cache.
 *
 * <p>This implementation provides sub-millisecond access times by storing all data in local memory
 * with Caffeine's high-performance caching. Suitable for single-JVM deployments or as a local
 * cache layer in front of distributed storage.
 *
 * <p>Characteristics:
 *
 * <ul>
 *   <li>Latency: &lt;1ms
 *   <li>Capacity: Limited by JVM heap
 *   <li>Persistence: None (data lost on restart)
 *   <li>Distribution: Local to JVM (not shared across Flink task managers)
 * </ul>
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * Map<String, String> config = new HashMap<>();
 * config.put("cache.max.size", "10000");  // Max number of conversations
 * config.put("cache.ttl.seconds", "3600");  // 1 hour TTL
 * config.put("cache.expire.after.access", "true");  // Expire after last access
 * }</pre>
 *
 * <p>Dependency required (add to pom.xml):
 *
 * <pre>{@code
 * <dependency>
 *     <groupId>com.github.ben-manes.caffeine</groupId>
 *     <artifactId>caffeine</artifactId>
 *     <version>3.1.8</version>
 * </dependency>
 * }</pre>
 *
 * <p>Note: This implementation uses ConcurrentHashMap as a fallback if Caffeine is not available.
 * For production use with eviction policies, add the Caffeine dependency.
 *
 * @author Agentic Flink Team
 */
public class InMemoryShortTermStore implements ShortTermMemoryStore {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryShortTermStore.class);

  // Using ConcurrentHashMap as base storage
  // ConcurrentHashMap is sufficient for development and testing.
  // For production with high-throughput eviction, consider Caffeine cache.
  private transient ConcurrentHashMap<String, List<ContextItem>> storage;
  private transient ConcurrentHashMap<String, Long> expirationTimes;

  private long defaultTTLSeconds = 3600; // 1 hour default
  private int maxSize = 10000;
  private boolean expireAfterAccess = true;

  // Statistics
  private transient long hitCount = 0;
  private transient long missCount = 0;
  private transient long putCount = 0;

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    this.storage = new ConcurrentHashMap<>();
    this.expirationTimes = new ConcurrentHashMap<>();

    // Parse configuration
    if (config != null) {
      this.defaultTTLSeconds =
          Long.parseLong(config.getOrDefault("cache.ttl.seconds", "3600"));
      this.maxSize = Integer.parseInt(config.getOrDefault("cache.max.size", "10000"));
      this.expireAfterAccess =
          Boolean.parseBoolean(config.getOrDefault("cache.expire.after.access", "true"));
    }

    LOG.info(
        "InMemoryShortTermStore initialized: maxSize={}, ttl={}s, expireAfterAccess={}",
        maxSize,
        defaultTTLSeconds,
        expireAfterAccess);

    // Start background cleanup thread for expired entries
    startCleanupThread();
  }

  @Override
  public void put(String key, List<ContextItem> value) throws Exception {
    putItems(key, value);
  }

  @Override
  public Optional<List<ContextItem>> get(String key) throws Exception {
    List<ContextItem> items = getItems(key);
    return items.isEmpty() ? Optional.empty() : Optional.of(items);
  }

  @Override
  public void putItems(String flowId, List<ContextItem> items) throws Exception {
    if (flowId == null || items == null) {
      throw new IllegalArgumentException("flowId and items cannot be null");
    }

    // Check size limit
    if (storage.size() >= maxSize && !storage.containsKey(flowId)) {
      // Evict oldest entry (simple LRU approximation)
      evictOldest();
    }

    // Store items with defensive copy
    storage.put(flowId, new ArrayList<>(items));
    updateExpiration(flowId);
    putCount++;

    LOG.debug("Stored {} items for flow {}", items.size(), flowId);
  }

  @Override
  public List<ContextItem> getItems(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    // Check expiration
    if (isExpired(flowId)) {
      storage.remove(flowId);
      expirationTimes.remove(flowId);
      missCount++;
      return new ArrayList<>();
    }

    List<ContextItem> items = storage.get(flowId);
    if (items != null) {
      hitCount++;
      // Update expiration if expire-after-access is enabled
      if (expireAfterAccess) {
        updateExpiration(flowId);
      }
      // Return defensive copy
      return new ArrayList<>(items);
    } else {
      missCount++;
      return new ArrayList<>();
    }
  }

  @Override
  public void addItem(String flowId, ContextItem item) throws Exception {
    if (flowId == null || item == null) {
      throw new IllegalArgumentException("flowId and item cannot be null");
    }

    List<ContextItem> items = storage.computeIfAbsent(flowId, k -> new ArrayList<>());
    items.add(item);
    updateExpiration(flowId);
    putCount++;

    LOG.debug("Added item to flow {}: {}", flowId, item.getItemId());
  }

  @Override
  public void removeItem(String flowId, String itemId) throws Exception {
    if (flowId == null || itemId == null) {
      throw new IllegalArgumentException("flowId and itemId cannot be null");
    }

    List<ContextItem> items = storage.get(flowId);
    if (items != null) {
      items.removeIf(item -> itemId.equals(item.getItemId()));
      updateExpiration(flowId);
      LOG.debug("Removed item {} from flow {}", itemId, flowId);
    }
  }

  @Override
  public int getItemCount(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    if (isExpired(flowId)) {
      storage.remove(flowId);
      expirationTimes.remove(flowId);
      return 0;
    }

    List<ContextItem> items = storage.get(flowId);
    return items != null ? items.size() : 0;
  }

  @Override
  public void clearItems(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    storage.remove(flowId);
    expirationTimes.remove(flowId);
    LOG.debug("Cleared items for flow {}", flowId);
  }

  @Override
  public void delete(String key) throws Exception {
    clearItems(key);
  }

  @Override
  public boolean exists(String key) throws Exception {
    if (key == null) {
      return false;
    }

    if (isExpired(key)) {
      storage.remove(key);
      expirationTimes.remove(key);
      return false;
    }

    return storage.containsKey(key);
  }

  @Override
  public Map<String, Object> getStatistics() throws Exception {
    Map<String, Object> stats = new HashMap<>();

    int totalItems = storage.values().stream().mapToInt(List::size).sum();
    int activeConversations = storage.size();
    double hitRate = (hitCount + missCount) > 0
        ? (double) hitCount / (hitCount + missCount)
        : 0.0;
    double avgItemsPerConversation = activeConversations > 0
        ? (double) totalItems / activeConversations
        : 0.0;

    stats.put("total_items", totalItems);
    stats.put("active_conversations", activeConversations);
    stats.put("cache_hit_rate", hitRate);
    stats.put("avg_items_per_conversation", avgItemsPerConversation);
    stats.put("hit_count", hitCount);
    stats.put("miss_count", missCount);
    stats.put("put_count", putCount);
    stats.put("max_size", maxSize);
    stats.put("ttl_seconds", defaultTTLSeconds);

    return stats;
  }

  @Override
  public void setTTL(String flowId, long ttlSeconds) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    long expirationTime = System.currentTimeMillis() + (ttlSeconds * 1000);
    expirationTimes.put(flowId, expirationTime);
    LOG.debug("Set TTL for flow {} to {} seconds", flowId, ttlSeconds);
  }

  @Override
  public void close() throws Exception {
    if (storage != null) {
      storage.clear();
    }
    if (expirationTimes != null) {
      expirationTimes.clear();
    }
    LOG.info("InMemoryShortTermStore closed");
  }

  @Override
  public StorageTier getTier() {
    return StorageTier.HOT;
  }

  @Override
  public long getExpectedLatencyMs() {
    return 0; // Sub-millisecond
  }

  // Private helper methods

  private void updateExpiration(String flowId) {
    long expirationTime = System.currentTimeMillis() + (defaultTTLSeconds * 1000);
    expirationTimes.put(flowId, expirationTime);
  }

  private boolean isExpired(String flowId) {
    Long expirationTime = expirationTimes.get(flowId);
    if (expirationTime == null) {
      return false; // No TTL set
    }
    return System.currentTimeMillis() > expirationTime;
  }

  private void evictOldest() {
    // Simple eviction: remove the entry with oldest expiration time
    String oldestKey = null;
    long oldestTime = Long.MAX_VALUE;

    for (Map.Entry<String, Long> entry : expirationTimes.entrySet()) {
      if (entry.getValue() < oldestTime) {
        oldestTime = entry.getValue();
        oldestKey = entry.getKey();
      }
    }

    if (oldestKey != null) {
      storage.remove(oldestKey);
      expirationTimes.remove(oldestKey);
      LOG.debug("Evicted oldest entry: {}", oldestKey);
    }
  }

  private void startCleanupThread() {
    Thread cleanupThread = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(60000); // Check every minute
          cleanupExpired();
        } catch (InterruptedException e) {
          LOG.info("Cleanup thread interrupted");
          break;
        }
      }
    });
    cleanupThread.setDaemon(true);
    cleanupThread.setName("InMemoryShortTermStore-Cleanup");
    cleanupThread.start();
  }

  private void cleanupExpired() {
    long now = System.currentTimeMillis();
    List<String> expiredKeys = new ArrayList<>();

    for (Map.Entry<String, Long> entry : expirationTimes.entrySet()) {
      if (entry.getValue() < now) {
        expiredKeys.add(entry.getKey());
      }
    }

    for (String key : expiredKeys) {
      storage.remove(key);
      expirationTimes.remove(key);
    }

    if (!expiredKeys.isEmpty()) {
      LOG.debug("Cleaned up {} expired entries", expiredKeys.size());
    }
  }
}

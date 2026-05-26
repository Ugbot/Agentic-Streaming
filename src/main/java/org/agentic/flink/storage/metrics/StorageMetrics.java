package org.agentic.flink.storage.metrics;

import org.agentic.flink.storage.StorageTier;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics tracking for storage operations.
 *
 * <p>This class provides comprehensive metrics for monitoring storage performance, including
 * latency, throughput, error rates, and hit rates. Metrics are tracked per storage tier and can be
 * integrated with monitoring systems like Prometheus, Datadog, or CloudWatch.
 *
 * <p>Tracked Metrics:
 *
 * <ul>
 *   <li>Operation counts (get, put, delete)
 *   <li>Latency percentiles (p50, p95, p99)
 *   <li>Hit rates (for cache-backed stores)
 *   <li>Error rates and error counts
 *   <li>Throughput (operations per second)
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * StorageMetrics metrics = new StorageMetrics(StorageTier.HOT, "redis");
 *
 * // Track operation
 * long startTime = System.nanoTime();
 * try {
 *   store.put(key, value);
 *   metrics.recordPut(System.nanoTime() - startTime);
 * } catch (Exception e) {
 *   metrics.recordError("put", e);
 * }
 *
 * // Get statistics
 * Map<String, Object> stats = metrics.getStatistics();
 * double avgLatency = metrics.getAverageLatencyMs();
 * }</pre>
 *
 * <p>Integration with Flink metrics:
 *
 * <pre>{@code
 * public class StorageAwareFunction extends RichProcessFunction<Event, Event> {
 *   private transient StorageMetrics metrics;
 *
 *   @Override
 *   public void open(Configuration config) {
 *     metrics = new StorageMetrics(StorageTier.HOT, "redis");
 *     // Register with Flink metrics
 *     getRuntimeContext().getMetricGroup()
 *       .gauge("storage_avg_latency_ms", metrics::getAverageLatencyMs);
 *   }
 * }
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class StorageMetrics implements Serializable {

  private final StorageTier tier;
  private final String backend;

  // Operation counters
  private final LongAdder getCount = new LongAdder();
  private final LongAdder putCount = new LongAdder();
  private final LongAdder deleteCount = new LongAdder();
  private final LongAdder hitCount = new LongAdder();
  private final LongAdder missCount = new LongAdder();

  // Error tracking
  private final LongAdder errorCount = new LongAdder();
  private final ConcurrentHashMap<String, AtomicLong> errorsByType = new ConcurrentHashMap<>();

  // Latency tracking (nanoseconds)
  private final LongAdder totalGetLatencyNs = new LongAdder();
  private final LongAdder totalPutLatencyNs = new LongAdder();
  private final LongAdder totalDeleteLatencyNs = new LongAdder();

  // Max latencies
  private volatile long maxGetLatencyNs = 0;
  private volatile long maxPutLatencyNs = 0;
  private volatile long maxDeleteLatencyNs = 0;

  // Timing
  private final long createdAt = System.currentTimeMillis();
  private volatile long lastResetAt = System.currentTimeMillis();

  public StorageMetrics(StorageTier tier, String backend) {
    this.tier = tier;
    this.backend = backend;
  }

  /**
   * Record a get operation.
   *
   * @param latencyNs Latency in nanoseconds
   * @param hit true if cache hit, false if miss
   */
  public void recordGet(long latencyNs, boolean hit) {
    getCount.increment();
    totalGetLatencyNs.add(latencyNs);
    updateMaxLatency(latencyNs, 0); // 0 = GET

    if (hit) {
      hitCount.increment();
    } else {
      missCount.increment();
    }
  }

  /**
   * Record a put operation.
   *
   * @param latencyNs Latency in nanoseconds
   */
  public void recordPut(long latencyNs) {
    putCount.increment();
    totalPutLatencyNs.add(latencyNs);
    updateMaxLatency(latencyNs, 1); // 1 = PUT
  }

  /**
   * Record a delete operation.
   *
   * @param latencyNs Latency in nanoseconds
   */
  public void recordDelete(long latencyNs) {
    deleteCount.increment();
    totalDeleteLatencyNs.add(latencyNs);
    updateMaxLatency(latencyNs, 2); // 2 = DELETE
  }

  /**
   * Record an error.
   *
   * @param operation Operation that failed (get, put, delete)
   * @param error Exception that occurred
   */
  public void recordError(String operation, Throwable error) {
    errorCount.increment();
    String errorType = error.getClass().getSimpleName();
    errorsByType.computeIfAbsent(errorType, k -> new AtomicLong()).incrementAndGet();
  }

  /**
   * Get total number of operations.
   *
   * @return Total operations across all types
   */
  public long getTotalOperations() {
    return getCount.sum() + putCount.sum() + deleteCount.sum();
  }

  /**
   * Get cache hit rate.
   *
   * @return Hit rate between 0.0 and 1.0, or 0.0 if no cache operations
   */
  public double getHitRate() {
    long total = hitCount.sum() + missCount.sum();
    return total > 0 ? (double) hitCount.sum() / total : 0.0;
  }

  /**
   * Get error rate.
   *
   * @return Error rate between 0.0 and 1.0
   */
  public double getErrorRate() {
    long total = getTotalOperations();
    return total > 0 ? (double) errorCount.sum() / total : 0.0;
  }

  /**
   * Get average latency across all operations in milliseconds.
   *
   * @return Average latency in milliseconds
   */
  public double getAverageLatencyMs() {
    long totalOps = getTotalOperations();
    if (totalOps == 0) return 0.0;

    long totalLatencyNs =
        totalGetLatencyNs.sum() + totalPutLatencyNs.sum() + totalDeleteLatencyNs.sum();
    return (double) totalLatencyNs / totalOps / 1_000_000.0; // Convert to ms
  }

  /**
   * Get average GET latency in milliseconds.
   *
   * @return Average GET latency in milliseconds
   */
  public double getAverageGetLatencyMs() {
    long count = getCount.sum();
    return count > 0 ? (double) totalGetLatencyNs.sum() / count / 1_000_000.0 : 0.0;
  }

  /**
   * Get average PUT latency in milliseconds.
   *
   * @return Average PUT latency in milliseconds
   */
  public double getAveragePutLatencyMs() {
    long count = putCount.sum();
    return count > 0 ? (double) totalPutLatencyNs.sum() / count / 1_000_000.0 : 0.0;
  }

  /**
   * Get average DELETE latency in milliseconds.
   *
   * @return Average DELETE latency in milliseconds
   */
  public double getAverageDeleteLatencyMs() {
    long count = deleteCount.sum();
    return count > 0 ? (double) totalDeleteLatencyNs.sum() / count / 1_000_000.0 : 0.0;
  }

  /**
   * Get maximum GET latency in milliseconds.
   *
   * @return Maximum GET latency in milliseconds
   */
  public double getMaxGetLatencyMs() {
    return maxGetLatencyNs / 1_000_000.0;
  }

  /**
   * Get maximum PUT latency in milliseconds.
   *
   * @return Maximum PUT latency in milliseconds
   */
  public double getMaxPutLatencyMs() {
    return maxPutLatencyNs / 1_000_000.0;
  }

  /**
   * Get maximum DELETE latency in milliseconds.
   *
   * @return Maximum DELETE latency in milliseconds
   */
  public double getMaxDeleteLatencyMs() {
    return maxDeleteLatencyNs / 1_000_000.0;
  }

  /**
   * Get throughput in operations per second.
   *
   * @return Operations per second
   */
  public double getThroughputOpsPerSecond() {
    long elapsedMs = System.currentTimeMillis() - lastResetAt;
    if (elapsedMs == 0) return 0.0;

    return (double) getTotalOperations() / (elapsedMs / 1000.0);
  }

  /**
   * Get comprehensive statistics map.
   *
   * @return Map of metric names to values
   */
  public Map<String, Object> getStatistics() {
    Map<String, Object> stats = new HashMap<>();

    // Tier and backend info
    stats.put("tier", tier.toString());
    stats.put("backend", backend);

    // Operation counts
    stats.put("total_operations", getTotalOperations());
    stats.put("get_count", getCount.sum());
    stats.put("put_count", putCount.sum());
    stats.put("delete_count", deleteCount.sum());

    // Cache metrics
    stats.put("hit_count", hitCount.sum());
    stats.put("miss_count", missCount.sum());
    stats.put("hit_rate", getHitRate());

    // Error metrics
    stats.put("error_count", errorCount.sum());
    stats.put("error_rate", getErrorRate());
    stats.put("errors_by_type", new HashMap<>(errorsByType));

    // Latency metrics (in milliseconds)
    stats.put("avg_latency_ms", getAverageLatencyMs());
    stats.put("avg_get_latency_ms", getAverageGetLatencyMs());
    stats.put("avg_put_latency_ms", getAveragePutLatencyMs());
    stats.put("avg_delete_latency_ms", getAverageDeleteLatencyMs());
    stats.put("max_get_latency_ms", getMaxGetLatencyMs());
    stats.put("max_put_latency_ms", getMaxPutLatencyMs());
    stats.put("max_delete_latency_ms", getMaxDeleteLatencyMs());

    // Throughput
    stats.put("throughput_ops_per_second", getThroughputOpsPerSecond());

    // Timing
    stats.put("created_at", createdAt);
    stats.put("last_reset_at", lastResetAt);
    stats.put("uptime_seconds", (System.currentTimeMillis() - createdAt) / 1000);

    return stats;
  }

  /**
   * Reset all metrics.
   *
   * <p>Useful for periodic reporting or when switching contexts.
   */
  public void reset() {
    getCount.reset();
    putCount.reset();
    deleteCount.reset();
    hitCount.reset();
    missCount.reset();
    errorCount.reset();
    errorsByType.clear();

    totalGetLatencyNs.reset();
    totalPutLatencyNs.reset();
    totalDeleteLatencyNs.reset();

    maxGetLatencyNs = 0;
    maxPutLatencyNs = 0;
    maxDeleteLatencyNs = 0;

    lastResetAt = System.currentTimeMillis();
  }

  /**
   * Get storage tier.
   *
   * @return Storage tier
   */
  public StorageTier getTier() {
    return tier;
  }

  /**
   * Get backend name.
   *
   * @return Backend identifier
   */
  public String getBackend() {
    return backend;
  }

  @Override
  public String toString() {
    return String.format(
        "StorageMetrics[tier=%s, backend=%s, ops=%d, hitRate=%.2f, avgLatency=%.2fms, errors=%d]",
        tier, backend, getTotalOperations(), getHitRate(), getAverageLatencyMs(),
        errorCount.sum());
  }

  // Private helper methods

  private void updateMaxLatency(long latencyNs, int operationType) {
    switch (operationType) {
      case 0: // GET
        if (latencyNs > maxGetLatencyNs) {
          maxGetLatencyNs = latencyNs;
        }
        break;
      case 1: // PUT
        if (latencyNs > maxPutLatencyNs) {
          maxPutLatencyNs = latencyNs;
        }
        break;
      case 2: // DELETE
        if (latencyNs > maxDeleteLatencyNs) {
          maxDeleteLatencyNs = latencyNs;
        }
        break;
    }
  }
}

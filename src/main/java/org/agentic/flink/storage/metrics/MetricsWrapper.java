package org.agentic.flink.storage.metrics;

import org.agentic.flink.storage.StorageProvider;
import org.agentic.flink.storage.StorageTier;
import java.util.Map;
import java.util.Optional;

/**
 * Wrapper that adds metrics tracking to any StorageProvider.
 *
 * <p>This class uses the decorator pattern to transparently add metrics collection to storage
 * operations without modifying the underlying storage implementation.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Create storage provider
 * ShortTermMemoryStore store = StorageFactory.createShortTermStore("redis", config);
 *
 * // Wrap with metrics
 * MetricsWrapper<String, List<ContextItem>> metricsStore =
 *     new MetricsWrapper<>(store);
 *
 * // Use normally - metrics are collected automatically
 * metricsStore.put("flow-001", items);
 * Optional<List<ContextItem>> result = metricsStore.get("flow-001");
 *
 * // Get metrics
 * StorageMetrics metrics = metricsStore.getMetrics();
 * double avgLatency = metrics.getAverageLatencyMs();
 * }</pre>
 *
 * @param <K> Key type
 * @param <V> Value type
 * @author Agentic Flink Team
 */
public class MetricsWrapper<K, V> implements StorageProvider<K, V> {

  private final StorageProvider<K, V> delegate;
  private final StorageMetrics metrics;

  /**
   * Create a metrics wrapper for a storage provider.
   *
   * @param delegate Storage provider to wrap
   */
  public MetricsWrapper(StorageProvider<K, V> delegate) {
    this.delegate = delegate;
    this.metrics = new StorageMetrics(delegate.getTier(), delegate.getProviderName());
  }

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    delegate.initialize(config);
  }

  @Override
  public void put(K key, V value) throws Exception {
    long startTime = System.nanoTime();
    try {
      delegate.put(key, value);
      long latency = System.nanoTime() - startTime;
      metrics.recordPut(latency);
    } catch (Exception e) {
      metrics.recordError("put", e);
      throw e;
    }
  }

  @Override
  public Optional<V> get(K key) throws Exception {
    long startTime = System.nanoTime();
    try {
      Optional<V> result = delegate.get(key);
      long latency = System.nanoTime() - startTime;
      metrics.recordGet(latency, result.isPresent());
      return result;
    } catch (Exception e) {
      metrics.recordError("get", e);
      throw e;
    }
  }

  @Override
  public void delete(K key) throws Exception {
    long startTime = System.nanoTime();
    try {
      delegate.delete(key);
      long latency = System.nanoTime() - startTime;
      metrics.recordDelete(latency);
    } catch (Exception e) {
      metrics.recordError("delete", e);
      throw e;
    }
  }

  @Override
  public boolean exists(K key) throws Exception {
    // exists() is typically a fast check, track as get operation
    long startTime = System.nanoTime();
    try {
      boolean result = delegate.exists(key);
      long latency = System.nanoTime() - startTime;
      metrics.recordGet(latency, result);
      return result;
    } catch (Exception e) {
      metrics.recordError("exists", e);
      throw e;
    }
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public StorageTier getTier() {
    return delegate.getTier();
  }

  @Override
  public long getExpectedLatencyMs() {
    return delegate.getExpectedLatencyMs();
  }

  @Override
  public String getProviderName() {
    return delegate.getProviderName() + " (with metrics)";
  }

  /**
   * Get the metrics collector for this storage provider.
   *
   * @return StorageMetrics instance
   */
  public StorageMetrics getMetrics() {
    return metrics;
  }

  /**
   * Get the underlying storage provider (unwrapped).
   *
   * @return Delegate storage provider
   */
  public StorageProvider<K, V> getDelegate() {
    return delegate;
  }
}

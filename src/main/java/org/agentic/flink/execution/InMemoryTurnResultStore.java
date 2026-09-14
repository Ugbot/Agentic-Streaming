package org.agentic.flink.execution;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded, TTL-expiring in-memory {@link TurnResultStore}.
 *
 * <p>Entries are evicted in insertion order once {@code maxEntries} is exceeded and are dropped
 * on read once older than {@code ttl}. The store lives in the operator JVM: it dedups turns that
 * are redelivered while the operator is running (CEP re-firing a match, a retried async request)
 * but does not survive a job restart. Restart-safe dedup of dispatched turns is done by the
 * keyed state in {@code org.agentic.flink.job.AgentExecutionFunction}.
 *
 * @deprecated Part of the legacy Flink DSL execution path. See
 *     {@code org.agentic.flink.runtime.WorkflowTurnFunction} for the event-sourced runtime.
 */
@Deprecated
public final class InMemoryTurnResultStore implements TurnResultStore {

  private static final long serialVersionUID = 1L;

  public static final int DEFAULT_MAX_ENTRIES = 10_000;
  public static final Duration DEFAULT_TTL = Duration.ofHours(1);

  private final int maxEntries;
  private final long ttlMillis;
  private transient LongSupplier clock;
  private transient LinkedHashMap<String, Timestamped<ExecutionResult>> turns;
  private transient LinkedHashMap<String, Timestamped<ToolCallResult>> tools;

  public InMemoryTurnResultStore() {
    this(DEFAULT_MAX_ENTRIES, DEFAULT_TTL);
  }

  public InMemoryTurnResultStore(int maxEntries, Duration ttl) {
    this(maxEntries, ttl, System::currentTimeMillis);
  }

  InMemoryTurnResultStore(int maxEntries, Duration ttl, LongSupplier clock) {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be positive, got " + maxEntries);
    }
    if (ttl == null || ttl.isNegative() || ttl.isZero()) {
      throw new IllegalArgumentException("ttl must be positive, got " + ttl);
    }
    this.maxEntries = maxEntries;
    this.ttlMillis = ttl.toMillis();
    this.clock = clock;
    initMaps();
  }

  private void initMaps() {
    this.turns = new LinkedHashMap<>();
    this.tools = new LinkedHashMap<>();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.clock = System::currentTimeMillis;
    initMaps();
  }

  @Override
  public synchronized Optional<ExecutionResult> getTurnResult(String turnId) {
    return get(turns, turnId);
  }

  @Override
  public synchronized void putTurnResult(String turnId, ExecutionResult result) {
    put(turns, turnId, result);
  }

  @Override
  public synchronized Optional<ToolCallResult> getToolResult(String turnId, int callIndex) {
    return get(tools, toolKey(turnId, callIndex));
  }

  @Override
  public synchronized void putToolResult(String turnId, int callIndex, ToolCallResult result) {
    put(tools, toolKey(turnId, callIndex), result);
  }

  @Override
  public synchronized int size() {
    expire(turns);
    return turns.size();
  }

  public int getMaxEntries() {
    return maxEntries;
  }

  public Duration getTtl() {
    return Duration.ofMillis(ttlMillis);
  }

  private static String toolKey(String turnId, int callIndex) {
    return turnId + "#" + callIndex;
  }

  private <V> Optional<V> get(LinkedHashMap<String, Timestamped<V>> map, String key) {
    Timestamped<V> entry = map.get(key);
    if (entry == null) {
      return Optional.empty();
    }
    if (isExpired(entry)) {
      map.remove(key);
      return Optional.empty();
    }
    return Optional.of(entry.value);
  }

  private <V> void put(LinkedHashMap<String, Timestamped<V>> map, String key, V value) {
    map.remove(key);
    map.put(key, new Timestamped<>(value, clock.getAsLong()));
    expire(map);
    Iterator<String> it = map.keySet().iterator();
    while (map.size() > maxEntries && it.hasNext()) {
      it.next();
      it.remove();
    }
  }

  private <V> void expire(LinkedHashMap<String, Timestamped<V>> map) {
    Iterator<Map.Entry<String, Timestamped<V>>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Timestamped<V>> e = it.next();
      if (isExpired(e.getValue())) {
        it.remove();
      } else {
        break;
      }
    }
  }

  private boolean isExpired(Timestamped<?> entry) {
    return clock.getAsLong() - entry.recordedAt >= ttlMillis;
  }

  private static final class Timestamped<V> {
    final V value;
    final long recordedAt;

    Timestamped(V value, long recordedAt) {
      this.value = value;
      this.recordedAt = recordedAt;
    }
  }
}

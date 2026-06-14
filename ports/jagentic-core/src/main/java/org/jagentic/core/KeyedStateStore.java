package org.jagentic.core;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The portable form of Flink keyed {@code ValueState} — a per-(key,name) scalar
 * slot. Engine adapters back this with native keyed state (Kafka Streams
 * {@code KeyValueStore}, a Ray actor field) or Redis.
 */
public interface KeyedStateStore {
  Optional<Object> get(String key, String name);

  void put(String key, String name, Object value);

  void clear(String key);

  /** Process-local default. */
  final class InMemory implements KeyedStateStore {
    private final Map<String, Map<String, Object>> d = new ConcurrentHashMap<>();

    @Override
    public Optional<Object> get(String key, String name) {
      return Optional.ofNullable(d.getOrDefault(key, Map.of()).get(name));
    }

    @Override
    public void put(String key, String name, Object value) {
      d.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(name, value);
    }

    @Override
    public void clear(String key) {
      d.remove(key);
    }
  }
}

package org.agentic.flink.inference;

import java.lang.ref.SoftReference;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Per-JVM cache of loaded model handles, keyed on {@code (modelUri, deviceType)}.
 *
 * <p>Two operators in the same task slot pointing at the same model file should share weights.
 * Entries are wrapped in {@link SoftReference}s so the JVM can reclaim under memory pressure
 * without our intervention.
 *
 * <p>The cache deliberately stores opaque {@link Object}s — backends decide what they want to
 * cache (a DJL {@code Model}, an ONNX {@code OrtSession}, a TF saved-model handle, etc.).
 * Type-safe access is the caller's responsibility.
 */
public final class InferenceModelCache {

  private static final InferenceModelCache INSTANCE = new InferenceModelCache();

  private final ConcurrentMap<Key, SoftReference<Object>> entries = new ConcurrentHashMap<>();

  private InferenceModelCache() {}

  public static InferenceModelCache global() {
    return INSTANCE;
  }

  /**
   * Get or compute a model handle for the given key. The supplier runs at most once per concrete
   * key while the {@link SoftReference} is live; if the JVM has reclaimed the reference, the
   * supplier runs again on the next call.
   */
  @SuppressWarnings("unchecked")
  public <T> T computeIfAbsent(String modelUri, String deviceType, Supplier<T> loader) {
    Objects.requireNonNull(modelUri, "modelUri");
    Objects.requireNonNull(loader, "loader");
    Key key = new Key(modelUri, deviceType == null ? "auto" : deviceType);

    SoftReference<Object> ref = entries.get(key);
    Object value = ref == null ? null : ref.get();
    if (value != null) {
      return (T) value;
    }
    synchronized (key.lock()) {
      ref = entries.get(key);
      value = ref == null ? null : ref.get();
      if (value != null) {
        return (T) value;
      }
      T loaded = loader.get();
      entries.put(key, new SoftReference<>(loaded));
      return loaded;
    }
  }

  /** For tests only. */
  public void clear() {
    entries.clear();
  }

  /** For tests only. */
  public int size() {
    return entries.size();
  }

  private static final class Key {
    private final String modelUri;
    private final String deviceType;
    private final Object lock = new Object();

    Key(String modelUri, String deviceType) {
      this.modelUri = modelUri;
      this.deviceType = deviceType;
    }

    Object lock() {
      // Lock on a string interned form so two equal keys share the same monitor — the field-
      // level Object lock above is only used inside the same Key instance.
      return (modelUri + "|" + deviceType).intern();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Key)) return false;
      Key k = (Key) o;
      return modelUri.equals(k.modelUri) && deviceType.equals(k.deviceType);
    }

    @Override
    public int hashCode() {
      return modelUri.hashCode() * 31 + deviceType.hashCode();
    }
  }
}

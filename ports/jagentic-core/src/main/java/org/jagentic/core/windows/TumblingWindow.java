package org.jagentic.core.windows;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Keyed tumbling (fixed, non-overlapping) time-window aggregate. Events accumulate into the bucket
 * {@code floor(ts / windowMillis)}; when an event arrives in a later bucket, the previous bucket
 * <b>closes</b> and is emitted. {@link #close(String)} flushes the open bucket (end of stream).
 */
public final class TumblingWindow {

  public record Bucket(String key, long start, int count, double sum) {}

  private static final class Open {
    long index;
    int count;
    double sum;
  }

  private final long windowMillis;
  private final Map<String, Open> byKey = new HashMap<>();

  public TumblingWindow(long windowMillis) {
    this.windowMillis = windowMillis;
  }

  /** Add an event; if it falls in a later bucket than the open one, emit (and start) — else empty. */
  public synchronized Optional<Bucket> add(String key, long ts, double value) {
    long index = Math.floorDiv(ts, windowMillis);
    Open open = byKey.get(key);
    Optional<Bucket> emitted = Optional.empty();
    if (open == null) {
      open = new Open();
      open.index = index;
      byKey.put(key, open);
    } else if (index > open.index) {
      emitted = Optional.of(new Bucket(key, open.index * windowMillis, open.count, open.sum));
      open.index = index;
      open.count = 0;
      open.sum = 0;
    }
    open.count++;
    open.sum += value;
    return emitted;
  }

  public Optional<Bucket> add(String key, long ts) {
    return add(key, ts, 1.0);
  }

  /** Flush the currently-open bucket for a key (returns empty if none open). */
  public synchronized Optional<Bucket> close(String key) {
    Open open = byKey.remove(key);
    if (open == null) {
      return Optional.empty();
    }
    return Optional.of(new Bucket(key, open.index * windowMillis, open.count, open.sum));
  }
}

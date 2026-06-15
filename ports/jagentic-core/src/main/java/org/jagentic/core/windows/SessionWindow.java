package org.jagentic.core.windows;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Keyed session window — groups events for a key into sessions separated by an inactivity
 * {@code gapMillis}. An event arriving more than {@code gapMillis} after the previous one closes the
 * prior session (emitted) and starts a new one. {@link #close(String)} flushes the open session.
 */
public final class SessionWindow {

  public record Session(String key, long start, long end, int count, double sum) {}

  private static final class Open {
    long start;
    long last;
    int count;
    double sum;
  }

  private final long gapMillis;
  private final Map<String, Open> byKey = new HashMap<>();

  public SessionWindow(long gapMillis) {
    this.gapMillis = gapMillis;
  }

  public synchronized Optional<Session> add(String key, long ts, double value) {
    Open open = byKey.get(key);
    Optional<Session> emitted = Optional.empty();
    if (open != null && ts - open.last > gapMillis) {
      emitted = Optional.of(new Session(key, open.start, open.last, open.count, open.sum));
      open = null;
    }
    if (open == null) {
      open = new Open();
      open.start = ts;
      byKey.put(key, open);
    }
    open.last = ts;
    open.count++;
    open.sum += value;
    return emitted;
  }

  public Optional<Session> add(String key, long ts) {
    return add(key, ts, 1.0);
  }

  public synchronized Optional<Session> close(String key) {
    Open open = byKey.remove(key);
    if (open == null) {
      return Optional.empty();
    }
    return Optional.of(new Session(key, open.start, open.last, open.count, open.sum));
  }
}

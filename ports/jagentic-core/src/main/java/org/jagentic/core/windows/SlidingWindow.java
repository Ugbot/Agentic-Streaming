package org.jagentic.core.windows;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Keyed sliding time-window aggregate — the portable {@code VelocityDetector}: "how many events (and
 * what summed value) for this key in the last {@code windowMillis}". Each {@link #add} evicts events
 * older than {@code ts - windowMillis} and returns the current windowed {@link WindowState}, so a
 * caller fires when {@code count >= threshold} ("5 payments on one account in 60s").
 */
public final class SlidingWindow {

  private record Entry(long ts, double value) {}

  private final long windowMillis;
  private final Map<String, Deque<Entry>> byKey = new HashMap<>();

  public SlidingWindow(long windowMillis) {
    this.windowMillis = windowMillis;
  }

  /** Record an event for {@code key} at {@code ts}; return count + sum within (ts-window, ts]. */
  public synchronized WindowState add(String key, long ts, double value) {
    Deque<Entry> q = byKey.computeIfAbsent(key, k -> new ArrayDeque<>());
    q.addLast(new Entry(ts, value));
    long cutoff = ts - windowMillis;
    while (!q.isEmpty() && q.peekFirst().ts() <= cutoff) {
      q.pollFirst();
    }
    int count = q.size();
    double sum = 0;
    for (Entry e : q) {
      sum += e.value();
    }
    return new WindowState(count, sum);
  }

  /** Convenience: count-only event. */
  public WindowState add(String key, long ts) {
    return add(key, ts, 1.0);
  }
}

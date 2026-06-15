package org.jagentic.core.timers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import org.jagentic.core.Event;

/** Process-local timers backed by an insertion-ordered map; due timers come out ascending by fireAt,
 * with schedule order as the stable tie-break. */
public final class InMemoryTimerService implements TimerService {

  private final LinkedHashMap<String, Timer> timers = new LinkedHashMap<>();

  @Override
  public synchronized void schedule(String id, long fireAt, Event payload) {
    timers.remove(id); // re-insert so a replaced timer takes the new schedule order
    timers.put(id, new Timer(id, fireAt, payload));
  }

  @Override
  public synchronized boolean cancel(String id) {
    return timers.remove(id) != null;
  }

  @Override
  public synchronized List<Timer> advanceTo(long now) {
    List<Timer> due = new ArrayList<>();
    for (Timer t : timers.values()) {
      if (t.fireAt() <= now) {
        due.add(t);
      }
    }
    due.sort(Comparator.comparingLong(Timer::fireAt)); // stable → equal fireAt keeps schedule order
    for (Timer t : due) {
      timers.remove(t.id());
    }
    return due;
  }

  @Override
  public synchronized Optional<Long> nextDeadline() {
    long min = Long.MAX_VALUE;
    for (Timer t : timers.values()) {
      min = Math.min(min, t.fireAt());
    }
    return timers.isEmpty() ? Optional.empty() : Optional.of(min);
  }

  /** Snapshot of pending timers in schedule order (for durable persistence). */
  synchronized List<Timer> pending() {
    return new ArrayList<>(timers.values());
  }

  synchronized void restoreAll(List<Timer> restored) {
    for (Timer t : restored) {
      timers.put(t.id(), t);
    }
  }
}

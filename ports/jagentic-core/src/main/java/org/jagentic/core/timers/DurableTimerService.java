package org.jagentic.core.timers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;

/**
 * A {@link TimerService} that persists its pending set through a {@link KeyedStateStore}, so timers
 * survive a restart (with a durable store backing — Redis, etc.). Pending timers are written to one
 * scalar slot as text; {@link #restore()} reloads them. Schedule/cancel/advance keep the slot current.
 *
 * <p>The serialization is per-process self-consistent (it is never read across languages), so each
 * core port may encode it idiomatically; behaviour — "a scheduled timer survives restore and fires" —
 * is what stays at parity.</p>
 */
public final class DurableTimerService implements TimerService {

  private final InMemoryTimerService delegate = new InMemoryTimerService();
  private final KeyedStateStore store;
  private final String slotKey;
  private final String slotName;

  public DurableTimerService(KeyedStateStore store) {
    this(store, "__timers__", "pending");
  }

  public DurableTimerService(KeyedStateStore store, String slotKey, String slotName) {
    this.store = store;
    this.slotKey = slotKey;
    this.slotName = slotName;
  }

  @Override
  public synchronized void schedule(String id, long fireAt, Event payload) {
    delegate.schedule(id, fireAt, payload);
    persist();
  }

  @Override
  public synchronized boolean cancel(String id) {
    boolean removed = delegate.cancel(id);
    if (removed) {
      persist();
    }
    return removed;
  }

  @Override
  public synchronized List<Timer> advanceTo(long now) {
    List<Timer> due = delegate.advanceTo(now);
    if (!due.isEmpty()) {
      persist();
    }
    return due;
  }

  @Override
  public synchronized Optional<Long> nextDeadline() {
    return delegate.nextDeadline();
  }

  /** Reload pending timers from the store into this service (call after a restart). */
  public synchronized void restore() {
    Object raw = store.get(slotKey, slotName).orElse(null);
    if (raw == null) {
      return;
    }
    delegate.restoreAll(decode(raw.toString()));
  }

  private void persist() {
    store.put(slotKey, slotName, encode(delegate.pending()));
  }

  // ---- serialization: one timer per line, fields tab-separated, text base64'd ----

  private static String encode(List<Timer> timers) {
    StringBuilder sb = new StringBuilder();
    for (Timer t : timers) {
      Event e = t.payload();
      sb.append(t.id()).append('\t')
        .append(t.fireAt()).append('\t')
        .append(b64(e.conversationId())).append('\t')
        .append(b64(e.userId())).append('\t')
        .append(b64(e.text())).append('\n');
    }
    return sb.toString();
  }

  private static List<Timer> decode(String s) {
    List<Timer> out = new ArrayList<>();
    for (String line : s.split("\n")) {
      if (line.isEmpty()) {
        continue;
      }
      String[] f = line.split("\t", -1);
      if (f.length < 5) {
        continue;
      }
      Event e = new Event(unb64(f[2]), unb64(f[3]), unb64(f[4]), Map.of());
      out.add(new Timer(f[0], Long.parseLong(f[1]), e));
    }
    return out;
  }

  private static String b64(String s) {
    return Base64.getEncoder().encodeToString((s == null ? "" : s).getBytes(StandardCharsets.UTF_8));
  }

  private static String unb64(String s) {
    return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
  }
}

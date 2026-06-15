package org.jagentic.core.suspend;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks which conversations are suspended awaiting input. The InMemory default is process-local;
 * a durable impl can persist via the {@code KeyedStateStore} SPI (as the timer service does), so a
 * suspended turn survives restart. */
public interface SuspensionService {

  void suspend(String conversationId, String reason, String pendingText, long now);

  boolean isSuspended(String conversationId);

  /** The pending suspension without clearing it. */
  Optional<Suspension> peek(String conversationId);

  /** Remove and return the pending suspension (the resume command). */
  Optional<Suspension> clear(String conversationId);

  /** All currently-suspended conversations (for timeout sweeps). */
  List<Suspension> allPending();

  final class InMemory implements SuspensionService {
    private final Map<String, Suspension> byKey = new ConcurrentHashMap<>();

    @Override
    public void suspend(String conversationId, String reason, String pendingText, long now) {
      byKey.put(conversationId, new Suspension(conversationId, reason, pendingText, now));
    }

    @Override
    public boolean isSuspended(String conversationId) {
      return byKey.containsKey(conversationId);
    }

    @Override
    public Optional<Suspension> peek(String conversationId) {
      return Optional.ofNullable(byKey.get(conversationId));
    }

    @Override
    public Optional<Suspension> clear(String conversationId) {
      return Optional.ofNullable(byKey.remove(conversationId));
    }

    @Override
    public List<Suspension> allPending() {
      return new ArrayList<>(byKey.values());
    }
  }
}

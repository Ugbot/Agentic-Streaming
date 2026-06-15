package org.jagentic.core.replay;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jagentic.core.Event;

/**
 * The append-only log of inbound events — the source of truth the agent's state is a materialized
 * view over. Recording is free via the {@code StreamRuntime} observer seam
 * ({@code stream.observe(log::record)}); {@link org.jagentic.core.replay.Replayer} re-materializes
 * state by replaying it (debug, eval, migration, "what would the new graph version have done").
 */
public interface EventLog {

  void record(Event event);

  /** All recorded events, in arrival order. */
  List<Event> events();

  /** Recorded events for one conversation, in arrival order. */
  List<Event> eventsFor(String conversationId);

  /** Process-local default. */
  final class InMemory implements EventLog {
    private final List<Event> all = new ArrayList<>();
    private final Map<String, List<Event>> byKey = new ConcurrentHashMap<>();

    @Override
    public synchronized void record(Event event) {
      all.add(event);
      byKey.computeIfAbsent(event.conversationId(), k -> new ArrayList<>()).add(event);
    }

    @Override
    public synchronized List<Event> events() {
      return List.copyOf(all);
    }

    @Override
    public List<Event> eventsFor(String conversationId) {
      return List.copyOf(byKey.getOrDefault(conversationId, List.of()));
    }
  }
}

package org.jagentic.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The per-conversation event log: the single source of truth for conversation state. Every runtime
 * event is appended here with a dense, zero-based, monotonic sequence number; {@link
 * ConversationState#fold(List)} over {@link #events(String)} is the only definition of state.
 *
 * <p>Implementations are single-writer per conversation; the runtime serialises appends with its
 * per-conversation lock, and {@code append} is additionally atomic on its own.</p>
 */
public interface ConversationLog {

  /** Appends and returns the stored event with its assigned sequence. */
  LogEvent append(String conversationId, String turnId, EventType type, Map<String, Object> payload);

  /** All events of one conversation in sequence order (empty if unknown). */
  List<LogEvent> events(String conversationId);

  /** Conversation ids that have at least one event, sorted. */
  Set<String> conversations();

  /** Folds the log of one conversation. */
  default ConversationState state(String conversationId) {
    return ConversationState.fold(events(conversationId));
  }

  /** Folds the log of one conversation under the workflow's {@code context} window. */
  default ConversationState state(String conversationId, ContextWindow window) {
    return ConversationState.fold(events(conversationId), window);
  }

  /** In-process, serializable log; the default for the local runtime. */
  final class InMemory implements ConversationLog, Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, List<LogEvent>> byConversation = new LinkedHashMap<>();

    @Override
    public synchronized LogEvent append(String conversationId, String turnId, EventType type,
                                        Map<String, Object> payload) {
      List<LogEvent> log = byConversation.computeIfAbsent(conversationId, k -> new ArrayList<>());
      LogEvent e = new LogEvent(conversationId, log.size(), turnId, type.wire(),
          payload == null ? Map.of() : new LinkedHashMap<>(payload));
      log.add(e);
      return e;
    }

    @Override
    public synchronized List<LogEvent> events(String conversationId) {
      List<LogEvent> log = byConversation.get(conversationId);
      return log == null ? List.of() : List.copyOf(log);
    }

    @Override
    public synchronized Set<String> conversations() {
      return new TreeSet<>(byConversation.keySet());
    }
  }
}

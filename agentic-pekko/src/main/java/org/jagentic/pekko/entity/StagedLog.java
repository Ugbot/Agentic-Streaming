package org.jagentic.pekko.entity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;

/**
 * The {@link ConversationLog} a turn runs against inside the entity: the journal already folded
 * into the entity state is the immutable prefix, and everything the graph appends during the turn
 * is staged in order behind it. Sequences continue densely from the prefix, so once the entity
 * persists the staged events the journal is exactly the log the graph observed.
 */
final class StagedLog implements ConversationLog {
  private final String conversationId;
  private final List<LogEvent> committed;
  private final List<LogEvent> staged = new ArrayList<>();

  StagedLog(String conversationId, List<LogEvent> committed) {
    this.conversationId = conversationId;
    this.committed = List.copyOf(committed);
  }

  @Override
  public synchronized LogEvent append(String cid, String turnId, EventType type, Map<String, Object> payload) {
    if (!conversationId.equals(cid)) {
      throw new IllegalArgumentException("entity " + conversationId + " cannot append to conversation " + cid);
    }
    LogEvent e = new LogEvent(cid, committed.size() + staged.size(), turnId, type.wire(),
        payload == null ? Map.of() : new LinkedHashMap<>(payload));
    staged.add(e);
    return e;
  }

  @Override
  public synchronized List<LogEvent> events(String cid) {
    if (!conversationId.equals(cid)) {
      return List.of();
    }
    List<LogEvent> all = new ArrayList<>(committed.size() + staged.size());
    all.addAll(committed);
    all.addAll(staged);
    return all;
  }

  @Override
  public Set<String> conversations() {
    return new TreeSet<>(Set.of(conversationId));
  }

  synchronized List<LogEvent> staged() {
    return List.copyOf(staged);
  }
}

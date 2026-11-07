package org.jagentic.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;

/**
 * A conversation's log as the results of its turns show it, for a runtime whose log is not readable
 * from outside, such as a Flink job's keyed state. A result's events are in sequence order, and a
 * redelivered one changes nothing.
 */
public final class ConversationView {

  private final Map<String, TreeMap<Long, LogEvent>> conversations = new ConcurrentHashMap<>();

  public void add(TurnResult result) {
    TreeMap<Long, LogEvent> log = conversations.computeIfAbsent(result.conversationId, k -> new TreeMap<>());
    synchronized (log) {
      for (LogEvent e : result.events) {
        log.putIfAbsent(e.sequence(), e);
      }
    }
  }

  public List<LogEvent> events(String key) {
    TreeMap<Long, LogEvent> log = conversations.get(key);
    if (log == null) {
      return List.of();
    }
    synchronized (log) {
      return new ArrayList<>(log.values());
    }
  }
}

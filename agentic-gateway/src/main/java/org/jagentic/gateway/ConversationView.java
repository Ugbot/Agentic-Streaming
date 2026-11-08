package org.jagentic.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.privacy.KeyVault;
import org.jagentic.core.privacy.SealedResults;

/**
 * A conversation's log as the results of its turns show it, for a runtime whose log is not readable
 * from outside, such as a Flink job's keyed state. A result's events are in sequence order, and a
 * redelivered one changes nothing. A sealed result is kept sealed and opened with the key vault on
 * each read, so once its conversation's key is destroyed it reads as nothing.
 */
public final class ConversationView {

  private final Map<String, TreeMap<Long, LogEvent>> conversations = new ConcurrentHashMap<>();
  /** Sealed results as they came, opened only when read, so an erased key leaves nothing here. */
  private final Map<String, List<Map<String, Object>>> sealed = new ConcurrentHashMap<>();
  private final KeyVault vault;

  public ConversationView() {
    this(null);
  }

  public ConversationView(KeyVault vault) {
    this.vault = vault;
  }

  /** A sealed result record ({@link SealedResults}): kept as ciphertext. */
  public void addSealed(String conversationId, Map<String, Object> record) {
    List<Map<String, Object>> held = sealed.computeIfAbsent(conversationId, k -> new ArrayList<>());
    synchronized (held) {
      held.add(record);
    }
  }

  public void add(TurnResult result) {
    TreeMap<Long, LogEvent> log = conversations.computeIfAbsent(result.conversationId, k -> new TreeMap<>());
    synchronized (log) {
      for (LogEvent e : result.events) {
        log.putIfAbsent(e.sequence(), e);
      }
    }
  }

  public List<LogEvent> events(String key) {
    TreeMap<Long, LogEvent> all = new TreeMap<>();
    TreeMap<Long, LogEvent> log = conversations.get(key);
    if (log != null) {
      synchronized (log) {
        all.putAll(log);
      }
    }
    List<Map<String, Object>> held = sealed.get(key);
    if (held != null) {
      List<Map<String, Object>> copy;
      synchronized (held) {
        copy = new ArrayList<>(held);
      }
      for (Map<String, Object> record : copy) {
        SealedResults.open(vault, record).ifPresent(plain -> {
          for (LogEvent e : TurnResults.decode(plain).events) {
            all.putIfAbsent(e.sequence(), e);
          }
        });
      }
    }
    return new ArrayList<>(all.values());
  }

  /** Decoding an opened result. */
  static final class TurnResults {
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON =
        new com.fasterxml.jackson.databind.ObjectMapper();

    private TurnResults() {}

    static TurnResult decode(byte[] plain) {
      try {
        return TurnResult.fromMap(JSON.readValue(plain,
            new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));
      } catch (java.io.IOException e) {
        throw new IllegalStateException("a sealed result does not decode", e);
      }
    }
  }
}

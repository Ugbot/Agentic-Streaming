package org.jagentic.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Process-local long-term store (default). */
public final class InMemoryLongTermStore implements LongTermStore {
  private final Map<String, List<String[]>> turns = new LinkedHashMap<>();
  private final Map<String, String> owner = new LinkedHashMap<>();
  private final Map<String, Map<String, String>> userFacts = new LinkedHashMap<>();

  @Override
  public synchronized void saveTurn(String conversationId, String userId, String role, String content) {
    turns.computeIfAbsent(conversationId, k -> new ArrayList<>()).add(new String[] {role, content});
    owner.put(conversationId, userId);
  }

  @Override
  public synchronized List<String[]> loadHistory(String conversationId) {
    return new ArrayList<>(turns.getOrDefault(conversationId, List.of()));
  }

  @Override
  public synchronized void saveFact(String userId, String key, String value) {
    userFacts.computeIfAbsent(userId, k -> new LinkedHashMap<>()).put(key, value);
  }

  @Override
  public synchronized Map<String, String> facts(String userId) {
    return new LinkedHashMap<>(userFacts.getOrDefault(userId, Map.of()));
  }

  @Override
  public synchronized List<String> conversationsForUser(String userId) {
    TreeSet<String> out = new TreeSet<>();
    owner.forEach((cid, u) -> {
      if (u.equals(userId)) out.add(cid);
    });
    return new ArrayList<>(out);
  }
}

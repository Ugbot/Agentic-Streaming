package org.jagentic.core;

import java.util.List;
import java.util.Map;

/** Long-term store SPI — conversation resumption + a per-user fact archive (portable
 * analogue of the Flink LongTermMemoryStore). {@link InMemoryLongTermStore} is the
 * default; the {@code store} package adds a real Postgres impl. */
public interface LongTermStore {
  void saveTurn(String conversationId, String userId, String role, String content);

  /** @return [role, content] pairs in order. */
  List<String[]> loadHistory(String conversationId);

  void saveFact(String userId, String key, String value);

  Map<String, String> facts(String userId);

  List<String> conversationsForUser(String userId);
}

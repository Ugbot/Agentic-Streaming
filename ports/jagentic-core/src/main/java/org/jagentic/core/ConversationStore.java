package org.jagentic.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-conversation memory — the portable analogue of the Flink-based
 * {@code org.agentic.flink.memory.conversation.ConversationStore}: durable, per-key
 * transcript + scalar attributes, keyed by conversationId, indexable by userId.
 * The single most important abstraction in the port. Swap {@link InMemory} for a
 * Redis/Fluss-backed implementation behind this interface; agent logic is unchanged.
 */
public interface ConversationStore {
  void append(String conversationId, ChatMessage message);

  List<ChatMessage> history(String conversationId);

  int messageCount(String conversationId);

  void putAttribute(String conversationId, String key, String value);

  Optional<String> getAttribute(String conversationId, String key);

  Map<String, String> attributes(String conversationId);

  void associateUser(String conversationId, String userId);

  List<String> conversationsForUser(String userId);

  void clear(String conversationId);

  /** Process-local, thread-safe default with a bounded transcript. */
  final class InMemory implements ConversationStore {
    private record Convo(Deque<ChatMessage> messages, Map<String, String> attrs, String[] owner) {}

    private final int maxMessages;
    private final Map<String, Convo> convos = new ConcurrentHashMap<>();
    private final Map<String, List<String>> userIndex = new ConcurrentHashMap<>();

    public InMemory() {
      this(200);
    }

    public InMemory(int maxMessages) {
      this.maxMessages = Math.max(1, maxMessages);
    }

    private Convo convo(String cid) {
      return convos.computeIfAbsent(cid, k -> new Convo(new ArrayDeque<>(), new LinkedHashMap<>(), new String[1]));
    }

    @Override
    public synchronized void append(String cid, ChatMessage m) {
      Deque<ChatMessage> q = convo(cid).messages();
      q.addLast(m);
      while (q.size() > maxMessages) {
        q.removeFirst();
      }
    }

    @Override
    public synchronized List<ChatMessage> history(String cid) {
      Convo c = convos.get(cid);
      return c == null ? new ArrayList<>() : new ArrayList<>(c.messages());
    }

    @Override
    public synchronized int messageCount(String cid) {
      Convo c = convos.get(cid);
      return c == null ? 0 : c.messages().size();
    }

    @Override
    public synchronized void putAttribute(String cid, String key, String value) {
      convo(cid).attrs().put(key, value);
    }

    @Override
    public synchronized Optional<String> getAttribute(String cid, String key) {
      Convo c = convos.get(cid);
      return Optional.ofNullable(c == null ? null : c.attrs().get(key));
    }

    @Override
    public synchronized Map<String, String> attributes(String cid) {
      Convo c = convos.get(cid);
      return c == null ? new LinkedHashMap<>() : new LinkedHashMap<>(c.attrs());
    }

    @Override
    public synchronized void associateUser(String cid, String userId) {
      String[] owner = convo(cid).owner();
      if (owner[0] != null && !owner[0].equals(userId)) {
        userIndex.getOrDefault(owner[0], new ArrayList<>()).remove(cid);
      }
      owner[0] = userId;
      List<String> ids = userIndex.computeIfAbsent(userId, k -> new ArrayList<>());
      if (!ids.contains(cid)) {
        ids.add(cid);
      }
    }

    @Override
    public synchronized List<String> conversationsForUser(String userId) {
      return new ArrayList<>(userIndex.getOrDefault(userId, new ArrayList<>()));
    }

    @Override
    public synchronized void clear(String cid) {
      Convo c = convos.remove(cid);
      if (c != null && c.owner()[0] != null) {
        userIndex.getOrDefault(c.owner()[0], new ArrayList<>()).remove(cid);
      }
    }
  }
}

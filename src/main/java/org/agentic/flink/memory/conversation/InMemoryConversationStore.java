package org.agentic.flink.memory.conversation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.agentic.flink.llm.ChatMessage;

/**
 * Process-local, in-JVM {@link ConversationStore} — the default. Backed by a {@link
 * ConcurrentHashMap} per conversation, with a bounded transcript (oldest messages evicted past the
 * cap) so a long-lived conversation cannot grow without limit.
 *
 * <p>This is the correct implementation for the <b>embedded single-JVM deployment</b> (all
 * operators of a routed graph share one process), and for unit/integration tests. For a distributed
 * cluster where operators run in different TaskManagers, supply a Redis/Postgres-backed {@link
 * ConversationStore} instead — the call sites are unchanged.
 *
 * <p>A {@link #shared() process-wide singleton} is provided for the embedded deployment, where the
 * store must be the <em>same</em> instance across operators even though each operator is handed its
 * own deserialized copy of the job graph: this class serializes to a handle that resolves back to
 * the singleton on the task side (see {@link #readResolve()}). Construct an isolated instance with
 * {@link #InMemoryConversationStore(int)} when you want independent state (e.g. per-test).
 */
public final class InMemoryConversationStore implements ConversationStore {
  private static final long serialVersionUID = 1L;

  /** Default cap on retained messages per conversation. */
  public static final int DEFAULT_MAX_MESSAGES = 200;

  private static final InMemoryConversationStore SHARED =
      new InMemoryConversationStore(DEFAULT_MAX_MESSAGES, true);

  private final int maxMessages;
  private final boolean singleton;

  private final transient Map<String, List<ChatMessage>> transcripts = new ConcurrentHashMap<>();
  private final transient Map<String, Map<String, String>> attributes = new ConcurrentHashMap<>();
  // conversationId -> userId, and the reverse index userId -> ordered conversationIds.
  private final transient Map<String, String> conversationUser = new ConcurrentHashMap<>();
  private final transient Map<String, java.util.Set<String>> userConversations =
      new ConcurrentHashMap<>();

  /** An isolated store with the {@link #DEFAULT_MAX_MESSAGES default} transcript cap. */
  public InMemoryConversationStore() {
    this(DEFAULT_MAX_MESSAGES, false);
  }

  /** An isolated store with a custom transcript cap ({@code <= 0} means unbounded). */
  public InMemoryConversationStore(int maxMessages) {
    this(maxMessages, false);
  }

  private InMemoryConversationStore(int maxMessages, boolean singleton) {
    this.maxMessages = maxMessages;
    this.singleton = singleton;
  }

  /**
   * The process-wide shared store — the same instance for every operator in this JVM. Use this for
   * the embedded single-JVM deployment so a routed graph's operators see one transcript.
   */
  public static InMemoryConversationStore shared() {
    return SHARED;
  }

  @Override
  public void append(String conversationId, ChatMessage message) {
    if (conversationId == null || message == null) {
      return;
    }
    transcripts.compute(
        conversationId,
        (k, list) -> {
          List<ChatMessage> l = list == null ? Collections.synchronizedList(new ArrayList<>()) : list;
          synchronized (l) {
            l.add(message);
            if (maxMessages > 0) {
              while (l.size() > maxMessages) {
                l.remove(0);
              }
            }
          }
          return l;
        });
  }

  @Override
  public List<ChatMessage> history(String conversationId) {
    List<ChatMessage> l = conversationId == null ? null : transcripts.get(conversationId);
    if (l == null) {
      return new ArrayList<>();
    }
    synchronized (l) {
      return new ArrayList<>(l);
    }
  }

  @Override
  public int messageCount(String conversationId) {
    List<ChatMessage> l = conversationId == null ? null : transcripts.get(conversationId);
    if (l == null) {
      return 0;
    }
    synchronized (l) {
      return l.size();
    }
  }

  @Override
  public void putAttribute(String conversationId, String key, String value) {
    if (conversationId == null || key == null) {
      return;
    }
    attributes
        .computeIfAbsent(conversationId, k -> new ConcurrentHashMap<>())
        .put(key, value == null ? "" : value);
  }

  @Override
  public Optional<String> getAttribute(String conversationId, String key) {
    if (conversationId == null || key == null) {
      return Optional.empty();
    }
    Map<String, String> a = attributes.get(conversationId);
    return a == null ? Optional.empty() : Optional.ofNullable(a.get(key));
  }

  @Override
  public Map<String, String> attributes(String conversationId) {
    Map<String, String> a = conversationId == null ? null : attributes.get(conversationId);
    return a == null ? new LinkedHashMap<>() : new LinkedHashMap<>(a);
  }

  @Override
  public void associateUser(String conversationId, String userId) {
    if (conversationId == null || userId == null) {
      return;
    }
    String prior = conversationUser.put(conversationId, userId);
    if (prior != null && !prior.equals(userId)) {
      java.util.Set<String> priorSet = userConversations.get(prior);
      if (priorSet != null) {
        priorSet.remove(conversationId);
      }
    }
    userConversations
        .computeIfAbsent(userId, k -> Collections.synchronizedSet(new java.util.LinkedHashSet<>()))
        .add(conversationId);
  }

  @Override
  public Optional<String> userOf(String conversationId) {
    return conversationId == null
        ? Optional.empty()
        : Optional.ofNullable(conversationUser.get(conversationId));
  }

  @Override
  public List<String> conversationsForUser(String userId) {
    java.util.Set<String> set = userId == null ? null : userConversations.get(userId);
    if (set == null) {
      return new ArrayList<>();
    }
    synchronized (set) {
      return new ArrayList<>(set);
    }
  }

  @Override
  public void clear(String conversationId) {
    if (conversationId == null) {
      return;
    }
    transcripts.remove(conversationId);
    attributes.remove(conversationId);
    String user = conversationUser.remove(conversationId);
    if (user != null) {
      java.util.Set<String> set = userConversations.get(user);
      if (set != null) {
        set.remove(conversationId);
        if (set.isEmpty()) {
          userConversations.remove(user);
        }
      }
    }
  }

  @Override
  public List<String> conversations() {
    java.util.Set<String> ids = new java.util.LinkedHashSet<>(transcripts.keySet());
    ids.addAll(attributes.keySet());
    ids.addAll(conversationUser.keySet());
    return new ArrayList<>(ids);
  }

  /**
   * On the task side, the singleton handle resolves back to the one shared instance so every
   * operator in this JVM shares the same transcript map (the transient maps are not serialized).
   * Isolated instances deserialize to a fresh empty store, as intended.
   */
  private Object readResolve() {
    return singleton ? SHARED : new InMemoryConversationStore(maxMessages, false);
  }
}

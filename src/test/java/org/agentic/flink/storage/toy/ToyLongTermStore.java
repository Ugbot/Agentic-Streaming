package org.agentic.flink.storage.toy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.ReopenableStore;
import org.agentic.flink.storage.StorageProvider;

/**
 * Minimal {@link LongTermMemoryStore} used to verify the registration procedure documented in
 * docs/guides/creating-storage-backends.md. It is registered through the test-scope service file
 * META-INF/services/org.agentic.flink.storage.LongTermMemoryStore and is therefore visible to
 * {@link org.agentic.flink.storage.StorageFactory} in the test classpath only.
 *
 * <p>The listing in the guide mirrors this class; keep the two in sync.
 */
public final class ToyLongTermStore extends ReopenableStore implements LongTermMemoryStore {

  private static final long serialVersionUID = 1L;

  public static final String PROVIDER_NAME = "toy";
  public static final String NAMESPACE_KEY = "toy.namespace";

  private String namespace;
  private transient Map<String, AgentContext> contexts;
  private transient Map<String, Map<String, ContextItem>> facts;
  private transient Map<String, Long> ttlSeconds;

  @Override
  protected void open(Map<String, String> config) {
    this.namespace = config.getOrDefault(NAMESPACE_KEY, "default");
    this.contexts = new HashMap<>();
    this.facts = new HashMap<>();
    this.ttlSeconds = new HashMap<>();
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public String getProviderName() {
    return PROVIDER_NAME;
  }

  @Override
  public void put(String key, AgentContext value) throws Exception {
    saveContext(key, value);
  }

  @Override
  public Optional<AgentContext> get(String key) throws Exception {
    return loadContext(key);
  }

  @Override
  public void delete(String key) throws Exception {
    deleteConversation(key);
  }

  @Override
  public boolean exists(String key) throws Exception {
    return conversationExists(key);
  }

  @Override
  public void close() {
    markClosed();
  }

  @Override
  public void saveContext(String flowId, AgentContext context) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(context, "context");
    contexts.put(flowId, context);
  }

  @Override
  public Optional<AgentContext> loadContext(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    return Optional.ofNullable(contexts.get(flowId));
  }

  @Override
  public boolean conversationExists(String flowId) {
    ensureOpen();
    return flowId != null && contexts.containsKey(flowId);
  }

  @Override
  public void deleteConversation(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    contexts.remove(flowId);
    facts.remove(flowId);
    ttlSeconds.remove(flowId);
  }

  @Override
  public void saveFacts(String flowId, Map<String, ContextItem> newFacts) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(newFacts, "facts");
    facts.put(flowId, new HashMap<>(newFacts));
  }

  @Override
  public Map<String, ContextItem> loadFacts(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    Map<String, ContextItem> stored = facts.get(flowId);
    return stored == null ? new HashMap<>() : new HashMap<>(stored);
  }

  @Override
  public void addFact(String flowId, String factId, ContextItem fact) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(factId, "factId");
    requireNonNull(fact, "fact");
    facts.computeIfAbsent(flowId, k -> new HashMap<>()).put(factId, fact);
  }

  @Override
  public void removeFact(String flowId, String factId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(factId, "factId");
    Map<String, ContextItem> stored = facts.get(flowId);
    if (stored != null) {
      stored.remove(factId);
    }
  }

  @Override
  public List<String> listActiveConversations() {
    ensureOpen();
    return new ArrayList<>(contexts.keySet());
  }

  @Override
  public List<String> listConversationsForUser(String userId) {
    ensureOpen();
    requireNonNull(userId, "userId");
    List<String> out = new ArrayList<>();
    for (Map.Entry<String, AgentContext> e : contexts.entrySet()) {
      if (userId.equals(e.getValue().getUserId())) {
        out.add(e.getKey());
      }
    }
    return out;
  }

  @Override
  public Map<String, Object> getConversationMetadata(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    Map<String, Object> meta = new HashMap<>();
    AgentContext context = contexts.get(flowId);
    if (context != null) {
      meta.put("flowId", flowId);
      meta.put("userId", context.getUserId());
      meta.put("agentId", context.getAgentId());
      meta.put("created_at", context.getCreatedAt());
      meta.put("last_updated_at", context.getLastUpdatedAt());
      meta.put("ttl_seconds", ttlSeconds.get(flowId));
    }
    return meta;
  }

  @Override
  public void setConversationTTL(String flowId, long seconds) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    ttlSeconds.put(flowId, seconds);
  }

  @Override
  public void archiveConversation(String flowId, StorageProvider<String, AgentContext> coldStore)
      throws Exception {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(coldStore, "coldStore");
    Optional<AgentContext> context = loadContext(flowId);
    if (context.isPresent()) {
      coldStore.put(flowId, context.get());
      deleteConversation(flowId);
    }
  }

  private static void requireNonNull(Object value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
  }
}

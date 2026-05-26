package org.agentic.flink.storage.memory;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.StorageProvider;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory implementation of LongTermMemoryStore.
 *
 * <p>This implementation stores complete conversation contexts and facts in memory. Suitable for
 * testing, development, and single-JVM deployments where conversation persistence across restarts
 * is not required.
 *
 * <p>Characteristics:
 *
 * <ul>
 *   <li>Latency: &lt;1ms
 *   <li>Capacity: Limited by JVM heap
 *   <li>Persistence: None (data lost on restart)
 *   <li>Distribution: Local to JVM
 * </ul>
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * Map<String, String> config = new HashMap<>();
 * config.put("cache.max.size", "5000");  // Max number of conversations
 * config.put("cache.ttl.seconds", "86400");  // 24 hours TTL
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class InMemoryLongTermStore implements LongTermMemoryStore {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryLongTermStore.class);

  private transient ConcurrentHashMap<String, AgentContext> contexts;
  private transient ConcurrentHashMap<String, Map<String, ContextItem>> facts;
  private transient ConcurrentHashMap<String, Map<String, Object>> metadata;
  private transient Set<String> activeConversations;
  private transient ConcurrentHashMap<String, Set<String>> userConversations;

  private long defaultTTLSeconds = 86400; // 24 hours
  private int maxSize = 5000;

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    this.contexts = new ConcurrentHashMap<>();
    this.facts = new ConcurrentHashMap<>();
    this.metadata = new ConcurrentHashMap<>();
    this.activeConversations = ConcurrentHashMap.newKeySet();
    this.userConversations = new ConcurrentHashMap<>();

    if (config != null) {
      this.defaultTTLSeconds =
          Long.parseLong(config.getOrDefault("cache.ttl.seconds", "86400"));
      this.maxSize = Integer.parseInt(config.getOrDefault("cache.max.size", "5000"));
    }

    LOG.info(
        "InMemoryLongTermStore initialized: maxSize={}, ttl={}s", maxSize, defaultTTLSeconds);
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
  public void saveContext(String flowId, AgentContext context) throws Exception {
    if (flowId == null || context == null) {
      throw new IllegalArgumentException("flowId and context cannot be null");
    }

    contexts.put(flowId, context);
    activeConversations.add(flowId);

    // Store metadata
    Map<String, Object> meta = new HashMap<>();
    meta.put("flowId", flowId);
    meta.put("userId", context.getUserId());
    meta.put("agentId", context.getAgentId());
    meta.put("created_at", System.currentTimeMillis());
    meta.put("last_updated_at", System.currentTimeMillis());
    metadata.put(flowId, meta);

    // Add to user's conversations
    String userId = context.getUserId();
    if (userId != null) {
      userConversations.computeIfAbsent(userId, k -> ConcurrentHashMap.newKeySet()).add(flowId);
    }

    LOG.debug("Saved context for flow {}", flowId);
  }

  @Override
  public Optional<AgentContext> loadContext(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    AgentContext context = contexts.get(flowId);
    return Optional.ofNullable(context);
  }

  @Override
  public boolean conversationExists(String flowId) throws Exception {
    return flowId != null && contexts.containsKey(flowId);
  }

  @Override
  public void deleteConversation(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    contexts.remove(flowId);
    facts.remove(flowId);
    metadata.remove(flowId);
    activeConversations.remove(flowId);

    // Remove from user conversations
    userConversations.values().forEach(set -> set.remove(flowId));

    LOG.debug("Deleted conversation {}", flowId);
  }

  @Override
  public void saveFacts(String flowId, Map<String, ContextItem> factsMap) throws Exception {
    if (flowId == null || factsMap == null) {
      throw new IllegalArgumentException("flowId and facts cannot be null");
    }

    facts.put(flowId, new HashMap<>(factsMap));
    LOG.debug("Saved {} facts for flow {}", factsMap.size(), flowId);
  }

  @Override
  public Map<String, ContextItem> loadFacts(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    Map<String, ContextItem> factsMap = facts.get(flowId);
    return factsMap != null ? new HashMap<>(factsMap) : new HashMap<>();
  }

  @Override
  public void addFact(String flowId, String factId, ContextItem fact) throws Exception {
    if (flowId == null || factId == null || fact == null) {
      throw new IllegalArgumentException("flowId, factId, and fact cannot be null");
    }

    facts.computeIfAbsent(flowId, k -> new ConcurrentHashMap<>()).put(factId, fact);
    LOG.debug("Added fact {} to flow {}", factId, flowId);
  }

  @Override
  public void removeFact(String flowId, String factId) throws Exception {
    if (flowId == null || factId == null) {
      throw new IllegalArgumentException("flowId and factId cannot be null");
    }

    Map<String, ContextItem> factsMap = facts.get(flowId);
    if (factsMap != null) {
      factsMap.remove(factId);
      LOG.debug("Removed fact {} from flow {}", factId, flowId);
    }
  }

  @Override
  public List<String> listActiveConversations() throws Exception {
    return new ArrayList<>(activeConversations);
  }

  @Override
  public List<String> listConversationsForUser(String userId) throws Exception {
    if (userId == null) {
      throw new IllegalArgumentException("userId cannot be null");
    }

    Set<String> conversations = userConversations.get(userId);
    return conversations != null ? new ArrayList<>(conversations) : new ArrayList<>();
  }

  @Override
  public Map<String, Object> getConversationMetadata(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    Map<String, Object> meta = metadata.get(flowId);
    return meta != null ? new HashMap<>(meta) : new HashMap<>();
  }

  @Override
  public void setConversationTTL(String flowId, long ttlSeconds) throws Exception {
    // In-memory implementation doesn't enforce TTL
    LOG.debug("Set TTL for conversation {} to {} seconds (no-op in memory)", flowId, ttlSeconds);
  }

  @Override
  public void archiveConversation(
      String flowId, StorageProvider<String, AgentContext> coldStore) throws Exception {
    if (flowId == null || coldStore == null) {
      throw new IllegalArgumentException("flowId and coldStore cannot be null");
    }

    Optional<AgentContext> context = loadContext(flowId);
    if (context.isPresent()) {
      coldStore.put(flowId, context.get());
      deleteConversation(flowId);
      LOG.info("Archived conversation {} to cold storage", flowId);
    }
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
  public void close() throws Exception {
    if (contexts != null) {
      contexts.clear();
    }
    if (facts != null) {
      facts.clear();
    }
    if (metadata != null) {
      metadata.clear();
    }
    if (activeConversations != null) {
      activeConversations.clear();
    }
    if (userConversations != null) {
      userConversations.clear();
    }
    LOG.info("InMemoryLongTermStore closed");
  }

  @Override
  public StorageTier getTier() {
    return StorageTier.WARM;
  }

  @Override
  public long getExpectedLatencyMs() {
    return 0; // Sub-millisecond
  }
}

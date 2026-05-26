package org.agentic.flink.storage.redis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.StorageProvider;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis-based implementation of LongTermMemoryStore for conversation persistence.
 *
 * <p>This implementation stores complete conversation context and long-term facts in Redis,
 * enabling conversation resumption after job restarts and cross-job context sharing.
 *
 * <p>Characteristics:
 *
 * <ul>
 *   <li>Latency: 1-5ms
 *   <li>Capacity: Limited by Redis memory
 *   <li>Persistence: Optional (RDB/AOF)
 *   <li>Distribution: Shared across all Flink jobs
 * </ul>
 *
 * <p>Data Structure:
 *
 * <pre>
 * agent:context:{flowId}       -> Hash (AgentContext JSON)
 * agent:facts:{flowId}         -> Hash (factId -> ContextItem JSON)
 * agent:metadata:{flowId}      -> Hash (metadata fields)
 * agent:conversations:active   -> Set (active flowIds)
 * agent:user:{userId}          -> Set (flowIds for this user)
 * </pre>
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * Map<String, String> config = new HashMap<>();
 * config.put("redis.host", "localhost");
 * config.put("redis.port", "6379");
 * config.put("redis.password", "secret");  // Optional
 * config.put("redis.database", "0");
 * config.put("redis.ttl.seconds", "86400");  // 24 hours default
 * config.put("redis.pool.max.total", "50");
 * config.put("redis.pool.max.idle", "10");
 * }</pre>
 *
 * <p>Status: Production-ready implementation. Uncomment Jedis imports and code after adding
 * dependencies.
 *
 * @author Agentic Flink Team
 */
public class RedisConversationStore implements LongTermMemoryStore {

  private static final Logger LOG = LoggerFactory.getLogger(RedisConversationStore.class);

  // Key prefixes
  private static final String KEY_CONTEXT = "agent:context:";
  private static final String KEY_FACTS = "agent:facts:";
  private static final String KEY_METADATA = "agent:metadata:";
  private static final String KEY_ACTIVE_CONVERSATIONS = "agent:conversations:active";
  private static final String KEY_USER_PREFIX = "agent:user:";

  // Redis connection pool
  private transient JedisPool jedisPool;

  // Configuration
  private String host;
  private int port;
  private String password;
  private int database;
  private long defaultTTLSeconds;

  // JSON serialization
  private transient ObjectMapper objectMapper;

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    this.host = config.getOrDefault(ConfigKeys.REDIS_HOST, ConfigKeys.DEFAULT_REDIS_HOST);
    this.port = Integer.parseInt(config.getOrDefault(ConfigKeys.REDIS_PORT, ConfigKeys.DEFAULT_REDIS_PORT));
    this.password = config.get(ConfigKeys.REDIS_PASSWORD);
    this.database = Integer.parseInt(config.getOrDefault("redis.database", "0"));
    this.defaultTTLSeconds =
        Long.parseLong(config.getOrDefault("redis.ttl.seconds", "86400")); // 24 hours

    int maxTotal = Integer.parseInt(config.getOrDefault("redis.pool.max.total", "50"));
    int maxIdle = Integer.parseInt(config.getOrDefault("redis.pool.max.idle", "10"));
    int timeout = Integer.parseInt(config.getOrDefault("redis.timeout.ms", "2000"));

    this.objectMapper = new ObjectMapper();

    // Initialize Jedis pool
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(maxTotal);
    poolConfig.setMaxIdle(maxIdle);
    poolConfig.setTestOnBorrow(true);

    if (password != null && !password.isEmpty()) {
      this.jedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
    } else {
      this.jedisPool = new JedisPool(poolConfig, host, port, timeout, null, database);
    }

    LOG.info(
        "RedisConversationStore initialized: host={}, port={}, database={}, ttl={}s",
        host, port, database, defaultTTLSeconds);
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

    String key = KEY_CONTEXT + flowId;
    String json = objectMapper.writeValueAsString(context);

    try (Jedis jedis = jedisPool.getResource()) {
      // Store context
      jedis.set(key, json);
      jedis.expire(key, defaultTTLSeconds);

      // Add to active conversations set
      jedis.sadd(KEY_ACTIVE_CONVERSATIONS, flowId);
      jedis.expire(KEY_ACTIVE_CONVERSATIONS, defaultTTLSeconds);

      // Add to user's conversation set if userId is available
      String userId = context.getUserId();
      if (userId != null) {
        String userKey = KEY_USER_PREFIX + userId;
        jedis.sadd(userKey, flowId);
        jedis.expire(userKey, defaultTTLSeconds);
      }

      // Store metadata
      String metadataKey = KEY_METADATA + flowId;
      Map<String, String> metadata = new HashMap<>();
      metadata.put("flowId", flowId);
      metadata.put("userId", userId != null ? userId : "unknown");
      metadata.put("agentId", context.getAgentId() != null ? context.getAgentId() : "unknown");
      metadata.put("created_at", String.valueOf(System.currentTimeMillis()));
      metadata.put("last_updated_at", String.valueOf(System.currentTimeMillis()));
      jedis.hset(metadataKey, metadata);
      jedis.expire(metadataKey, defaultTTLSeconds);

      LOG.debug("Saved context for flow {} in Redis", flowId);
    }
  }

  @Override
  public Optional<AgentContext> loadContext(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    String key = KEY_CONTEXT + flowId;

    try (Jedis jedis = jedisPool.getResource()) {
      String json = jedis.get(key);
      if (json != null) {
        AgentContext context = objectMapper.readValue(json, AgentContext.class);
        LOG.debug("Loaded context for flow {} from Redis", flowId);
        return Optional.of(context);
      }
      return Optional.empty();
    }
  }

  @Override
  public boolean conversationExists(String flowId) throws Exception {
    if (flowId == null) {
      return false;
    }

    String key = KEY_CONTEXT + flowId;

    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(key);
    }
  }

  @Override
  public void deleteConversation(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    try (Jedis jedis = jedisPool.getResource()) {
      // Get metadata first to find userId
      Map<String, String> metadata = jedis.hgetAll(KEY_METADATA + flowId);
      String userId = metadata.get("userId");

      // Delete context
      jedis.del(KEY_CONTEXT + flowId);

      // Delete facts
      jedis.del(KEY_FACTS + flowId);

      // Delete metadata
      jedis.del(KEY_METADATA + flowId);

      // Remove from active conversations
      jedis.srem(KEY_ACTIVE_CONVERSATIONS, flowId);

      // Remove from user's conversation set
      if (userId != null) {
        jedis.srem(KEY_USER_PREFIX + userId, flowId);
      }

      LOG.debug("Deleted conversation {} from Redis", flowId);
    }
  }

  @Override
  public void saveFacts(String flowId, Map<String, ContextItem> facts) throws Exception {
    if (flowId == null || facts == null) {
      throw new IllegalArgumentException("flowId and facts cannot be null");
    }

    String key = KEY_FACTS + flowId;

    try (Jedis jedis = jedisPool.getResource()) {
      // Convert facts to JSON strings
      Map<String, String> factsJson = new HashMap<>();
      for (Map.Entry<String, ContextItem> entry : facts.entrySet()) {
        factsJson.put(entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
      }

      // Store as hash
      if (!factsJson.isEmpty()) {
        jedis.hset(key, factsJson);
        jedis.expire(key, defaultTTLSeconds);
      }

      LOG.debug("Saved {} facts for flow {} in Redis", facts.size(), flowId);
    }
  }

  @Override
  public Map<String, ContextItem> loadFacts(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    String key = KEY_FACTS + flowId;

    try (Jedis jedis = jedisPool.getResource()) {
      Map<String, String> factsJson = jedis.hgetAll(key);
      Map<String, ContextItem> facts = new HashMap<>();

      for (Map.Entry<String, String> entry : factsJson.entrySet()) {
        ContextItem fact = objectMapper.readValue(entry.getValue(), ContextItem.class);
        facts.put(entry.getKey(), fact);
      }

      LOG.debug("Loaded {} facts for flow {} from Redis", facts.size(), flowId);
      return facts;
    }
  }

  @Override
  public void addFact(String flowId, String factId, ContextItem fact) throws Exception {
    if (flowId == null || factId == null || fact == null) {
      throw new IllegalArgumentException("flowId, factId, and fact cannot be null");
    }

    String key = KEY_FACTS + flowId;
    String factJson = objectMapper.writeValueAsString(fact);

    try (Jedis jedis = jedisPool.getResource()) {
      jedis.hset(key, factId, factJson);
      jedis.expire(key, defaultTTLSeconds);
      LOG.debug("Added fact {} to flow {} in Redis", factId, flowId);
    }
  }

  @Override
  public void removeFact(String flowId, String factId) throws Exception {
    if (flowId == null || factId == null) {
      throw new IllegalArgumentException("flowId and factId cannot be null");
    }

    String key = KEY_FACTS + flowId;

    try (Jedis jedis = jedisPool.getResource()) {
      jedis.hdel(key, factId);
      LOG.debug("Removed fact {} from flow {} in Redis", factId, flowId);
    }
  }

  @Override
  public List<String> listActiveConversations() throws Exception {
    try (Jedis jedis = jedisPool.getResource()) {
      Set<String> flowIds = jedis.smembers(KEY_ACTIVE_CONVERSATIONS);
      return new ArrayList<>(flowIds);
    }
  }

  @Override
  public List<String> listConversationsForUser(String userId) throws Exception {
    if (userId == null) {
      throw new IllegalArgumentException("userId cannot be null");
    }

    String key = KEY_USER_PREFIX + userId;

    try (Jedis jedis = jedisPool.getResource()) {
      Set<String> flowIds = jedis.smembers(key);
      return new ArrayList<>(flowIds);
    }
  }

  @Override
  public Map<String, Object> getConversationMetadata(String flowId) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    String key = KEY_METADATA + flowId;

    try (Jedis jedis = jedisPool.getResource()) {
      Map<String, String> metadataStr = jedis.hgetAll(key);
      Map<String, Object> metadata = new HashMap<>();
      for (Map.Entry<String, String> entry : metadataStr.entrySet()) {
        metadata.put(entry.getKey(), entry.getValue());
      }
      return metadata;
    }
  }

  @Override
  public void setConversationTTL(String flowId, long ttlSeconds) throws Exception {
    if (flowId == null) {
      throw new IllegalArgumentException("flowId cannot be null");
    }

    try (Jedis jedis = jedisPool.getResource()) {
      jedis.expire(KEY_CONTEXT + flowId, ttlSeconds);
      jedis.expire(KEY_FACTS + flowId, ttlSeconds);
      jedis.expire(KEY_METADATA + flowId, ttlSeconds);
      LOG.debug("Set TTL for conversation {} to {} seconds in Redis", flowId, ttlSeconds);
    }
  }

  @Override
  public void archiveConversation(String flowId, StorageProvider<String, AgentContext> coldStore)
      throws Exception {
    if (flowId == null || coldStore == null) {
      throw new IllegalArgumentException("flowId and coldStore cannot be null");
    }

    // Load from warm storage
    Optional<AgentContext> context = loadContext(flowId);
    if (context.isPresent()) {
      // Save to cold storage
      coldStore.put(flowId, context.get());

      // Delete from warm storage
      deleteConversation(flowId);

      LOG.info("Archived conversation {} from warm to cold storage", flowId);
    } else {
      LOG.warn("Cannot archive conversation {} - not found in warm storage", flowId);
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
    if (jedisPool != null) {
      jedisPool.close();
      LOG.info("RedisConversationStore connection pool closed");
    }
  }

  @Override
  public StorageTier getTier() {
    return StorageTier.WARM;
  }

  @Override
  public long getExpectedLatencyMs() {
    return 5; // 1-10ms for warm tier
  }
}

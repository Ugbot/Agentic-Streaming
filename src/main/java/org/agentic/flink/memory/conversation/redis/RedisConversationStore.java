package org.agentic.flink.memory.conversation.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis-backed {@link ConversationStore} — the cross-process "state spine" for the banking
 * deployment, where the A2A gateway and the embedded Flink job are separate processes and the
 * routed graph's operators must share one per-{@code contextId} transcript + workflow phase.
 *
 * <p>Layout (all keys TTL'd so finished simulations self-clean, keeping concurrent contextIds
 * isolated):
 *
 * <pre>
 *   conv:{id}:msgs    -&gt; List  (JSON ChatMessage per element, LTRIM-bounded)
 *   conv:{id}:attrs   -&gt; Hash  (workflow attributes; banking.phase lives here)
 *   conv:{id}:owner   -&gt; String userId   (the user this conversation belongs to)
 *   conv:user:{user}  -&gt; Set   (conversationIds owned by a user)
 *   conv:all          -&gt; Set   (known conversationIds — for conversations()/monitoring)
 * </pre>
 *
 * <p>Discovered via {@link java.util.ServiceLoader} (registered in {@code
 * META-INF/services/org.agentic.flink.memory.conversation.ConversationStore}). The no-arg
 * constructor self-gates on {@link ConfigKeys#CONVERSATION_STORE}{@code =redis}: when not selected
 * it throws so {@link org.agentic.flink.memory.conversation.ConversationStores#discover()} skips it
 * and falls back to the in-JVM store — so unit tests and dev default to in-JVM with no Redis needed.
 *
 * <p>{@link java.io.Serializable} (host/port config fields); the {@link JedisPool} + mapper are
 * transient and rebuilt lazily on the task side, matching the framework's other Redis-backed
 * components. Per the SPI contract these methods degrade gracefully (log + empty/no-op) rather than
 * fail a turn.
 */
public final class RedisConversationStore implements ConversationStore {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(RedisConversationStore.class);

  private static final String MSGS_SUFFIX = ":msgs";
  private static final String ATTRS_SUFFIX = ":attrs";
  private static final String OWNER_SUFFIX = ":owner";
  private static final String CONV_PREFIX = "conv:";
  private static final String USER_PREFIX = "conv:user:";
  private static final String ALL_KEY = "conv:all";

  private final String host;
  private final int port;
  private final String password;
  private final int maxMessages;
  private final long ttlSeconds;

  private transient volatile JedisPool pool;
  private transient volatile ObjectMapper mapper;

  /**
   * ServiceLoader constructor — configures from the environment and throws if the Redis
   * conversation store is not selected (so discovery falls back to the in-JVM store).
   */
  public RedisConversationStore() {
    this(AgenticFlinkConfig.fromEnvironment(), true);
  }

  /** Explicit constructor (tests / programmatic wiring). Always enabled. */
  public RedisConversationStore(String host, int port) {
    this.host = host;
    this.port = port;
    this.password = null;
    this.maxMessages = Integer.parseInt(ConfigKeys.DEFAULT_CONVERSATION_STORE_MAX_MESSAGES);
    this.ttlSeconds = Long.parseLong(ConfigKeys.DEFAULT_CONVERSATION_STORE_TTL_SECONDS);
  }

  private RedisConversationStore(AgenticFlinkConfig config, boolean requireSelected) {
    if (requireSelected) {
      String selected =
          config.get(ConfigKeys.CONVERSATION_STORE, ConfigKeys.DEFAULT_CONVERSATION_STORE);
      if (!"redis".equalsIgnoreCase(selected)) {
        throw new IllegalStateException(
            "RedisConversationStore not selected (" + ConfigKeys.CONVERSATION_STORE + "=" + selected + ")");
      }
    }
    this.host = config.get(ConfigKeys.REDIS_HOST, ConfigKeys.DEFAULT_REDIS_HOST);
    this.port =
        Integer.parseInt(config.get(ConfigKeys.REDIS_PORT, ConfigKeys.DEFAULT_REDIS_PORT));
    this.password = config.get(ConfigKeys.REDIS_PASSWORD);
    this.maxMessages =
        config.getInt(
            ConfigKeys.CONVERSATION_STORE_MAX_MESSAGES,
            Integer.parseInt(ConfigKeys.DEFAULT_CONVERSATION_STORE_MAX_MESSAGES));
    this.ttlSeconds =
        config.getInt(
            ConfigKeys.CONVERSATION_STORE_TTL_SECONDS,
            Integer.parseInt(ConfigKeys.DEFAULT_CONVERSATION_STORE_TTL_SECONDS));
    LOG.info("RedisConversationStore enabled: host={} port={} ttl={}s cap={}", host, port, ttlSeconds, maxMessages);
  }

  // ==================== transcript ====================

  @Override
  public void append(String conversationId, ChatMessage message) {
    if (conversationId == null || message == null) {
      return;
    }
    String key = CONV_PREFIX + conversationId + MSGS_SUFFIX;
    try (Jedis jedis = pool().getResource()) {
      jedis.rpush(key, encode(message));
      if (maxMessages > 0) {
        jedis.ltrim(key, -maxMessages, -1);
      }
      jedis.expire(key, ttlSeconds);
      jedis.sadd(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("append failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public List<ChatMessage> history(String conversationId) {
    if (conversationId == null) {
      return new ArrayList<>();
    }
    String key = CONV_PREFIX + conversationId + MSGS_SUFFIX;
    try (Jedis jedis = pool().getResource()) {
      List<String> raw = jedis.lrange(key, 0, -1);
      List<ChatMessage> out = new ArrayList<>(raw.size());
      for (String s : raw) {
        ChatMessage m = decode(s);
        if (m != null) {
          out.add(m);
        }
      }
      return out;
    } catch (Exception e) {
      LOG.warn("history failed for {}: {}", conversationId, e.toString());
      return new ArrayList<>();
    }
  }

  @Override
  public int messageCount(String conversationId) {
    if (conversationId == null) {
      return 0;
    }
    try (Jedis jedis = pool().getResource()) {
      return (int) jedis.llen(CONV_PREFIX + conversationId + MSGS_SUFFIX);
    } catch (Exception e) {
      LOG.warn("messageCount failed for {}: {}", conversationId, e.toString());
      return 0;
    }
  }

  // ==================== attributes ====================

  @Override
  public void putAttribute(String conversationId, String key, String value) {
    if (conversationId == null || key == null) {
      return;
    }
    String hkey = CONV_PREFIX + conversationId + ATTRS_SUFFIX;
    try (Jedis jedis = pool().getResource()) {
      jedis.hset(hkey, key, value == null ? "" : value);
      jedis.expire(hkey, ttlSeconds);
      jedis.sadd(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("putAttribute failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public Optional<String> getAttribute(String conversationId, String key) {
    if (conversationId == null || key == null) {
      return Optional.empty();
    }
    try (Jedis jedis = pool().getResource()) {
      return Optional.ofNullable(jedis.hget(CONV_PREFIX + conversationId + ATTRS_SUFFIX, key));
    } catch (Exception e) {
      LOG.warn("getAttribute failed for {}: {}", conversationId, e.toString());
      return Optional.empty();
    }
  }

  @Override
  public Map<String, String> attributes(String conversationId) {
    if (conversationId == null) {
      return new LinkedHashMap<>();
    }
    try (Jedis jedis = pool().getResource()) {
      Map<String, String> a = jedis.hgetAll(CONV_PREFIX + conversationId + ATTRS_SUFFIX);
      return a == null ? new LinkedHashMap<>() : new LinkedHashMap<>(a);
    } catch (Exception e) {
      LOG.warn("attributes failed for {}: {}", conversationId, e.toString());
      return new LinkedHashMap<>();
    }
  }

  // ==================== user index ====================

  @Override
  public void associateUser(String conversationId, String userId) {
    if (conversationId == null || userId == null) {
      return;
    }
    String ownerKey = CONV_PREFIX + conversationId + OWNER_SUFFIX;
    try (Jedis jedis = pool().getResource()) {
      String prior = jedis.get(ownerKey);
      if (prior != null && !prior.equals(userId)) {
        jedis.srem(USER_PREFIX + prior, conversationId);
      }
      jedis.set(ownerKey, userId);
      jedis.expire(ownerKey, ttlSeconds);
      jedis.sadd(USER_PREFIX + userId, conversationId);
      jedis.expire(USER_PREFIX + userId, ttlSeconds);
      jedis.sadd(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("associateUser failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public Optional<String> userOf(String conversationId) {
    if (conversationId == null) {
      return Optional.empty();
    }
    try (Jedis jedis = pool().getResource()) {
      return Optional.ofNullable(jedis.get(CONV_PREFIX + conversationId + OWNER_SUFFIX));
    } catch (Exception e) {
      LOG.warn("userOf failed for {}: {}", conversationId, e.toString());
      return Optional.empty();
    }
  }

  @Override
  public List<String> conversationsForUser(String userId) {
    if (userId == null) {
      return new ArrayList<>();
    }
    try (Jedis jedis = pool().getResource()) {
      return new ArrayList<>(jedis.smembers(USER_PREFIX + userId));
    } catch (Exception e) {
      LOG.warn("conversationsForUser failed for {}: {}", userId, e.toString());
      return new ArrayList<>();
    }
  }

  @Override
  public void clear(String conversationId) {
    if (conversationId == null) {
      return;
    }
    try (Jedis jedis = pool().getResource()) {
      String owner = jedis.get(CONV_PREFIX + conversationId + OWNER_SUFFIX);
      if (owner != null) {
        jedis.srem(USER_PREFIX + owner, conversationId);
      }
      jedis.del(
          CONV_PREFIX + conversationId + MSGS_SUFFIX,
          CONV_PREFIX + conversationId + ATTRS_SUFFIX,
          CONV_PREFIX + conversationId + OWNER_SUFFIX);
      jedis.srem(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("clear failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public List<String> conversations() {
    try (Jedis jedis = pool().getResource()) {
      return new ArrayList<>(jedis.smembers(ALL_KEY));
    } catch (Exception e) {
      LOG.warn("conversations failed: {}", e.toString());
      return new ArrayList<>();
    }
  }

  // ==================== plumbing ====================

  private JedisPool pool() {
    JedisPool p = pool;
    if (p == null) {
      synchronized (this) {
        if (pool == null) {
          JedisPoolConfig cfg = new JedisPoolConfig();
          cfg.setMaxTotal(50);
          cfg.setMaxIdle(10);
          cfg.setTestOnBorrow(true);
          pool =
              (password != null && !password.isEmpty())
                  ? new JedisPool(cfg, host, port, 2000, password)
                  : new JedisPool(cfg, host, port, 2000);
        }
        p = pool;
      }
    }
    return p;
  }

  private ObjectMapper mapper() {
    ObjectMapper m = mapper;
    if (m == null) {
      synchronized (this) {
        if (mapper == null) {
          mapper = new ObjectMapper();
        }
        m = mapper;
      }
    }
    return m;
  }

  private String encode(ChatMessage m) throws Exception {
    Map<String, String> o = new LinkedHashMap<>();
    o.put("role", m.getRole().name());
    o.put("content", m.getContent());
    if (m.getToolCallId() != null) {
      o.put("toolCallId", m.getToolCallId());
    }
    if (m.getToolName() != null) {
      o.put("toolName", m.getToolName());
    }
    return mapper().writeValueAsString(o);
  }

  @SuppressWarnings("unchecked")
  private ChatMessage decode(String json) {
    try {
      Map<String, String> o = mapper().readValue(json, Map.class);
      String content = o.getOrDefault("content", "");
      switch (ChatRole.valueOf(o.get("role"))) {
        case SYSTEM:
          return ChatMessage.system(content);
        case USER:
          return ChatMessage.user(content);
        case ASSISTANT:
          return ChatMessage.assistant(content);
        case TOOL:
          return ChatMessage.tool(o.get("toolCallId"), o.get("toolName"), content);
        default:
          return ChatMessage.user(content);
      }
    } catch (Exception e) {
      LOG.warn("decode failed: {}", e.toString());
      return null;
    }
  }
}

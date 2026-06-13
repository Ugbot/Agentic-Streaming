package org.agentic.flink.a2a.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.agentic.flink.a2a.A2AJson;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.config.ConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis-backed {@link A2ATaskStore} — lighter-weight durable gateway task state. Optional: requires
 * the {@code jedis} dependency on the classpath (marked optional in the build).
 *
 * <p>Key layout: {@code a2a:task:{id}} (task JSON), {@code a2a:ctx:{contextId}} +
 * {@code a2a:state:{state}} (sets of task ids for context/state queries), {@code a2a:push:{taskId}}
 * (hash of configId → config JSON). Mirrors {@link
 * org.agentic.flink.storage.redis.RedisConversationStore}.
 */
public final class RedisA2ATaskStore implements A2ATaskStore {
  private static final Logger LOG = LoggerFactory.getLogger(RedisA2ATaskStore.class);

  private static final String TASK = "a2a:task:";
  private static final String CTX = "a2a:ctx:";
  private static final String STATE = "a2a:state:";
  private static final String PUSH = "a2a:push:";

  private transient JedisPool pool;
  private transient ObjectMapper mapper;

  @Override
  public void initialize(Map<String, String> config) {
    String host = config.getOrDefault(ConfigKeys.REDIS_HOST, ConfigKeys.DEFAULT_REDIS_HOST);
    int port = Integer.parseInt(config.getOrDefault(ConfigKeys.REDIS_PORT, ConfigKeys.DEFAULT_REDIS_PORT));
    String password = config.get(ConfigKeys.REDIS_PASSWORD);
    int timeout = Integer.parseInt(config.getOrDefault("redis.timeout.ms", "2000"));
    int database = Integer.parseInt(config.getOrDefault("redis.database", "0"));
    this.mapper = A2AJson.create();
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    this.pool =
        (password == null || password.isEmpty())
            ? new JedisPool(poolConfig, host, port, timeout, null, database)
            : new JedisPool(poolConfig, host, port, timeout, password, database);
    LOG.info("RedisA2ATaskStore initialized: {}:{}/{}", host, port, database);
  }

  @Override
  public void saveTask(A2ATask task) throws Exception {
    String json = mapper.writeValueAsString(task);
    try (Jedis jedis = pool.getResource()) {
      // Drop the task from any previous state set before recording the current one
      // (same connection — avoid nested pool checkout).
      String priorJson = jedis.get(TASK + task.getId());
      if (priorJson != null) {
        A2ATask prior = mapper.readValue(priorJson, A2ATask.class);
        jedis.srem(STATE + prior.getState().wire(), task.getId());
      }
      jedis.set(TASK + task.getId(), json);
      if (task.getContextId() != null) {
        jedis.sadd(CTX + task.getContextId(), task.getId());
      }
      jedis.sadd(STATE + task.getState().wire(), task.getId());
    }
  }

  @Override
  public Optional<A2ATask> loadTask(String taskId) throws Exception {
    try (Jedis jedis = pool.getResource()) {
      String json = jedis.get(TASK + taskId);
      return json == null ? Optional.empty() : Optional.of(mapper.readValue(json, A2ATask.class));
    }
  }

  @Override
  public List<A2ATask> listTasksByContext(String contextId) throws Exception {
    return bySet(CTX + contextId);
  }

  @Override
  public List<A2ATask> listTasksByState(A2ATaskState state) throws Exception {
    return bySet(STATE + state.wire());
  }

  private List<A2ATask> bySet(String setKey) throws Exception {
    List<A2ATask> out = new ArrayList<>();
    try (Jedis jedis = pool.getResource()) {
      Set<String> ids = jedis.smembers(setKey);
      for (String id : ids) {
        String json = jedis.get(TASK + id);
        if (json != null) {
          out.add(mapper.readValue(json, A2ATask.class));
        }
      }
    }
    return out;
  }

  @Override
  public void deleteTask(String taskId) throws Exception {
    try (Jedis jedis = pool.getResource()) {
      String priorJson = jedis.get(TASK + taskId);
      jedis.del(TASK + taskId);
      jedis.del(PUSH + taskId);
      if (priorJson != null) {
        A2ATask prior = mapper.readValue(priorJson, A2ATask.class);
        jedis.srem(STATE + prior.getState().wire(), taskId);
        if (prior.getContextId() != null) {
          jedis.srem(CTX + prior.getContextId(), taskId);
        }
      }
    }
  }

  @Override
  public void savePushConfig(String taskId, A2APushConfig config) throws Exception {
    String json = mapper.writeValueAsString(config);
    try (Jedis jedis = pool.getResource()) {
      jedis.hset(PUSH + taskId, config.getId(), json);
    }
  }

  @Override
  public List<A2APushConfig> listPushConfigs(String taskId) throws Exception {
    List<A2APushConfig> out = new ArrayList<>();
    try (Jedis jedis = pool.getResource()) {
      for (String json : jedis.hgetAll(PUSH + taskId).values()) {
        out.add(mapper.readValue(json, A2APushConfig.class));
      }
    }
    return out;
  }

  @Override
  public Optional<A2APushConfig> getPushConfig(String taskId, String configId) throws Exception {
    try (Jedis jedis = pool.getResource()) {
      String json = jedis.hget(PUSH + taskId, configId);
      return json == null
          ? Optional.empty()
          : Optional.of(mapper.readValue(json, A2APushConfig.class));
    }
  }

  @Override
  public void deletePushConfig(String taskId, String configId) {
    try (Jedis jedis = pool.getResource()) {
      jedis.hdel(PUSH + taskId, configId);
    }
  }

  @Override
  public String getProviderName() {
    return "redis";
  }

  @Override
  public void close() {
    if (pool != null) {
      pool.close();
    }
  }
}

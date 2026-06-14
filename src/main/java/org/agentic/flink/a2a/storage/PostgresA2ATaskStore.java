package org.agentic.flink.a2a.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.a2a.A2AJson;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.config.ConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL-backed {@link A2ATaskStore} — durable A2A task lifecycle + push-config persistence for
 * the gateway. Mirrors {@link org.agentic.flink.storage.postgres.PostgresConversationStore}:
 * HikariCP pool, {@code MERGE INTO} upserts (H2/PostgreSQL compatible), JSON {@code TEXT} columns.
 *
 * <pre>
 * CREATE TABLE a2a_tasks (
 *   task_id VARCHAR(255) PRIMARY KEY, context_id VARCHAR(255), state VARCHAR(64) NOT NULL,
 *   task_json TEXT NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL);
 * CREATE TABLE a2a_push_configs (
 *   task_id VARCHAR(255) NOT NULL, config_id VARCHAR(255) NOT NULL, config_json TEXT NOT NULL,
 *   PRIMARY KEY (task_id, config_id));
 * </pre>
 */
public final class PostgresA2ATaskStore implements A2ATaskStore {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresA2ATaskStore.class);

  private String jdbcUrl;
  private String username;
  private String password;
  private boolean autoCreateTables;
  private transient HikariDataSource dataSource;
  private transient ObjectMapper mapper;

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    this.jdbcUrl = config.getOrDefault(ConfigKeys.POSTGRES_URL, ConfigKeys.DEFAULT_POSTGRES_URL);
    this.username = config.getOrDefault(ConfigKeys.POSTGRES_USER, ConfigKeys.DEFAULT_POSTGRES_USER);
    this.password = config.get("postgres.password");
    this.autoCreateTables =
        Boolean.parseBoolean(config.getOrDefault("postgres.auto.create.tables", "true"));
    this.mapper = A2AJson.create();

    HikariConfig hikari = new HikariConfig();
    hikari.setJdbcUrl(jdbcUrl);
    hikari.setUsername(username);
    if (password != null) {
      hikari.setPassword(password);
    }
    hikari.setMaximumPoolSize(Integer.parseInt(config.getOrDefault("postgres.pool.max.size", "10")));
    hikari.setMinimumIdle(Integer.parseInt(config.getOrDefault("postgres.pool.min.idle", "2")));
    hikari.setPoolName("a2a-task-store");
    this.dataSource = new HikariDataSource(hikari);

    if (autoCreateTables) {
      createSchema();
    }
    LOG.info("PostgresA2ATaskStore initialized: {}", jdbcUrl);
  }

  private void createSchema() throws Exception {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement()) {
      st.execute(
          "CREATE TABLE IF NOT EXISTS a2a_tasks ("
              + "task_id VARCHAR(255) PRIMARY KEY, context_id VARCHAR(255), "
              + "state VARCHAR(64) NOT NULL, task_json TEXT NOT NULL, "
              + "created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL)");
      st.execute(
          "CREATE TABLE IF NOT EXISTS a2a_push_configs ("
              + "task_id VARCHAR(255) NOT NULL, config_id VARCHAR(255) NOT NULL, "
              + "config_json TEXT NOT NULL, PRIMARY KEY (task_id, config_id))");
      st.execute("CREATE INDEX IF NOT EXISTS idx_a2a_tasks_context ON a2a_tasks(context_id)");
      st.execute("CREATE INDEX IF NOT EXISTS idx_a2a_tasks_state ON a2a_tasks(state)");
    }
  }

  @Override
  public void saveTask(A2ATask task) throws Exception {
    String json = mapper.writeValueAsString(task);
    String sql =
        "MERGE INTO a2a_tasks (task_id, context_id, state, task_json, created_at, updated_at) "
            + "KEY (task_id) VALUES (?, ?, ?, ?, ?, ?)";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, task.getId());
      ps.setString(2, task.getContextId());
      ps.setString(3, task.getState().wire());
      ps.setString(4, json);
      ps.setLong(5, task.getCreatedAtEpochMs());
      ps.setLong(6, task.getUpdatedAtEpochMs());
      ps.executeUpdate();
    }
  }

  @Override
  public Optional<A2ATask> loadTask(String taskId) throws Exception {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement("SELECT task_json FROM a2a_tasks WHERE task_id = ?")) {
      ps.setString(1, taskId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(mapper.readValue(rs.getString(1), A2ATask.class));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public List<A2ATask> listTasksByContext(String contextId) throws Exception {
    return query(
        "SELECT task_json FROM a2a_tasks WHERE context_id = ? ORDER BY created_at", contextId);
  }

  @Override
  public List<A2ATask> listTasksByState(A2ATaskState state) throws Exception {
    return query("SELECT task_json FROM a2a_tasks WHERE state = ? ORDER BY updated_at", state.wire());
  }

  private List<A2ATask> query(String sql, String param) throws Exception {
    List<A2ATask> out = new ArrayList<>();
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, param);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(mapper.readValue(rs.getString(1), A2ATask.class));
        }
      }
    }
    return out;
  }

  @Override
  public void deleteTask(String taskId) throws Exception {
    try (Connection c = dataSource.getConnection()) {
      try (PreparedStatement ps = c.prepareStatement("DELETE FROM a2a_tasks WHERE task_id = ?")) {
        ps.setString(1, taskId);
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          c.prepareStatement("DELETE FROM a2a_push_configs WHERE task_id = ?")) {
        ps.setString(1, taskId);
        ps.executeUpdate();
      }
    }
  }

  @Override
  public void savePushConfig(String taskId, A2APushConfig config) throws Exception {
    String json = mapper.writeValueAsString(config);
    String sql =
        "MERGE INTO a2a_push_configs (task_id, config_id, config_json) "
            + "KEY (task_id, config_id) VALUES (?, ?, ?)";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, taskId);
      ps.setString(2, config.getId());
      ps.setString(3, json);
      ps.executeUpdate();
    }
  }

  @Override
  public List<A2APushConfig> listPushConfigs(String taskId) throws Exception {
    List<A2APushConfig> out = new ArrayList<>();
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement("SELECT config_json FROM a2a_push_configs WHERE task_id = ?")) {
      ps.setString(1, taskId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(mapper.readValue(rs.getString(1), A2APushConfig.class));
        }
      }
    }
    return out;
  }

  @Override
  public Optional<A2APushConfig> getPushConfig(String taskId, String configId) throws Exception {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement(
                "SELECT config_json FROM a2a_push_configs WHERE task_id = ? AND config_id = ?")) {
      ps.setString(1, taskId);
      ps.setString(2, configId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(mapper.readValue(rs.getString(1), A2APushConfig.class));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public void deletePushConfig(String taskId, String configId) throws Exception {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement(
                "DELETE FROM a2a_push_configs WHERE task_id = ? AND config_id = ?")) {
      ps.setString(1, taskId);
      ps.setString(2, configId);
      ps.executeUpdate();
    }
  }

  @Override
  public String getProviderName() {
    return "postgres";
  }

  @Override
  public void close() {
    if (dataSource != null) {
      dataSource.close();
    }
  }
}

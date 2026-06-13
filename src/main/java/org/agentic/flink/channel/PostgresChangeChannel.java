package org.agentic.flink.channel;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.agentic.flink.context.core.ContextItem;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls Postgres for newly inserted rows in {@code agent_facts}, surfacing each as a {@link
 * KeyedContextItem}.
 *
 * <p>Polling is intentionally chosen over {@code LISTEN/NOTIFY} for the first cut: it works
 * regardless of the connection's transaction state, survives connection loss without dropped
 * notifications, and doesn't need a long-lived dedicated thread. The trade-off is end-to-end
 * latency bounded by the poll interval (default 5s).
 *
 * <p>This source is intentionally not parallel: every subtask would otherwise scan the same
 * watermark range and produce duplicates.
 *
 * <p>Migrated from {@code PostgresChangeFeed}; behaviour unchanged.
 */
public final class PostgresChangeChannel implements Channel<KeyedContextItem> {
  private static final long serialVersionUID = 1L;

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final Duration pollInterval;

  public PostgresChangeChannel(String jdbcUrl, String username, String password) {
    this(jdbcUrl, username, password, Duration.ofSeconds(5));
  }

  public PostgresChangeChannel(
      String jdbcUrl, String username, String password, Duration pollInterval) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.pollInterval = pollInterval;
  }

  @Override
  public DataStream<KeyedContextItem> open(StreamExecutionEnvironment env) {
    return env.addSource(new Source(jdbcUrl, username, password, pollInterval.toMillis()))
        .name("postgres-change-channel")
        .setParallelism(1);
  }

  @Override
  public TypeInformation<KeyedContextItem> elementType() {
    return TypeInformation.of(new TypeHint<KeyedContextItem>() {});
  }

  @Override
  public String providerName() {
    return "postgres-change";
  }

  static final class Source implements SourceFunction<KeyedContextItem> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final long pollIntervalMs;

    private volatile boolean running = true;

    Source(String jdbcUrl, String username, String password, long pollIntervalMs) {
      this.jdbcUrl = jdbcUrl;
      this.username = username;
      this.password = password;
      this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void run(SourceContext<KeyedContextItem> ctx) throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.registerModule(new ParameterNamesModule());
      mapper.setVisibility(
          mapper
              .getSerializationConfig()
              .getDefaultVisibilityChecker()
              .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
              .withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
              .withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
              .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));

      Timestamp watermark = new Timestamp(0L);
      while (running) {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
          try (PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT flow_id, fact_id, fact_json, created_at "
                      + "FROM agent_facts WHERE created_at > ? ORDER BY created_at ASC")) {
            ps.setTimestamp(1, watermark);
            try (ResultSet rs = ps.executeQuery()) {
              while (rs.next()) {
                String flowId = rs.getString("flow_id");
                String factJson = rs.getString("fact_json");
                Timestamp createdAt = rs.getTimestamp("created_at");
                ContextItem item = mapper.readValue(factJson, ContextItem.class);
                synchronized (ctx.getCheckpointLock()) {
                  ctx.collect(new KeyedContextItem(flowId, item));
                }
                if (createdAt.after(watermark)) {
                  watermark = createdAt;
                }
              }
            }
          }
        } catch (Exception e) {
          LOG.warn("PostgresChangeChannel poll failed; will retry: {}", e.getMessage());
        }
        Thread.sleep(pollIntervalMs);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }
}

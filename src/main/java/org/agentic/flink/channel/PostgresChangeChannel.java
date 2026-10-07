package org.agentic.flink.channel;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayDeque;
import org.agentic.flink.annotation.Public;
import java.util.Objects;
import org.agentic.flink.channel.source.PollingSource;
import org.agentic.flink.context.core.ContextItem;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * <p>The read position ({@code created_at, flow_id, fact_id} of the last emitted row) is stored in
 * the source split and checkpointed, so a job restored from a checkpoint or savepoint resumes after
 * the last row it emitted instead of re-reading the table from epoch zero.
 *
 * <p>Migrated from {@code PostgresChangeFeed}.
 */
@Public
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
    return env.fromSource(
            new PollingSource<>(
                new PostgresPollFn(jdbcUrl, username, password, pollInterval.toMillis())),
            WatermarkStrategy.noWatermarks(),
            "postgres-change-channel",
            elementType())
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

  /**
   * Native FLIP-27 {@link PollingSource.PositionedPollFn}: each query (throttled to {@code
   * pollIntervalMs}) fetches rows strictly after the cursor, ordered by {@code (created_at,
   * flow_id, fact_id)}, into a buffer; {@link #poll} returns them one at a time and advances the
   * cursor to the row it returned. The cursor is the split position {@link PollingSource}
   * checkpoints, so a restored job resumes after the last emitted row rather than re-scanning from
   * epoch zero. Rows sharing a {@code created_at} are totally ordered by the primary key, so a
   * checkpoint taken between two rows with the same timestamp neither skips nor repeats either of
   * them.
   */
  static final class PostgresPollFn implements PollingSource.PositionedPollFn<KeyedContextItem> {
    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPollFn.class);

    static final String QUERY =
        "SELECT flow_id, fact_id, fact_json, created_at FROM agent_facts "
            + "WHERE (created_at, flow_id, fact_id) > (?, ?, ?) "
            + "ORDER BY created_at ASC, flow_id ASC, fact_id ASC";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final long pollIntervalMs;

    private transient ObjectMapper mapper;
    private transient ArrayDeque<Row> buffer;
    private transient Cursor cursor;
    private transient long lastQueryMs;

    PostgresPollFn(String jdbcUrl, String username, String password, long pollIntervalMs) {
      this.jdbcUrl = jdbcUrl;
      this.username = username;
      this.password = password;
      this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void open(int subtaskIndex) {
      mapper = new ObjectMapper();
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
      buffer = new ArrayDeque<>();
      cursor = Cursor.START;
      lastQueryMs = 0L;
    }

    @Override
    public void seek(String position) {
      buffer.clear();
      cursor = Cursor.decode(position);
      lastQueryMs = 0L;
    }

    @Override
    public String position() {
      return cursor == Cursor.START ? null : cursor.encode();
    }

    @Override
    public KeyedContextItem poll(long timeoutMs) {
      if (buffer.isEmpty()) {
        long now = System.currentTimeMillis();
        if (now - lastQueryMs < pollIntervalMs) {
          return null; // throttle DB queries to the poll interval
        }
        lastQueryMs = now;
        fetch();
      }
      Row row = buffer.poll();
      if (row == null) {
        return null;
      }
      cursor = row.cursor;
      return row.item;
    }

    private void fetch() {
      try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
          PreparedStatement ps = conn.prepareStatement(QUERY)) {
        ps.setTimestamp(1, cursor.createdAt);
        ps.setString(2, cursor.flowId);
        ps.setString(3, cursor.factId);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String flowId = rs.getString("flow_id");
            String factId = rs.getString("fact_id");
            String factJson = rs.getString("fact_json");
            Timestamp createdAt = rs.getTimestamp("created_at");
            ContextItem item = mapper.readValue(factJson, ContextItem.class);
            buffer.add(
                new Row(new KeyedContextItem(flowId, item), new Cursor(createdAt, flowId, factId)));
          }
        }
      } catch (Exception e) {
        LOG.warn("PostgresChangeChannel poll failed; will retry: {}", e.getMessage());
      }
    }

    private static final class Row {
      final KeyedContextItem item;
      final Cursor cursor;

      Row(KeyedContextItem item, Cursor cursor) {
        this.item = item;
        this.cursor = cursor;
      }
    }
  }

  /**
   * Durable read position over {@code agent_facts}: the {@code (created_at, flow_id, fact_id)} of
   * the last emitted row. Encoded as {@code epochMillis:nanos:urlenc(flowId):urlenc(factId)} so it
   * is timezone independent and survives any characters in the ids.
   */
  static final class Cursor {
    /** Before the first row: the tuple comparison is strict, so empty ids sort before any row. */
    static final Cursor START = new Cursor(new Timestamp(0L), "", "");

    final Timestamp createdAt;
    final String flowId;
    final String factId;

    Cursor(Timestamp createdAt, String flowId, String factId) {
      this.createdAt = createdAt;
      this.flowId = flowId;
      this.factId = factId;
    }

    String encode() {
      return createdAt.getTime()
          + ":"
          + createdAt.getNanos()
          + ":"
          + URLEncoder.encode(flowId, StandardCharsets.UTF_8)
          + ":"
          + URLEncoder.encode(factId, StandardCharsets.UTF_8);
    }

    static Cursor decode(String position) {
      if (position == null) {
        return START;
      }
      String[] parts = position.split(":", -1);
      if (parts.length != 4) {
        throw new IllegalArgumentException("Malformed PostgresChangeChannel cursor: " + position);
      }
      Timestamp ts = new Timestamp(Long.parseLong(parts[0]));
      ts.setNanos(Integer.parseInt(parts[1]));
      return new Cursor(
          ts,
          URLDecoder.decode(parts[2], StandardCharsets.UTF_8),
          URLDecoder.decode(parts[3], StandardCharsets.UTF_8));
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Cursor)) {
        return false;
      }
      Cursor c = (Cursor) o;
      return createdAt.equals(c.createdAt) && flowId.equals(c.flowId) && factId.equals(c.factId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(createdAt, flowId, factId);
    }

    @Override
    public String toString() {
      return encode();
    }
  }
}

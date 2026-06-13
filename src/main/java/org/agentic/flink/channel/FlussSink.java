package org.agentic.flink.channel;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.agentic.flink.channel.sink.ForEachSink;
import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fluss producer side: upserts each emitted {@code T} into a {@code (key, payload)} table. A native
 * Flink 2.x ({@link org.apache.flink.api.connector.sink2 FLIP-143}) sink — the factories return a
 * {@link ForEachSink} backed by a {@link FlussWriteFn}; wire it with {@code stream.sinkTo(...)}.
 * Pairs with {@link FlussChannel} on the source side; the key is provided by a {@link KeySelector}.
 *
 * <p>Table is auto-created if missing using the same envelope layout
 * {@link FlussChannel} reads. Schema is fixed:
 *
 * <pre>
 *   CREATE TABLE database.table (
 *     key      STRING NOT NULL,
 *     payload  STRING NOT NULL,
 *     PRIMARY KEY (key) NOT ENFORCED
 *   ) DISTRIBUTED BY (key) INTO {buckets} BUCKETS;
 * </pre>
 */
public final class FlussSink<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FlussSink.class);

  static final int COL_KEY = 0;
  static final int COL_PAYLOAD = 1;

  private FlussSink() {}

  /** Convenience factory: caller supplies a key selector. */
  public static <T> ForEachSink<T> of(
      String bootstrapServers,
      String database,
      String table,
      SerializableKeySelector<T> keySelector) {
    return new Builder<T>(bootstrapServers, database, table).keySelector(keySelector).build();
  }

  /** Convenience factory: random-UUID keys per element (no upsert dedup). */
  public static <T> ForEachSink<T> randomKey(
      String bootstrapServers, String database, String table) {
    return new Builder<T>(bootstrapServers, database, table).build();
  }

  public static <T> Builder<T> builder(String bootstrapServers, String database, String table) {
    return new Builder<>(bootstrapServers, database, table);
  }

  /** Per-subtask Fluss upsert writer behavior. */
  static final class FlussWriteFn<T> implements ForEachSink.WriteFn<T> {
    private static final long serialVersionUID = 1L;

    private final String bootstrapServers;
    private final String database;
    private final String table;
    private final int buckets;
    private final SerializableKeySelector<T> keySelector;

    private transient Connection connection;
    private transient Table flussTable;
    private transient UpsertWriter writer;
    private transient ObjectMapper mapper;

    FlussWriteFn(Builder<T> b) {
      this.bootstrapServers = Objects.requireNonNull(b.bootstrapServers, "bootstrapServers");
      this.database = Objects.requireNonNull(b.database, "database");
      this.table = Objects.requireNonNull(b.table, "table");
      this.buckets = b.buckets;
      this.keySelector = b.keySelector != null ? b.keySelector : new RandomKeySelector<>();
    }

    @Override
    public void open(int subtaskIndex) throws Exception {
      Configuration conf = new Configuration();
      conf.set(ConfigOptions.BOOTSTRAP_SERVERS, List.of(bootstrapServers.split(",")));
      this.connection = ConnectionFactory.createConnection(conf);

      try (Admin admin = connection.getAdmin()) {
        TablePath path = TablePath.of(database, table);
        if (!admin.databaseExists(database).get()) {
          admin.createDatabase(database, DatabaseDescriptor.EMPTY, true).get();
        }
        if (!admin.tableExists(path).get()) {
          Schema schema =
              Schema.newBuilder()
                  .column("key", DataTypes.STRING())
                  .column("payload", DataTypes.STRING())
                  .primaryKey("key")
                  .build();
          TableDescriptor descriptor =
              TableDescriptor.builder().schema(schema).distributedBy(buckets, "key").build();
          admin.createTable(path, descriptor, true).get();
          LOG.info("fluss sink created table {} (buckets={})", path, buckets);
        }
      }

      this.flussTable = connection.getTable(TablePath.of(database, table));
      this.writer = flussTable.newUpsert().createWriter();
      this.mapper = new ObjectMapper().registerModule(new ParameterNamesModule());
      LOG.info("fluss sink open db={} table={}", database, table);
    }

    @Override
    public void write(T value) throws Exception {
      if (value == null) {
        return;
      }
      String key;
      try {
        key = keySelector.getKey(value);
      } catch (Exception e) {
        LOG.warn("fluss sink: key selector failed, falling back to random UUID: {}", e.getMessage());
        key = UUID.randomUUID().toString();
      }
      if (key == null || key.isEmpty()) {
        key = UUID.randomUUID().toString();
      }
      String payload;
      try {
        payload = mapper.writeValueAsString(value);
      } catch (IOException e) {
        throw new RuntimeException("fluss sink: JSON encode failed: " + e.getMessage(), e);
      }
      GenericRow row = new GenericRow(2);
      row.setField(COL_KEY, BinaryString.fromString(key));
      row.setField(COL_PAYLOAD, BinaryString.fromString(payload));
      writer.upsert(row).get();
    }

    @Override
    public void flush() throws Exception {
      if (writer != null) {
        writer.flush();
      }
    }

    @Override
    public void close() {
      if (flussTable != null) {
        try {
          flussTable.close();
        } catch (Exception ignored) {
          // best-effort
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception ignored) {
          // best-effort
        }
      }
    }
  }

  /** {@link KeySelector} that also implements {@link Serializable}. */
  @FunctionalInterface
  public interface SerializableKeySelector<T> extends KeySelector<T, String>, Serializable {}

  /** Default key selector — generates a fresh UUID per element. */
  public static final class RandomKeySelector<T> implements SerializableKeySelector<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(T value) {
      return UUID.randomUUID().toString();
    }
  }

  /** Builder for {@link FlussSink}. */
  public static final class Builder<T> {
    private final String bootstrapServers;
    private final String database;
    private final String table;
    private int buckets = 1;
    private SerializableKeySelector<T> keySelector;

    Builder(String bootstrapServers, String database, String table) {
      this.bootstrapServers = bootstrapServers;
      this.database = database;
      this.table = table;
    }

    public Builder<T> buckets(int buckets) {
      this.buckets = Math.max(1, buckets);
      return this;
    }

    public Builder<T> keySelector(SerializableKeySelector<T> keySelector) {
      this.keySelector = keySelector;
      return this;
    }

    public ForEachSink<T> build() {
      return new ForEachSink<>(new FlussWriteFn<>(this));
    }
  }
}

package org.agentic.flink.channel;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import org.agentic.flink.channel.source.PollingSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fluss-backed {@link Channel} that emits each row of a Fluss table as a typed {@code T}.
 *
 * <p>Table schema is the conventional two-column "envelope" layout used by every framework
 * channel: {@code key STRING PRIMARY KEY}, {@code payload STRING} (JSON-encoded {@code T}).
 * Pairs with {@link FlussSink}; durable, replayable, and serves as the boundary between
 * job hops when ZeroMQ's at-most-once semantics aren't acceptable.
 *
 * <p>Single-parallelism by design — a single {@link LogScanner} drains every bucket and emits in
 * upsert order. For higher throughput, scale up Fluss buckets and shard the consuming operator
 * downstream.
 */
public final class FlussChannel<T> implements Channel<T> {
  private static final long serialVersionUID = 1L;

  private final String bootstrapServers;
  private final String database;
  private final String table;
  private final int buckets;
  private final Class<T> type;
  private final TypeInformation<T> typeInfo;
  private final boolean fromBeginning;

  private FlussChannel(Builder<T> b) {
    this.bootstrapServers = Objects.requireNonNull(b.bootstrapServers, "bootstrapServers");
    this.database = Objects.requireNonNull(b.database, "database");
    this.table = Objects.requireNonNull(b.table, "table");
    this.buckets = b.buckets;
    this.type = Objects.requireNonNull(b.type, "type");
    this.typeInfo = b.typeInfo != null ? b.typeInfo : TypeInformation.of(b.type);
    this.fromBeginning = b.fromBeginning;
  }

  /** Convenience: read all buckets of {@code database.table} from the beginning. */
  public static <T> FlussChannel<T> of(
      String bootstrapServers, String database, String table, Class<T> type) {
    return builder(bootstrapServers, database, table, type).build();
  }

  public static <T> Builder<T> builder(
      String bootstrapServers, String database, String table, Class<T> type) {
    return new Builder<>(bootstrapServers, database, table, type);
  }

  @Override
  public DataStream<T> open(StreamExecutionEnvironment env) {
    PollingSource<T> source =
        new PollingSource<>(
            new FlussLogPollFn<>(bootstrapServers, database, table, buckets, type, fromBeginning));
    return env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "fluss[" + database + "." + table + "]", typeInfo)
        .setParallelism(1);
  }

  @Override
  public TypeInformation<T> elementType() {
    return typeInfo;
  }

  @Override
  public String providerName() {
    return "fluss";
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  /** Builder for {@link FlussChannel}. */
  public static final class Builder<T> {
    private final String bootstrapServers;
    private final String database;
    private final String table;
    private final Class<T> type;
    private TypeInformation<T> typeInfo;
    private int buckets = 1;
    private boolean fromBeginning = true;

    Builder(String bootstrapServers, String database, String table, Class<T> type) {
      this.bootstrapServers = bootstrapServers;
      this.database = database;
      this.table = table;
      this.type = type;
    }

    public Builder<T> typeInfo(TypeInformation<T> ti) {
      this.typeInfo = ti;
      return this;
    }

    public Builder<T> buckets(int buckets) {
      this.buckets = Math.max(1, buckets);
      return this;
    }

    public Builder<T> fromBeginning(boolean v) {
      this.fromBeginning = v;
      return this;
    }

    public FlussChannel<T> build() {
      return new FlussChannel<>(this);
    }
  }

  /**
   * Native FLIP-27 {@link PollingSource.PollFn} that tails a Fluss table's log as a stage-to-stage
   * stream: opens a {@link LogScanner} subscribed from the beginning of every bucket, and returns one
   * decoded {@code T} per {@link #poll} (buffering each {@link ScanRecords} batch). This is the
   * "Fluss logs between stages" boundary — durable, replayable, ordered per bucket.
   */
  static final class FlussLogPollFn<T> implements PollingSource.PollFn<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FlussLogPollFn.class);

    private final String bootstrapServers;
    private final String database;
    private final String table;
    private final int buckets;
    private final Class<T> type;
    private final boolean fromBeginning;

    private transient Connection connection;
    private transient Table flussTable;
    private transient LogScanner scanner;
    private transient ObjectMapper mapper;
    private transient ArrayDeque<T> buffer;

    FlussLogPollFn(
        String bootstrapServers,
        String database,
        String table,
        int buckets,
        Class<T> type,
        boolean fromBeginning) {
      this.bootstrapServers = bootstrapServers;
      this.database = database;
      this.table = table;
      this.buckets = buckets;
      this.type = type;
      this.fromBeginning = fromBeginning;
    }

    @Override
    public void open(int subtaskIndex) throws Exception {
      mapper =
          new ObjectMapper()
              .registerModule(new ParameterNamesModule())
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      buffer = new ArrayDeque<>();

      Configuration conf = new Configuration();
      conf.set(ConfigOptions.BOOTSTRAP_SERVERS, List.of(bootstrapServers.split(",")));
      connection = ConnectionFactory.createConnection(conf);
      try (Admin admin = connection.getAdmin()) {
        ensureTable(admin, TablePath.of(database, table));
      }
      flussTable = connection.getTable(TablePath.of(database, table));
      scanner = flussTable.newScan().createLogScanner();
      for (int b = 0; b < buckets; b++) {
        // Fluss 0.7 only exposes subscribeFromBeginning; "from latest" lands in a future version.
        scanner.subscribeFromBeginning(b);
      }
      LOG.info(
          "fluss source open db={} table={} buckets={} fromBeginning={}",
          database, table, buckets, fromBeginning);
    }

    @Override
    public T poll(long timeoutMs) {
      if (buffer.isEmpty()) {
        ScanRecords records = scanner.poll(Duration.ofMillis(Math.max(1, timeoutMs)));
        if (records == null || records.isEmpty()) {
          return null;
        }
        for (ScanRecord rec : records) {
          InternalRow row = rec.getRow();
          String payload = row.getString(FlussSink.COL_PAYLOAD).toString(); // col 0 = key, col 1 = payload
          try {
            buffer.add(mapper.readValue(payload, type));
          } catch (Exception e) {
            LOG.warn("fluss source: failed to decode payload, skipping: {}", e.getMessage());
          }
        }
      }
      return buffer.poll();
    }

    @Override
    public void close() {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (Exception ignored) {
          // best-effort
        }
      }
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

    private void ensureTable(Admin admin, TablePath path) throws Exception {
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
        LOG.info("fluss source created table {} (buckets={})", path, buckets);
      }
    }
  }
}

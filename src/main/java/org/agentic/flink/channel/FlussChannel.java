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
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
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
    return env.addSource(
            new Source<>(bootstrapServers, database, table, buckets, type, fromBeginning),
            typeInfo)
        .name("fluss[" + database + "." + table + "]")
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

  static final class Source<T> implements SourceFunction<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private final String bootstrapServers;
    private final String database;
    private final String table;
    private final int buckets;
    private final Class<T> type;
    private final boolean fromBeginning;

    private volatile boolean running = true;

    Source(
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
    public void run(SourceContext<T> ctx) throws Exception {
      ObjectMapper mapper =
          new ObjectMapper()
              .registerModule(new ParameterNamesModule())
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      Configuration conf = new Configuration();
      conf.set(ConfigOptions.BOOTSTRAP_SERVERS, List.of(bootstrapServers.split(",")));

      try (Connection connection = ConnectionFactory.createConnection(conf);
          Admin admin = connection.getAdmin()) {
        TablePath path = TablePath.of(database, table);
        ensureTable(admin, path);
        try (Table flussTable = connection.getTable(path);
            LogScanner scanner = flussTable.newScan().createLogScanner()) {
          for (int b = 0; b < buckets; b++) {
            // Fluss 0.7 only exposes subscribeFromBeginning; "from latest" semantics will land
            // in a future version. The fromBeginning flag is preserved for forward-compat.
            scanner.subscribeFromBeginning(b);
          }
          LOG.info(
              "fluss source open db={} table={} buckets={} fromBeginning={}",
              database,
              table,
              buckets,
              fromBeginning);

          while (running) {
            ScanRecords records = scanner.poll(Duration.ofMillis(500));
            if (records == null || records.isEmpty()) {
              continue;
            }
            for (ScanRecord rec : records) {
              if (!running) {
                return;
              }
              InternalRow row = rec.getRow();
              // payload is column 1 (col 0 is the key).
              String payload = row.getString(FlussSink.COL_PAYLOAD).toString();
              try {
                T value = mapper.readValue(payload, type);
                synchronized (ctx.getCheckpointLock()) {
                  ctx.collect(value);
                }
              } catch (Exception e) {
                LOG.warn("fluss source: failed to decode payload, skipping: {}", e.getMessage());
              }
            }
          }
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

    @Override
    public void cancel() {
      running = false;
    }
  }
}

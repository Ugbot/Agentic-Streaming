package org.agentic.flink.storage.vector;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.StorageTier;
import org.agentic.flink.storage.VectorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link VectorStore} implementation backed by <a href="https://fluss.apache.org">Apache Fluss</a>
 * for durability, with an embedded {@link InMemoryVectorStore} providing the actual similarity
 * search index.
 *
 * <p>Fluss is the source of truth: every embedding is upserted into a primary-key table whose
 * changelog is replayed on {@link #initialize(Map)} to rebuild the in-memory index after a restart.
 * Because Fluss durably persists the changelog, a fresh process can fully reconstruct its search
 * state simply by scanning the table from the beginning. Similarity queries
 * ({@link #searchSimilar}/{@link #searchSimilarWithFilter}) are served entirely from the in-memory
 * brute-force index; point reads ({@link #getEmbedding}/{@link #getMetadata}) are served by a Fluss
 * primary-key lookup (falling back to the in-memory copy if Fluss has not yet propagated the row).
 *
 * <p>Table layout (all columns {@code STRING}, primary key {@code id}):
 *
 * <ul>
 *   <li>{@code id} — the caller-supplied embedding id
 *   <li>{@code vector} — the float[] embedding encoded as a JSON array
 *   <li>{@code text} — convenience copy of {@code metadata.text} (or empty)
 *   <li>{@code metadata} — the full metadata map encoded as a JSON object
 * </ul>
 *
 * <p>Discovered via {@link java.util.ServiceLoader}; provider name {@code "fluss"}. Requires a
 * reachable Fluss coordinator/tablet cluster.
 *
 * <p>Config keys:
 *
 * <ul>
 *   <li>{@code fluss.bootstrap.servers} (default {@code "localhost:9123"})
 *   <li>{@code fluss.database} (default {@code "agentic_flink"})
 *   <li>{@code fluss.table} (default {@code "vectors"})
 *   <li>{@code fluss.buckets} (default {@code 1})
 *   <li>{@code vector.dimension} (default {@code 0} = infer from first stored vector)
 *   <li>{@code vector.similarity} (default {@code "cosine"}; also {@code euclidean}/{@code l2},
 *       {@code dot_product})
 * </ul>
 */
public final class FlussVectorStore implements VectorStore {

  private static final Logger LOG = LoggerFactory.getLogger(FlussVectorStore.class);

  private static final int COL_ID = 0;
  private static final int COL_VECTOR = 1;
  private static final int COL_TEXT = 2;
  private static final int COL_METADATA = 3;
  private static final int NUM_COLUMNS = 4;

  /** Number of consecutive empty polls that signals the changelog has been fully drained. */
  private static final int EMPTY_POLLS_TO_FINISH = 3;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Serializable configuration — re-used to reconnect in initialize().
  private String bootstrapServers;
  private String database;
  private String table;
  private int buckets;
  private int dimension;
  private String similarity;

  // Live, non-serializable resources — (re)built in initialize().
  private transient Connection connection;
  private transient Admin admin;
  private transient Table flussTable;
  private transient UpsertWriter writer;
  private transient Lookuper lookuper;
  private transient TablePath tablePath;
  private transient InMemoryVectorStore index;

  public FlussVectorStore() {}

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    this.bootstrapServers = config.getOrDefault("fluss.bootstrap.servers", "localhost:9123");
    this.database = config.getOrDefault("fluss.database", "agentic_flink");
    this.table = config.getOrDefault("fluss.table", "vectors");
    this.buckets = Integer.parseInt(config.getOrDefault("fluss.buckets", "1"));
    if (this.buckets < 1) {
      throw new IllegalArgumentException("fluss.buckets must be >= 1, was " + this.buckets);
    }
    String dim = config.get("vector.dimension");
    this.dimension = (dim == null || dim.isBlank()) ? 0 : Integer.parseInt(dim.trim());
    this.similarity = config.getOrDefault("vector.similarity", "cosine").trim().toLowerCase();

    // Embedded in-memory index — same similarity / dimension semantics.
    this.index = new InMemoryVectorStore();
    Map<String, String> indexConfig = new HashMap<>();
    if (this.dimension > 0) {
      indexConfig.put("vector.dimension", String.valueOf(this.dimension));
    }
    indexConfig.put("vector.similarity", this.similarity);
    this.index.initialize(indexConfig);

    // Connect to Fluss and ensure the database + table exist.
    Configuration conf = new Configuration();
    conf.set(ConfigOptions.BOOTSTRAP_SERVERS, List.of(bootstrapServers.split(",")));
    this.connection = ConnectionFactory.createConnection(conf);
    this.admin = connection.getAdmin();
    this.tablePath = TablePath.of(database, table);

    ensureDatabase();
    ensureTable();

    this.flussTable = connection.getTable(tablePath);
    this.writer = flussTable.newUpsert().createWriter();
    this.lookuper = flussTable.newLookup().createLookuper();

    hydrateFromChangelog();

    LOG.info(
        "FlussVectorStore initialized: bootstrap={} db={} table={} buckets={} dim={} similarity={} indexed={}",
        bootstrapServers,
        database,
        table,
        buckets,
        dimension,
        similarity,
        index.getStatistics().get("total_vectors"));
  }

  private void ensureDatabase() throws Exception {
    if (!admin.databaseExists(database).get()) {
      admin.createDatabase(database, DatabaseDescriptor.EMPTY, true).get();
      LOG.info("Created Fluss database '{}'", database);
    }
  }

  private void ensureTable() throws Exception {
    if (!admin.tableExists(tablePath).get()) {
      Schema schema =
          Schema.newBuilder()
              .column("id", DataTypes.STRING())
              .column("vector", DataTypes.STRING())
              .column("text", DataTypes.STRING())
              .column("metadata", DataTypes.STRING())
              .primaryKey("id")
              .build();
      TableDescriptor descriptor =
          TableDescriptor.builder().schema(schema).distributedBy(buckets, "id").build();
      admin.createTable(tablePath, descriptor, true).get();
      LOG.info("Created Fluss table '{}' (buckets={})", tablePath, buckets);
    }
  }

  /**
   * Rebuild the in-memory search index by replaying the table changelog from the beginning. A
   * failure here is logged but does not fail initialization — the table may be brand new / empty.
   */
  private void hydrateFromChangelog() {
    LogScanner scanner = null;
    int replayed = 0;
    try {
      scanner = flussTable.newScan().createLogScanner();
      for (int bucket = 0; bucket < buckets; bucket++) {
        scanner.subscribeFromBeginning(bucket);
      }
      int emptyPolls = 0;
      while (emptyPolls < EMPTY_POLLS_TO_FINISH) {
        ScanRecords records = scanner.poll(Duration.ofMillis(500));
        if (records == null || records.isEmpty()) {
          emptyPolls++;
          continue;
        }
        emptyPolls = 0;
        for (ScanRecord record : records) {
          if (applyChangelogRecord(record)) {
            replayed++;
          }
        }
      }
      LOG.info("Hydrated FlussVectorStore index with {} record(s) from changelog", replayed);
    } catch (Exception e) {
      LOG.warn(
          "FlussVectorStore changelog hydration failed (table may be new/empty); "
              + "starting with {} indexed record(s): {}",
          replayed,
          e.toString());
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (Exception e) {
          LOG.warn("Failed to close hydration LogScanner: {}", e.toString());
        }
      }
    }
  }

  /**
   * Apply a single changelog record to the in-memory index. Returns {@code true} if a row was
   * inserted/updated into the index.
   */
  private boolean applyChangelogRecord(ScanRecord record) {
    InternalRow row = record.getRow();
    if (row == null || row.isNullAt(COL_ID)) {
      return false;
    }
    String id = row.getString(COL_ID).toString();
    ChangeType type = record.getChangeType();
    switch (type) {
      case DELETE:
        index.deleteEmbedding(id);
        return false;
      case UPDATE_BEFORE:
        // Superseded by the matching UPDATE_AFTER; ignore to avoid transient removal.
        return false;
      case INSERT:
      case UPDATE_AFTER:
      case APPEND_ONLY:
      default:
        try {
          float[] vector = decodeVector(rowString(row, COL_VECTOR));
          Map<String, Object> metadata = decodeMetadata(rowString(row, COL_METADATA));
          index.storeEmbedding(id, vector, metadata);
          return true;
        } catch (Exception e) {
          LOG.warn("Skipping malformed changelog row id='{}': {}", id, e.toString());
          return false;
        }
    }
  }

  @Override
  public void storeEmbedding(String id, float[] embedding, Map<String, Object> metadata)
      throws Exception {
    if (id == null) throw new IllegalArgumentException("id must not be null");
    if (embedding == null) throw new IllegalArgumentException("embedding must not be null");
    if (dimension > 0 && embedding.length != dimension) {
      throw new IllegalArgumentException(
          "embedding dimension " + embedding.length + " != configured " + dimension);
    }
    Map<String, Object> meta = metadata == null ? new HashMap<>() : new HashMap<>(metadata);

    // Durable write to Fluss first, then mirror into the in-memory index.
    writer.upsert(toRow(id, embedding, meta)).get();
    writer.flush();
    index.storeEmbedding(id, embedding, meta);
  }

  @Override
  public void storeEmbeddingsBatch(
      Map<String, float[]> embeddings, Map<String, Map<String, Object>> metadata) throws Exception {
    if (embeddings == null || embeddings.isEmpty()) return;
    List<GenericRow> pending = new ArrayList<>(embeddings.size());
    List<String> ids = new ArrayList<>(embeddings.size());
    List<float[]> vectors = new ArrayList<>(embeddings.size());
    List<Map<String, Object>> metas = new ArrayList<>(embeddings.size());
    for (Map.Entry<String, float[]> e : embeddings.entrySet()) {
      float[] vec = e.getValue();
      if (vec == null) continue;
      if (dimension > 0 && vec.length != dimension) {
        throw new IllegalArgumentException(
            "embedding dimension " + vec.length + " != configured " + dimension + " for id "
                + e.getKey());
      }
      Map<String, Object> md =
          metadata == null || metadata.get(e.getKey()) == null
              ? new HashMap<>()
              : new HashMap<>(metadata.get(e.getKey()));
      pending.add(toRow(e.getKey(), vec, md));
      ids.add(e.getKey());
      vectors.add(vec);
      metas.add(md);
    }
    if (pending.isEmpty()) return;
    List<java.util.concurrent.CompletableFuture<?>> futures = new ArrayList<>(pending.size());
    for (GenericRow row : pending) {
      futures.add(writer.upsert(row));
    }
    for (java.util.concurrent.CompletableFuture<?> f : futures) {
      f.get();
    }
    writer.flush();
    for (int i = 0; i < ids.size(); i++) {
      index.storeEmbedding(ids.get(i), vectors.get(i), metas.get(i));
    }
  }

  @Override
  public List<VectorSearchResult> searchSimilar(float[] queryEmbedding, int topK) throws Exception {
    return index.searchSimilar(queryEmbedding, topK);
  }

  @Override
  public List<VectorSearchResult> searchSimilarWithFilter(
      float[] queryEmbedding, int topK, Map<String, Object> metadataFilter) throws Exception {
    return index.searchSimilarWithFilter(queryEmbedding, topK, metadataFilter);
  }

  @Override
  public void storeContextItem(String flowId, ContextItem item) {
    throw new UnsupportedOperationException(
        "FlussVectorStore.storeContextItem requires an external embedder; use storeEmbedding instead");
  }

  @Override
  public List<ContextItem> searchContextItems(String queryText, int topK) {
    throw new UnsupportedOperationException(
        "FlussVectorStore.searchContextItems requires an external embedder; use searchSimilar instead");
  }

  @Override
  public float[] getEmbedding(String id) throws Exception {
    if (id == null) return null;
    InternalRow row = lookup(id);
    if (row == null) {
      // Fall back to the in-memory index (e.g. Fluss not yet propagated).
      return index.getEmbedding(id);
    }
    String vectorJson = rowString(row, COL_VECTOR);
    return vectorJson == null ? index.getEmbedding(id) : decodeVector(vectorJson);
  }

  @Override
  public Map<String, Object> getMetadata(String id) throws Exception {
    if (id == null) return null;
    InternalRow row = lookup(id);
    if (row == null) {
      return index.getMetadata(id);
    }
    String metaJson = rowString(row, COL_METADATA);
    return metaJson == null ? index.getMetadata(id) : decodeMetadata(metaJson);
  }

  private InternalRow lookup(String id) throws Exception {
    GenericRow key = new GenericRow(1);
    key.setField(0, BinaryString.fromString(id));
    LookupResult result = lookuper.lookup(key).get();
    return result == null ? null : result.getSingletonRow();
  }

  @Override
  public void deleteEmbedding(String id) throws Exception {
    if (id == null) return;
    GenericRow key = new GenericRow(1);
    key.setField(0, BinaryString.fromString(id));
    writer.delete(key).get();
    writer.flush();
    index.deleteEmbedding(id);
  }

  @Override
  public void deleteByFlowId(String flowId) throws Exception {
    if (flowId == null) return;
    // Identify affected ids via the in-memory index (mirror of Fluss state), then delete each.
    List<String> toDelete = new ArrayList<>();
    for (VectorSearchResult r : index.searchSimilarWithFilter(zeroQuery(), Integer.MAX_VALUE, null)) {
      if (flowId.equals(r.getMetadata().get("flowId"))) {
        toDelete.add(r.getId());
      }
    }
    for (String id : toDelete) {
      deleteEmbedding(id);
    }
  }

  @Override
  public int getEmbeddingDimension() {
    return index != null ? index.getEmbeddingDimension() : dimension;
  }

  @Override
  public String getSimilarityMetric() {
    return similarity;
  }

  @Override
  public Map<String, Object> getStatistics() throws Exception {
    Map<String, Object> stats = new LinkedHashMap<>();
    Map<String, Object> indexStats = index.getStatistics();
    stats.put("total_vectors", indexStats.get("total_vectors"));
    stats.put("dimension", getEmbeddingDimension());
    stats.put("similarity", similarity);
    stats.put("database", database);
    stats.put("table", table);
    stats.put("buckets", buckets);
    stats.put("bootstrap_servers", bootstrapServers);
    return stats;
  }

  @Override
  public void createCollection(String collectionName, int dimension, Map<String, Object> config) {
    // The Fluss table is created in initialize(); a single flat namespace is used.
  }

  @Override
  public String getProviderName() {
    return "fluss";
  }

  @Override
  public StorageTier getTier() {
    return StorageTier.VECTOR;
  }

  // ---------- StorageProvider plumbing ----------

  @Override
  public void put(String key, float[] value) throws Exception {
    storeEmbedding(key, value, Map.of());
  }

  @Override
  public Optional<float[]> get(String key) throws Exception {
    return Optional.ofNullable(getEmbedding(key));
  }

  @Override
  public void delete(String key) throws Exception {
    deleteEmbedding(key);
  }

  @Override
  public boolean exists(String key) throws Exception {
    if (key == null) return false;
    return lookup(key) != null || index.exists(key);
  }

  @Override
  public void close() {
    if (writer != null) {
      try {
        writer.flush();
      } catch (Exception e) {
        LOG.warn("Failed to flush UpsertWriter on close: {}", e.toString());
      }
    }
    if (flussTable != null) {
      try {
        flussTable.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Fluss table: {}", e.toString());
      }
    }
    if (admin != null) {
      try {
        admin.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Fluss admin: {}", e.toString());
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Fluss connection: {}", e.toString());
      }
    }
    if (index != null) {
      index.close();
    }
  }

  // ---------- helpers ----------

  private GenericRow toRow(String id, float[] embedding, Map<String, Object> metadata)
      throws Exception {
    GenericRow row = new GenericRow(NUM_COLUMNS);
    row.setField(COL_ID, BinaryString.fromString(id));
    row.setField(COL_VECTOR, BinaryString.fromString(encodeVector(embedding)));
    Object text = metadata == null ? null : metadata.get("text");
    row.setField(COL_TEXT, BinaryString.fromString(text == null ? "" : String.valueOf(text)));
    row.setField(COL_METADATA, BinaryString.fromString(encodeMetadata(metadata)));
    return row;
  }

  private static String rowString(InternalRow row, int column) {
    if (row.isNullAt(column)) return null;
    BinaryString s = row.getString(column);
    return s == null ? null : s.toString();
  }

  private static String encodeVector(float[] embedding) {
    StringBuilder sb = new StringBuilder(embedding.length * 8 + 2);
    sb.append('[');
    for (int i = 0; i < embedding.length; i++) {
      if (i > 0) sb.append(',');
      sb.append(embedding[i]);
    }
    sb.append(']');
    return sb.toString();
  }

  private static float[] decodeVector(String json) throws Exception {
    if (json == null || json.isBlank()) return new float[0];
    List<Number> values = MAPPER.readValue(json, new TypeReference<List<Number>>() {});
    float[] out = new float[values.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = values.get(i).floatValue();
    }
    return out;
  }

  private static String encodeMetadata(Map<String, Object> metadata) throws Exception {
    return MAPPER.writeValueAsString(metadata == null ? Map.of() : metadata);
  }

  private static Map<String, Object> decodeMetadata(String json) throws Exception {
    if (json == null || json.isBlank()) return new HashMap<>();
    return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
  }

  /** A zero query used purely to enumerate every indexed entry for filtered deletion. */
  private float[] zeroQuery() {
    int dim = index.getEmbeddingDimension();
    return new float[dim > 0 ? dim : 1];
  }
}

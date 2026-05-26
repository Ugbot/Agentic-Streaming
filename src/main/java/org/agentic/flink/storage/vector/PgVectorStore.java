package org.agentic.flink.storage.vector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.VectorStore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link VectorStore} implementation backed by PostgreSQL + the
 * <a href="https://github.com/pgvector/pgvector">pgvector</a> extension.
 *
 * <p>Schema is created lazily on {@link #initialize(Map)} if {@code postgres.auto.create.tables}
 * is true (the default). Embeddings live in a {@code vector(dim)} column; metadata is stored as
 * {@code jsonb} for flexible querying. Cosine similarity is computed via the {@code <=>}
 * operator and converted to a "higher = more similar" score before returning to callers.
 *
 * <p>Requires the {@code postgres.dimension} configuration key on first initialize.
 */
public final class PgVectorStore implements VectorStore {

  private static final Logger LOG = LoggerFactory.getLogger(PgVectorStore.class);

  // Configuration
  private String jdbcUrl;
  private String username;
  private String password;
  private String tableName;
  private int dimension;
  private String similarity;

  // Live state
  private transient HikariDataSource dataSource;
  private transient ObjectMapper mapper;

  public PgVectorStore() {}

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    this.jdbcUrl =
        config.getOrDefault("postgres.url", "jdbc:postgresql://localhost:5432/agentic_flink");
    this.username = config.getOrDefault("postgres.user", "flink_user");
    this.password = config.getOrDefault("postgres.password", "flink_password");
    this.tableName = config.getOrDefault("pgvector.table", "agent_vectors");
    this.dimension = Integer.parseInt(config.getOrDefault("postgres.dimension", "384"));
    this.similarity = config.getOrDefault("pgvector.similarity", "cosine");

    HikariConfig hc = new HikariConfig();
    hc.setJdbcUrl(jdbcUrl);
    hc.setUsername(username);
    hc.setPassword(password);
    hc.setMaximumPoolSize(
        Integer.parseInt(config.getOrDefault("postgres.pool.max.size", "10")));
    hc.setMinimumIdle(Integer.parseInt(config.getOrDefault("postgres.pool.min.idle", "2")));
    this.dataSource = new HikariDataSource(hc);
    this.mapper = new ObjectMapper();

    if (Boolean.parseBoolean(config.getOrDefault("postgres.auto.create.tables", "true"))) {
      createSchema();
    }
    LOG.info("PgVectorStore initialized: url={} table={} dim={}", jdbcUrl, tableName, dimension);
  }

  private void createSchema() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE EXTENSION IF NOT EXISTS vector");
      st.execute(
          "CREATE TABLE IF NOT EXISTS " + tableName
              + " (id TEXT PRIMARY KEY, embedding vector("
              + dimension
              + ") NOT NULL, metadata JSONB NOT NULL DEFAULT '{}'::jsonb,"
              + " created_at TIMESTAMP NOT NULL DEFAULT now())");
      // ivfflat index is the standard pgvector choice for cosine.
      st.execute(
          "CREATE INDEX IF NOT EXISTS " + tableName + "_embedding_cos_idx ON "
              + tableName + " USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)");
    }
  }

  @Override
  public void storeEmbedding(String id, float[] embedding, Map<String, Object> metadata)
      throws Exception {
    if (embedding.length != dimension) {
      throw new IllegalArgumentException(
          "embedding dimension " + embedding.length + " != configured " + dimension);
    }
    String vec = floatArrayToPgVector(embedding);
    String json = metadata == null ? "{}" : mapper.writeValueAsString(metadata);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO " + tableName
                    + " (id, embedding, metadata) VALUES (?, ?::vector, ?::jsonb)"
                    + " ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding,"
                    + " metadata = EXCLUDED.metadata, created_at = now()")) {
      ps.setString(1, id);
      ps.setString(2, vec);
      ps.setString(3, json);
      ps.executeUpdate();
    }
  }

  @Override
  public void storeEmbeddingsBatch(
      Map<String, float[]> embeddings, Map<String, Map<String, Object>> metadata)
      throws Exception {
    if (embeddings == null || embeddings.isEmpty()) return;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO " + tableName
                    + " (id, embedding, metadata) VALUES (?, ?::vector, ?::jsonb)"
                    + " ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding,"
                    + " metadata = EXCLUDED.metadata, created_at = now()")) {
      for (Map.Entry<String, float[]> e : embeddings.entrySet()) {
        Map<String, Object> md = metadata == null ? Map.of() : metadata.getOrDefault(e.getKey(), Map.of());
        ps.setString(1, e.getKey());
        ps.setString(2, floatArrayToPgVector(e.getValue()));
        ps.setString(3, mapper.writeValueAsString(md));
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }

  @Override
  public List<VectorSearchResult> searchSimilar(float[] queryEmbedding, int topK) throws Exception {
    String vec = floatArrayToPgVector(queryEmbedding);
    String op = similarityOperator();
    List<VectorSearchResult> out = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT id, metadata, embedding " + op + " ?::vector AS distance FROM "
                    + tableName
                    + " ORDER BY embedding " + op + " ?::vector ASC LIMIT ?")) {
      ps.setString(1, vec);
      ps.setString(2, vec);
      ps.setInt(3, Math.max(1, topK));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String id = rs.getString(1);
          String metadataJson = rs.getString(2);
          double distance = rs.getDouble(3);
          @SuppressWarnings("unchecked")
          Map<String, Object> md = mapper.readValue(metadataJson, Map.class);
          out.add(new VectorSearchResult(id, distanceToScore(distance), md));
        }
      }
    }
    return out;
  }

  @Override
  public List<VectorSearchResult> searchSimilarWithFilter(
      float[] queryEmbedding, int topK, Map<String, Object> metadataFilter) throws Exception {
    if (metadataFilter == null || metadataFilter.isEmpty()) {
      return searchSimilar(queryEmbedding, topK);
    }
    String vec = floatArrayToPgVector(queryEmbedding);
    String op = similarityOperator();
    String filterJson = mapper.writeValueAsString(metadataFilter);
    List<VectorSearchResult> out = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT id, metadata, embedding " + op + " ?::vector AS distance FROM "
                    + tableName + " WHERE metadata @> ?::jsonb"
                    + " ORDER BY embedding " + op + " ?::vector ASC LIMIT ?")) {
      ps.setString(1, vec);
      ps.setString(2, filterJson);
      ps.setString(3, vec);
      ps.setInt(4, Math.max(1, topK));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String id = rs.getString(1);
          String md = rs.getString(2);
          double distance = rs.getDouble(3);
          @SuppressWarnings("unchecked")
          Map<String, Object> mdMap = mapper.readValue(md, Map.class);
          out.add(new VectorSearchResult(id, distanceToScore(distance), mdMap));
        }
      }
    }
    return out;
  }

  @Override
  public void storeContextItem(String flowId, ContextItem item) {
    throw new UnsupportedOperationException(
        "PgVectorStore.storeContextItem requires an external embedder; use storeEmbedding instead");
  }

  @Override
  public List<ContextItem> searchContextItems(String queryText, int topK) {
    throw new UnsupportedOperationException(
        "PgVectorStore.searchContextItems requires an external embedder; use searchSimilar instead");
  }

  @Override
  public float[] getEmbedding(String id) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT embedding::text FROM " + tableName + " WHERE id = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return null;
        return pgVectorToFloatArray(rs.getString(1));
      }
    }
  }

  @Override
  public Map<String, Object> getMetadata(String id) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT metadata FROM " + tableName + " WHERE id = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return Map.of();
        @SuppressWarnings("unchecked")
        Map<String, Object> md = mapper.readValue(rs.getString(1), Map.class);
        return md;
      }
    }
  }

  @Override
  public void deleteEmbedding(String id) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("DELETE FROM " + tableName + " WHERE id = ?")) {
      ps.setString(1, id);
      ps.executeUpdate();
    }
  }

  @Override
  public void deleteByFlowId(String flowId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "DELETE FROM " + tableName + " WHERE metadata->>'flow_id' = ?")) {
      ps.setString(1, flowId);
      ps.executeUpdate();
    }
  }

  @Override
  public int getEmbeddingDimension() {
    return dimension;
  }

  @Override
  public String getSimilarityMetric() {
    return similarity;
  }

  @Override
  public Map<String, Object> getStatistics() throws Exception {
    Map<String, Object> out = new HashMap<>();
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM " + tableName)) {
      if (rs.next()) out.put("total_embeddings", rs.getInt(1));
    } catch (SQLException ignored) {
      // table may not exist yet
    }
    out.put("dimension", dimension);
    out.put("similarity", similarity);
    return out;
  }

  @Override
  public void createCollection(String collectionName, int dimension, Map<String, Object> config) {
    // pgvector uses tables not collections; schema is created in initialize().
  }

  @Override
  public String getProviderName() {
    return "pgvector";
  }

  // ---------- StorageProvider plumbing ----------

  @Override
  public void put(String key, float[] value) throws Exception {
    storeEmbedding(key, value, Map.of());
  }

  @Override
  public Optional<float[]> get(String key) throws Exception {
    float[] v = getEmbedding(key);
    return Optional.ofNullable(v);
  }

  @Override
  public void delete(String key) throws Exception {
    deleteEmbedding(key);
  }

  @Override
  public boolean exists(String key) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT 1 FROM " + tableName + " WHERE id = ?")) {
      ps.setString(1, key);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  @Override
  public void close() {
    if (dataSource != null) dataSource.close();
  }

  // ---------- helpers ----------

  private String similarityOperator() {
    switch (similarity) {
      case "dot_product":
        return "<#>";
      case "euclidean":
      case "l2":
        return "<->";
      case "cosine":
      default:
        return "<=>";
    }
  }

  private float distanceToScore(double distance) {
    switch (similarity) {
      case "dot_product":
        return (float) (-distance); // pgvector returns negative dot product
      case "euclidean":
      case "l2":
        return (float) (1.0 / (1.0 + distance));
      case "cosine":
      default:
        return (float) (1.0 - distance);
    }
  }

  private static String floatArrayToPgVector(float[] v) {
    StringBuilder b = new StringBuilder(v.length * 8 + 2);
    b.append('[');
    for (int i = 0; i < v.length; i++) {
      if (i > 0) b.append(',');
      b.append(v[i]);
    }
    b.append(']');
    return b.toString();
  }

  private static float[] pgVectorToFloatArray(String s) {
    if (s == null) return null;
    String trimmed = s.trim();
    if (trimmed.startsWith("[")) trimmed = trimmed.substring(1);
    if (trimmed.endsWith("]")) trimmed = trimmed.substring(0, trimmed.length() - 1);
    String[] parts = trimmed.split(",");
    float[] out = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      out[i] = Float.parseFloat(parts[i].trim());
    }
    return out;
  }
}

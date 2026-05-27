package org.agentic.flink.storage.vector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import java.util.ArrayList;
import java.util.Collections;
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
 * {@link VectorStore} implementation backed by <a href="https://milvus.io">Milvus</a> via the
 * official Java SDK ({@code io.milvus:milvus-sdk-java:2.4.8}, v2 high-level client
 * {@link MilvusClientV2}).
 *
 * <p>Discovered via {@link java.util.ServiceLoader}; provider name {@code "milvus"}. Requires a
 * running Milvus instance (standalone or cluster) reachable at the configured host/port.
 *
 * <p>The collection layout is:
 *
 * <ul>
 *   <li>{@code id} — {@link DataType#VarChar} primary key holding the caller-supplied string id
 *   <li>{@code vector} — {@link DataType#FloatVector} of the configured {@code vector.dimension}
 *   <li>{@code metadata} — {@link DataType#JSON} field holding the serialized metadata map
 * </ul>
 *
 * <p>An index is created on the vector field with the configured metric ({@code COSINE},
 * {@code L2}, or {@code IP}) and the collection is loaded into memory on {@link #initialize(Map)}.
 *
 * <p>Configuration keys:
 *
 * <ul>
 *   <li>{@code milvus.host} (default {@code localhost})
 *   <li>{@code milvus.port} (default {@code 19530})
 *   <li>{@code milvus.token} or {@code milvus.api.key} (optional auth token)
 *   <li>{@code milvus.database} (optional database name)
 *   <li>{@code milvus.collection} (default {@code agentic_flink})
 *   <li>{@code vector.dimension} (required, integer)
 *   <li>{@code vector.similarity} (default {@code cosine}; one of cosine, euclidean/l2, dot_product)
 * </ul>
 *
 * <p>The Milvus client is {@code transient} and reconnected in {@link #initialize(Map)} so the
 * store can be serialized and distributed across the Flink cluster.
 */
public final class MilvusVectorStore implements VectorStore {

  private static final Logger LOG = LoggerFactory.getLogger(MilvusVectorStore.class);

  private static final String ID_FIELD = "id";
  private static final String VECTOR_FIELD = "vector";
  private static final String METADATA_FIELD = "metadata";
  private static final int ID_MAX_LENGTH = 512;

  // Serializable configuration (rehydrated by reconnecting in initialize()).
  private String host;
  private int port;
  private String token;
  private String database;
  private String collection;
  private int dimension;
  private String similarity;

  // Live, non-serializable state.
  private transient MilvusClientV2 client;
  private transient ObjectMapper mapper;
  private transient Gson gson;

  public MilvusVectorStore() {}

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    if (config == null) {
      config = Collections.emptyMap();
    }
    this.host = config.getOrDefault("milvus.host", "localhost");
    this.port = Integer.parseInt(config.getOrDefault("milvus.port", "19530"));
    String tok = config.get("milvus.token");
    if (tok == null || tok.isBlank()) {
      tok = config.get("milvus.api.key");
    }
    this.token = (tok == null || tok.isBlank()) ? null : tok.trim();
    String db = config.get("milvus.database");
    this.database = (db == null || db.isBlank()) ? null : db.trim();
    this.collection = config.getOrDefault("milvus.collection", "agentic_flink");

    String dim = config.get("vector.dimension");
    if (dim == null || dim.isBlank()) {
      throw new IllegalArgumentException("MilvusVectorStore requires 'vector.dimension'");
    }
    this.dimension = Integer.parseInt(dim.trim());
    if (this.dimension <= 0) {
      throw new IllegalArgumentException("vector.dimension must be positive, got " + this.dimension);
    }
    this.similarity = config.getOrDefault("vector.similarity", "cosine").trim().toLowerCase();

    this.mapper = new ObjectMapper();
    this.gson = new Gson();

    connect();
    ensureCollection(collection, dimension);
    LOG.info(
        "MilvusVectorStore initialized: host={} port={} collection={} dim={} similarity={}",
        host,
        port,
        collection,
        dimension,
        similarity);
  }

  private void connect() {
    ConnectConfig.ConnectConfigBuilder<?, ?> builder =
        ConnectConfig.builder().uri("http://" + host + ":" + port);
    if (token != null) {
      builder.token(token);
    }
    if (database != null) {
      builder.dbName(database);
    }
    this.client = new MilvusClientV2(builder.build());
  }

  private void ensureCollection(String name, int dim) {
    boolean exists =
        Boolean.TRUE.equals(
            client.hasCollection(HasCollectionReq.builder().collectionName(name).build()));
    if (!exists) {
      createCollectionInternal(name, dim);
    }
    client.loadCollection(LoadCollectionReq.builder().collectionName(name).build());
  }

  private void createCollectionInternal(String name, int dim) {
    CreateCollectionReq.CollectionSchema schema = client.createSchema();
    schema.setEnableDynamicField(true);
    schema.addField(
        AddFieldReq.builder()
            .fieldName(ID_FIELD)
            .dataType(DataType.VarChar)
            .isPrimaryKey(true)
            .autoID(false)
            .maxLength(ID_MAX_LENGTH)
            .build());
    schema.addField(
        AddFieldReq.builder()
            .fieldName(VECTOR_FIELD)
            .dataType(DataType.FloatVector)
            .dimension(dim)
            .build());
    schema.addField(
        AddFieldReq.builder().fieldName(METADATA_FIELD).dataType(DataType.JSON).build());

    IndexParam indexParam =
        IndexParam.builder()
            .fieldName(VECTOR_FIELD)
            .indexType(IndexParam.IndexType.AUTOINDEX)
            .metricType(metricType())
            .build();

    client.createCollection(
        CreateCollectionReq.builder()
            .collectionName(name)
            .collectionSchema(schema)
            .indexParams(List.of(indexParam))
            .enableDynamicField(true)
            .build());
    LOG.info("Created Milvus collection {} (dim={}, metric={})", name, dim, metricType());
  }

  @Override
  public void storeEmbedding(String id, float[] embedding, Map<String, Object> metadata)
      throws Exception {
    if (id == null) {
      throw new IllegalArgumentException("id must not be null");
    }
    if (embedding == null) {
      throw new IllegalArgumentException("embedding must not be null");
    }
    if (embedding.length != dimension) {
      throw new IllegalArgumentException(
          "embedding dimension " + embedding.length + " != configured " + dimension);
    }
    JsonObject row = toRow(id, embedding, metadata);
    client.upsert(
        UpsertReq.builder().collectionName(collection).data(List.of(row)).build());
  }

  @Override
  public void storeEmbeddingsBatch(
      Map<String, float[]> embeddings, Map<String, Map<String, Object>> metadata) throws Exception {
    if (embeddings == null || embeddings.isEmpty()) {
      return;
    }
    List<JsonObject> rows = new ArrayList<>(embeddings.size());
    for (Map.Entry<String, float[]> e : embeddings.entrySet()) {
      float[] vec = e.getValue();
      if (vec == null) {
        throw new IllegalArgumentException("null embedding for id " + e.getKey());
      }
      if (vec.length != dimension) {
        throw new IllegalArgumentException(
            "embedding dimension " + vec.length + " != configured " + dimension + " for id "
                + e.getKey());
      }
      Map<String, Object> md = metadata == null ? null : metadata.get(e.getKey());
      rows.add(toRow(e.getKey(), vec, md));
    }
    client.upsert(
        UpsertReq.builder().collectionName(collection).data(rows).build());
  }

  @Override
  public List<VectorSearchResult> searchSimilar(float[] queryEmbedding, int topK) throws Exception {
    return searchSimilarWithFilter(queryEmbedding, topK, null);
  }

  @Override
  public List<VectorSearchResult> searchSimilarWithFilter(
      float[] queryEmbedding, int topK, Map<String, Object> metadataFilter) throws Exception {
    if (queryEmbedding == null) {
      throw new IllegalArgumentException("queryEmbedding must not be null");
    }
    if (topK <= 0) {
      return new ArrayList<>();
    }
    SearchReq.SearchReqBuilder<?, ?> builder =
        SearchReq.builder()
            .collectionName(collection)
            .annsField(VECTOR_FIELD)
            .metricType(metricType())
            .topK(topK)
            .data(List.of(new FloatVec(queryEmbedding)))
            .outputFields(List.of(ID_FIELD, METADATA_FIELD));
    String expr = buildFilterExpression(metadataFilter);
    if (expr != null) {
      builder.filter(expr);
    }

    SearchResp resp = client.search(builder.build());
    List<VectorSearchResult> out = new ArrayList<>();
    List<List<SearchResp.SearchResult>> all = resp.getSearchResults();
    if (all == null || all.isEmpty()) {
      return out;
    }
    for (SearchResp.SearchResult r : all.get(0)) {
      Map<String, Object> entity = r.getEntity();
      String id = extractId(r.getId(), entity);
      Map<String, Object> md = extractMetadata(entity);
      float score = r.getScore() == null ? 0f : r.getScore();
      out.add(new VectorSearchResult(id, normalizeScore(score), md));
    }
    // Milvus returns results ordered best-first per the metric; ensure descending by score so
    // callers see "higher = more similar" consistently regardless of raw metric direction.
    out.sort((a, b) -> Float.compare(b.getScore(), a.getScore()));
    return out;
  }

  @Override
  public void storeContextItem(String flowId, ContextItem item) {
    throw new UnsupportedOperationException(
        "MilvusVectorStore.storeContextItem requires an external embedder; use storeEmbedding instead");
  }

  @Override
  public List<ContextItem> searchContextItems(String queryText, int topK) {
    throw new UnsupportedOperationException(
        "MilvusVectorStore.searchContextItems requires an external embedder; use searchSimilar instead");
  }

  @Override
  public float[] getEmbedding(String id) throws Exception {
    GetResp resp =
        client.get(
            GetReq.builder()
                .collectionName(collection)
                .ids(List.of(id))
                .outputFields(List.of(ID_FIELD, VECTOR_FIELD))
                .build());
    List<QueryResp.QueryResult> results = resp.getGetResults();
    if (results == null || results.isEmpty()) {
      return null;
    }
    return extractVector(results.get(0).getEntity());
  }

  @Override
  public Map<String, Object> getMetadata(String id) throws Exception {
    GetResp resp =
        client.get(
            GetReq.builder()
                .collectionName(collection)
                .ids(List.of(id))
                .outputFields(List.of(ID_FIELD, METADATA_FIELD))
                .build());
    List<QueryResp.QueryResult> results = resp.getGetResults();
    if (results == null || results.isEmpty()) {
      return null;
    }
    return extractMetadata(results.get(0).getEntity());
  }

  @Override
  public void deleteEmbedding(String id) throws Exception {
    client.delete(
        DeleteReq.builder().collectionName(collection).ids(List.of(id)).build());
  }

  @Override
  public void deleteByFlowId(String flowId) throws Exception {
    client.delete(
        DeleteReq.builder()
            .collectionName(collection)
            .filter(METADATA_FIELD + "[\"flowId\"] == \"" + escape(flowId) + "\"")
            .build());
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
    Map<String, Object> out = new LinkedHashMap<>();
    try {
      GetCollectionStatsResp stats =
          client.getCollectionStats(
              GetCollectionStatsReq.builder().collectionName(collection).build());
      out.put("total_vectors", stats.getNumOfEntities());
    } catch (RuntimeException e) {
      LOG.warn("Failed to read Milvus collection stats for {}: {}", collection, e.getMessage());
      out.put("total_vectors", -1L);
    }
    out.put("dimension", dimension);
    out.put("similarity", similarity);
    out.put("collection", collection);
    return out;
  }

  @Override
  public void createCollection(String collectionName, int dimension, Map<String, Object> config) {
    boolean exists =
        Boolean.TRUE.equals(
            client.hasCollection(
                HasCollectionReq.builder().collectionName(collectionName).build()));
    if (!exists) {
      createCollectionInternal(collectionName, dimension);
    }
    client.loadCollection(LoadCollectionReq.builder().collectionName(collectionName).build());
  }

  @Override
  public String getProviderName() {
    return "milvus";
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
    QueryResp resp =
        client.query(
            QueryReq.builder()
                .collectionName(collection)
                .filter(ID_FIELD + " == \"" + escape(key) + "\"")
                .outputFields(List.of(ID_FIELD))
                .limit(1L)
                .build());
    List<QueryResp.QueryResult> results = resp.getQueryResults();
    return results != null && !results.isEmpty();
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @Override
  public StorageTier getTier() {
    return StorageTier.VECTOR;
  }

  // ---------- helpers ----------

  private IndexParam.MetricType metricType() {
    switch (similarity) {
      case "euclidean":
      case "l2":
        return IndexParam.MetricType.L2;
      case "dot_product":
      case "ip":
        return IndexParam.MetricType.IP;
      case "cosine":
      default:
        return IndexParam.MetricType.COSINE;
    }
  }

  /**
   * Normalize a raw Milvus score to "higher = more similar". COSINE and IP already follow that
   * convention (larger is better). L2 is a distance (smaller is better), so it is mapped to a
   * bounded 0-1 similarity.
   */
  private float normalizeScore(float rawScore) {
    if ("euclidean".equals(similarity) || "l2".equals(similarity)) {
      return (float) (1.0 / (1.0 + rawScore));
    }
    return rawScore;
  }

  private JsonObject toRow(String id, float[] embedding, Map<String, Object> metadata)
      throws Exception {
    JsonObject row = new JsonObject();
    row.addProperty(ID_FIELD, id);
    com.google.gson.JsonArray vec = new com.google.gson.JsonArray(embedding.length);
    for (float v : embedding) {
      vec.add(v);
    }
    row.add(VECTOR_FIELD, vec);
    String json = metadata == null ? "{}" : mapper.writeValueAsString(metadata);
    row.add(METADATA_FIELD, gson.fromJson(json, JsonElement.class));
    return row;
  }

  /** Build a Milvus boolean expression from an equality metadata filter, or null if empty. */
  private String buildFilterExpression(Map<String, Object> filter) {
    if (filter == null || filter.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, Object> e : filter.entrySet()) {
      if (!first) {
        sb.append(" && ");
      }
      first = false;
      sb.append(METADATA_FIELD).append("[\"").append(escape(e.getKey())).append("\"]").append(" == ");
      Object v = e.getValue();
      if (v instanceof Number || v instanceof Boolean) {
        sb.append(v);
      } else {
        sb.append('"').append(escape(String.valueOf(v))).append('"');
      }
    }
    return sb.toString();
  }

  private static String escape(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private String extractId(Object rawId, Map<String, Object> entity) {
    if (entity != null) {
      Object fromEntity = entity.get(ID_FIELD);
      if (fromEntity != null) {
        return String.valueOf(fromEntity);
      }
    }
    return rawId == null ? null : String.valueOf(rawId);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> extractMetadata(Map<String, Object> entity) {
    if (entity == null) {
      return new HashMap<>();
    }
    Object raw = entity.get(METADATA_FIELD);
    if (raw == null) {
      return new HashMap<>();
    }
    try {
      if (raw instanceof Map) {
        return new HashMap<>((Map<String, Object>) raw);
      }
      if (raw instanceof JsonElement) {
        String json = gson.toJson((JsonElement) raw);
        return mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
      }
      // Fallback: assume it serializes to a JSON string/object.
      return mapper.readValue(String.valueOf(raw), new TypeReference<Map<String, Object>>() {});
    } catch (Exception e) {
      LOG.warn("Failed to parse Milvus metadata field: {}", e.getMessage());
      return new HashMap<>();
    }
  }

  private float[] extractVector(Map<String, Object> entity) {
    if (entity == null) {
      return null;
    }
    Object raw = entity.get(VECTOR_FIELD);
    if (raw == null) {
      return null;
    }
    if (raw instanceof float[]) {
      return (float[]) raw;
    }
    if (raw instanceof List) {
      List<?> list = (List<?>) raw;
      float[] out = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        out[i] = ((Number) list.get(i)).floatValue();
      }
      return out;
    }
    return null;
  }
}

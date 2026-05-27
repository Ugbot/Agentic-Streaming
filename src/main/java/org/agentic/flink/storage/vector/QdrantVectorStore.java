package org.agentic.flink.storage.vector;

import io.qdrant.client.ConditionFactory;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.QueryFactory;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.VectorsFactory;
import io.qdrant.client.WithPayloadSelectorFactory;
import io.qdrant.client.WithVectorsSelectorFactory;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.StorageTier;
import org.agentic.flink.storage.VectorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link VectorStore} implementation backed by <a href="https://qdrant.tech">Qdrant</a> via its
 * native gRPC client ({@code io.qdrant:client}).
 *
 * <p>Each embedding is stored as a Qdrant point. Because Qdrant point ids must be unsigned integers
 * or UUIDs, arbitrary string ids supplied by callers are deterministically mapped to a UUID via
 * {@link UUID#nameUUIDFromBytes(byte[])}; the original string id is preserved in the point payload
 * under the {@code "_id"} key and returned to callers in search results. All other metadata entries
 * are stored as payload values.
 *
 * <p>Cosine similarity is the default; scores are returned exactly as Qdrant computes them (higher
 * = more similar for cosine / dot, lower distance for Euclid — Qdrant always orders descending by
 * its score so the natural ordering is preserved).
 *
 * <p>The collection is created automatically on {@link #initialize(Map)} if it does not already
 * exist, using the configured {@code vector.dimension} and {@code vector.similarity}.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}; provider name {@code "qdrant"}. Requires a
 * running Qdrant instance reachable on its gRPC port (default {@code 6334}).
 *
 * <p>Config keys:
 *
 * <ul>
 *   <li>{@code qdrant.host} (default {@code "localhost"})
 *   <li>{@code qdrant.port} (default {@code 6334} — the gRPC port)
 *   <li>{@code qdrant.api.key} (optional)
 *   <li>{@code qdrant.collection} (default {@code "agentic_flink"})
 *   <li>{@code qdrant.use.tls} (default {@code false})
 *   <li>{@code vector.dimension} (required — used to create the collection)
 *   <li>{@code vector.similarity} (default {@code "cosine"}; also {@code euclidean}/{@code l2},
 *       {@code dot_product})
 * </ul>
 */
public final class QdrantVectorStore implements VectorStore {

  private static final Logger LOG = LoggerFactory.getLogger(QdrantVectorStore.class);

  private static final String ORIGINAL_ID_KEY = "_id";

  // Serializable configuration (re-used to reconnect in initialize()).
  private String host;
  private int port;
  private String apiKey;
  private String collection;
  private boolean useTls;
  private int dimension;
  private String similarity;

  // Live, non-serializable connection — rebuilt in initialize().
  private transient QdrantClient client;

  public QdrantVectorStore() {}

  @Override
  public void initialize(Map<String, String> config) throws Exception {
    this.host = config.getOrDefault("qdrant.host", "localhost");
    this.port = Integer.parseInt(config.getOrDefault("qdrant.port", "6334"));
    this.apiKey = config.get("qdrant.api.key");
    this.collection = config.getOrDefault("qdrant.collection", "agentic_flink");
    this.useTls = Boolean.parseBoolean(config.getOrDefault("qdrant.use.tls", "false"));
    String dim = config.get("vector.dimension");
    if (dim == null || dim.isBlank()) {
      throw new IllegalArgumentException(
          "QdrantVectorStore requires the 'vector.dimension' config key");
    }
    this.dimension = Integer.parseInt(dim.trim());
    this.similarity = config.getOrDefault("vector.similarity", "cosine").trim().toLowerCase();

    this.client = buildClient();
    ensureCollection(collection, dimension, distanceFor(similarity));

    LOG.info(
        "QdrantVectorStore initialized: host={} port={} collection={} dim={} similarity={} tls={}",
        host,
        port,
        collection,
        dimension,
        similarity,
        useTls);
  }

  private QdrantClient buildClient() {
    QdrantGrpcClient.Builder b = QdrantGrpcClient.newBuilder(host, port, useTls);
    if (apiKey != null && !apiKey.isBlank()) {
      b = b.withApiKey(apiKey);
    }
    return new QdrantClient(b.build());
  }

  private void ensureCollection(String name, int dim, Collections.Distance distance)
      throws Exception {
    boolean exists = client.collectionExistsAsync(name).get();
    if (!exists) {
      client
          .createCollectionAsync(
              name,
              Collections.VectorParams.newBuilder()
                  .setSize(dim)
                  .setDistance(distance)
                  .build())
          .get();
      LOG.info("Created Qdrant collection '{}' (dim={}, distance={})", name, dim, distance);
    }
  }

  @Override
  public void storeEmbedding(String id, float[] embedding, Map<String, Object> metadata)
      throws Exception {
    if (id == null) throw new IllegalArgumentException("id must not be null");
    if (embedding == null) throw new IllegalArgumentException("embedding must not be null");
    if (embedding.length != dimension) {
      throw new IllegalArgumentException(
          "embedding dimension " + embedding.length + " != configured " + dimension);
    }
    client.upsertAsync(collection, List.of(toPoint(id, embedding, metadata))).get();
  }

  @Override
  public void storeEmbeddingsBatch(
      Map<String, float[]> embeddings, Map<String, Map<String, Object>> metadata)
      throws Exception {
    if (embeddings == null || embeddings.isEmpty()) return;
    List<Points.PointStruct> points = new ArrayList<>(embeddings.size());
    for (Map.Entry<String, float[]> e : embeddings.entrySet()) {
      float[] vec = e.getValue();
      if (vec == null) continue;
      if (vec.length != dimension) {
        throw new IllegalArgumentException(
            "embedding dimension " + vec.length + " != configured " + dimension + " for id "
                + e.getKey());
      }
      Map<String, Object> md = metadata == null ? null : metadata.get(e.getKey());
      points.add(toPoint(e.getKey(), vec, md));
    }
    if (!points.isEmpty()) {
      client.upsertAsync(collection, points).get();
    }
  }

  @Override
  public List<VectorSearchResult> searchSimilar(float[] queryEmbedding, int topK) throws Exception {
    return runQuery(queryEmbedding, topK, null);
  }

  @Override
  public List<VectorSearchResult> searchSimilarWithFilter(
      float[] queryEmbedding, int topK, Map<String, Object> metadataFilter) throws Exception {
    return runQuery(queryEmbedding, topK, buildFilter(metadataFilter));
  }

  private List<VectorSearchResult> runQuery(float[] queryEmbedding, int topK, Points.Filter filter)
      throws Exception {
    if (queryEmbedding == null) throw new IllegalArgumentException("queryEmbedding must not be null");
    if (topK <= 0) return new ArrayList<>();

    Points.QueryPoints.Builder qb =
        Points.QueryPoints.newBuilder()
            .setCollectionName(collection)
            .setQuery(QueryFactory.nearest(queryEmbedding))
            .setLimit(topK)
            .setWithPayload(WithPayloadSelectorFactory.enable(true));
    if (filter != null) {
      qb.setFilter(filter);
    }

    List<Points.ScoredPoint> scored = client.queryAsync(qb.build()).get();
    List<VectorSearchResult> out = new ArrayList<>(scored.size());
    for (Points.ScoredPoint sp : scored) {
      Map<String, Object> payload = payloadToMap(sp.getPayloadMap());
      String originalId = resolveOriginalId(payload, sp.getId());
      out.add(new VectorSearchResult(originalId, sp.getScore(), payload));
    }
    return out;
  }

  @Override
  public void storeContextItem(String flowId, ContextItem item) {
    throw new UnsupportedOperationException(
        "QdrantVectorStore.storeContextItem requires an external embedder; use storeEmbedding instead");
  }

  @Override
  public List<ContextItem> searchContextItems(String queryText, int topK) {
    throw new UnsupportedOperationException(
        "QdrantVectorStore.searchContextItems requires an external embedder; use searchSimilar instead");
  }

  @Override
  public float[] getEmbedding(String id) throws Exception {
    Points.RetrievedPoint point = retrieve(id, true);
    if (point == null) return null;
    if (!point.getVectors().hasVector()) return null;
    List<Float> data = point.getVectors().getVector().getDataList();
    float[] out = new float[data.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = data.get(i);
    }
    return out;
  }

  @Override
  public Map<String, Object> getMetadata(String id) throws Exception {
    Points.RetrievedPoint point = retrieve(id, false);
    if (point == null) return null;
    return payloadToMap(point.getPayloadMap());
  }

  private Points.RetrievedPoint retrieve(String id, boolean withVectors) throws Exception {
    List<Points.RetrievedPoint> points =
        client
            .retrieveAsync(
                collection,
                List.of(pointId(id)),
                WithPayloadSelectorFactory.enable(true),
                WithVectorsSelectorFactory.enable(withVectors),
                null)
            .get();
    return points.isEmpty() ? null : points.get(0);
  }

  @Override
  public void deleteEmbedding(String id) throws Exception {
    client.deleteAsync(collection, List.of(pointId(id))).get();
  }

  @Override
  public void deleteByFlowId(String flowId) throws Exception {
    Points.Filter filter =
        Points.Filter.newBuilder()
            .addMust(ConditionFactory.matchKeyword("flowId", flowId))
            .build();
    client.deleteAsync(collection, filter).get();
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
    Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("total_vectors", client.countAsync(collection).get());
    stats.put("dimension", dimension);
    stats.put("similarity", similarity);
    stats.put("collection", collection);
    return stats;
  }

  @Override
  public void createCollection(String collectionName, int dimension, Map<String, Object> config)
      throws Exception {
    String sim = similarity;
    if (config != null && config.get("similarity") != null) {
      sim = String.valueOf(config.get("similarity")).trim().toLowerCase();
    }
    ensureCollection(collectionName, dimension, distanceFor(sim));
  }

  @Override
  public String getProviderName() {
    return "qdrant";
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
    return retrieve(key, false) != null;
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  // ---------- helpers ----------

  private Points.PointStruct toPoint(String id, float[] embedding, Map<String, Object> metadata) {
    Points.PointStruct.Builder b =
        Points.PointStruct.newBuilder()
            .setId(pointId(id))
            .setVectors(VectorsFactory.vectors(embedding))
            .putPayload(ORIGINAL_ID_KEY, ValueFactory.value(id));
    if (metadata != null) {
      for (Map.Entry<String, Object> e : metadata.entrySet()) {
        if (ORIGINAL_ID_KEY.equals(e.getKey())) continue; // reserved
        b.putPayload(e.getKey(), toValue(e.getValue()));
      }
    }
    return b.build();
  }

  private Points.Filter buildFilter(Map<String, Object> metadataFilter) {
    if (metadataFilter == null || metadataFilter.isEmpty()) return null;
    Points.Filter.Builder fb = Points.Filter.newBuilder();
    for (Map.Entry<String, Object> e : metadataFilter.entrySet()) {
      fb.addMust(toCondition(e.getKey(), e.getValue()));
    }
    return fb.build();
  }

  private static Points.Condition toCondition(String key, Object value) {
    if (value instanceof Boolean) {
      return ConditionFactory.match(key, (Boolean) value);
    }
    if (value instanceof Integer || value instanceof Long || value instanceof Short
        || value instanceof Byte) {
      return ConditionFactory.match(key, ((Number) value).longValue());
    }
    return ConditionFactory.matchKeyword(key, String.valueOf(value));
  }

  private static JsonWithInt.Value toValue(Object o) {
    if (o == null) return ValueFactory.nullValue();
    if (o instanceof String) return ValueFactory.value((String) o);
    if (o instanceof Boolean) return ValueFactory.value((Boolean) o);
    if (o instanceof Integer || o instanceof Long || o instanceof Short || o instanceof Byte) {
      return ValueFactory.value(((Number) o).longValue());
    }
    if (o instanceof Float || o instanceof Double) {
      return ValueFactory.value(((Number) o).doubleValue());
    }
    if (o instanceof Iterable<?>) {
      List<JsonWithInt.Value> list = new ArrayList<>();
      for (Object item : (Iterable<?>) o) {
        list.add(toValue(item));
      }
      return ValueFactory.list(list);
    }
    return ValueFactory.value(String.valueOf(o));
  }

  private static Map<String, Object> payloadToMap(Map<String, JsonWithInt.Value> payload) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<String, JsonWithInt.Value> e : payload.entrySet()) {
      out.put(e.getKey(), fromValue(e.getValue()));
    }
    return out;
  }

  private static Object fromValue(JsonWithInt.Value v) {
    switch (v.getKindCase()) {
      case STRING_VALUE:
        return v.getStringValue();
      case INTEGER_VALUE:
        return v.getIntegerValue();
      case DOUBLE_VALUE:
        return v.getDoubleValue();
      case BOOL_VALUE:
        return v.getBoolValue();
      case LIST_VALUE:
        List<Object> list = new ArrayList<>();
        for (JsonWithInt.Value item : v.getListValue().getValuesList()) {
          list.add(fromValue(item));
        }
        return list;
      case STRUCT_VALUE:
        Map<String, Object> struct = new LinkedHashMap<>();
        for (Map.Entry<String, JsonWithInt.Value> e : v.getStructValue().getFieldsMap().entrySet()) {
          struct.put(e.getKey(), fromValue(e.getValue()));
        }
        return struct;
      case NULL_VALUE:
      case KIND_NOT_SET:
      default:
        return null;
    }
  }

  private static String resolveOriginalId(Map<String, Object> payload, Points.PointId pointId) {
    Object original = payload.get(ORIGINAL_ID_KEY);
    if (original != null) {
      return String.valueOf(original);
    }
    return pointId.hasUuid() ? pointId.getUuid() : String.valueOf(pointId.getNum());
  }

  private static Points.PointId pointId(String id) {
    return PointIdFactory.id(UUID.nameUUIDFromBytes(id.getBytes()));
  }

  private static Collections.Distance distanceFor(String similarity) {
    switch (similarity == null ? "cosine" : similarity.toLowerCase()) {
      case "euclidean":
      case "l2":
        return Collections.Distance.Euclid;
      case "dot_product":
      case "dot":
        return Collections.Distance.Dot;
      case "cosine":
      default:
        return Collections.Distance.Cosine;
    }
  }
}

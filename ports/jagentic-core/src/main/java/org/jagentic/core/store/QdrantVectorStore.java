package org.jagentic.core.store;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.Retrieval;
import org.jagentic.core.VectorStore;

/** Real {@link VectorStore} backed by a Qdrant server (REST API). The reference
 * cold-tier impl — no heavy client dep, just the stable REST surface. */
public final class QdrantVectorStore implements VectorStore {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String base;
  private final String collection;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  public QdrantVectorStore(String baseUrl, String collection, int dim) {
    this.base = (baseUrl == null || baseUrl.isBlank() ? "http://localhost:6333" : baseUrl).replaceAll("/+$", "");
    this.collection = collection;
    int status = send("PUT", "/collections/" + collection,
        Map.of("vectors", Map.of("size", dim, "distance", "Cosine")));
    if (status / 100 != 2 && status != 409) { // 409 = already exists
      throw new RuntimeException("qdrant create collection -> " + status);
    }
  }

  private int send(String method, String path, Object body) {
    return request(method, path, body).statusCode();
  }

  private HttpResponse<String> request(String method, String path, Object body) {
    try {
      String json = body == null ? "" : MAPPER.writeValueAsString(body);
      HttpRequest req = HttpRequest.newBuilder(URI.create(base + path))
          .header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(json))
          .timeout(Duration.ofSeconds(10)).build();
      return http.send(req, HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new RuntimeException("qdrant " + method + " " + path + ": " + e.getMessage(), e);
    }
  }

  @Override
  public void upsert(String docId, float[] embedding, String text) {
    List<Object> vec = new ArrayList<>();
    for (float f : embedding) vec.add(f);
    send("PUT", "/collections/" + collection + "/points?wait=true", Map.of("points", List.of(
        Map.of("id", Integer.toUnsignedLong(Retrieval.fnv1a32(docId)),
            "vector", vec, "payload", Map.of("doc_id", docId, "text", text)))));
  }

  @Override
  public List<Retrieval.Scored> search(float[] query, int k) {
    List<Object> vec = new ArrayList<>();
    for (float f : query) vec.add(f);
    HttpResponse<String> resp = request("POST", "/collections/" + collection + "/points/query",
        Map.of("query", vec, "limit", k, "with_payload", true));
    List<Retrieval.Scored> out = new ArrayList<>();
    try {
      JsonNode points = MAPPER.readTree(resp.body()).path("result").path("points");
      for (JsonNode p : points) {
        out.add(new Retrieval.Scored(p.path("payload").path("doc_id").asText(),
            p.path("score").asDouble(), p.path("payload").path("text").asText()));
      }
    } catch (Exception ignore) {
      // empty on parse failure
    }
    return out;
  }
}

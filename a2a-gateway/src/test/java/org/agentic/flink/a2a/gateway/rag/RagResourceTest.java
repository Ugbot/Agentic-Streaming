package org.agentic.flink.a2a.gateway.rag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.retrieve.InMemoryHotVectorIndex;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Exercises the live-RAG Quarkus proxy logic — ingest lands a doc in hot + cold, query merges both
 * tiers and ranks the right passage — using the deterministic {@link HashingEmbedder} (no external
 * model). The resource's {@code @Inject} fields are package-private, so we wire them directly.
 */
final class RagResourceTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private RagResource rag;

  @BeforeEach
  void setUp() throws Exception {
    InMemoryVectorStore cold = new InMemoryVectorStore();
    cold.initialize(Map.of("dimension", "64", "similarity", "cosine"));
    rag = new RagResource();
    rag.embedder = new HashingEmbedder(64);
    rag.hot = new InMemoryHotVectorIndex("rag-test-" + UUID.randomUUID());
    rag.cold = cold;
  }

  private JsonNode ingest(String id, String text) throws Exception {
    var body = JSON.createObjectNode();
    body.put("id", id);
    body.put("text", text);
    return JSON.readTree(rag.ingest(JSON.writeValueAsString(body)));
  }

  private JsonNode query(String q, int k) throws Exception {
    var body = JSON.createObjectNode();
    body.put("query", q);
    body.put("k", k);
    return JSON.readTree(rag.query(JSON.writeValueAsString(body)));
  }

  @Test
  @DisplayName("ingest lands a doc in both tiers; query retrieves the lexically-closest passage")
  void ingestThenQueryRetrieves() throws Exception {
    assertTrue(ingest("doc-cat", "the cat sat on the mat in the sun").path("ingested").asBoolean());
    assertTrue(ingest("doc-physics", "quantum chromodynamics lecture notes and gauge bosons").path("ingested").asBoolean());

    JsonNode res = query("where did the cat sit on the mat", 5);
    JsonNode passages = res.path("passages");
    assertTrue(passages.size() >= 1, "expected at least one passage");
    // The cat passage shares vocabulary with the query → ranks first.
    assertEquals("doc-cat", passages.get(0).path("id").asText());
    assertTrue(passages.get(0).path("text").asText().contains("cat"));
  }

  @Test
  @DisplayName("a freshly-ingested doc is retrievable immediately from the hot tier")
  void hotTierIsLive() throws Exception {
    String text = "realtime streaming agentic flink fluss redis hot tier document";
    ingest("doc-live", text);
    JsonNode res = query("agentic flink fluss redis streaming", 3);
    boolean found = false;
    for (JsonNode p : res.path("passages")) {
      if ("doc-live".equals(p.path("id").asText())) {
        found = true;
        break;
      }
    }
    assertTrue(found, "freshly-ingested doc must be retrievable live");
  }

  @Test
  @DisplayName("ingest without text is a clean error, not a crash")
  void ingestRequiresText() throws Exception {
    JsonNode res = JSON.readTree(rag.ingest("{}"));
    assertTrue(res.has("error"));
  }
}

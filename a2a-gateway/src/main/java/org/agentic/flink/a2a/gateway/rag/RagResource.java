package org.agentic.flink.a2a.gateway.rag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.memory.vector.ScoredItem;
import org.agentic.flink.retrieve.HotVectorIndex;
import org.agentic.flink.retrieve.TwoTierRetriever;
import org.agentic.flink.storage.VectorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Quarkus inbound REST proxy for live RAG: {@code POST /rag/ingest} lands a document in both the
 * hot tier (immediately searchable) and the durable cold {@link VectorStore}; {@code POST /rag/query}
 * embeds the query and returns the merged hot+cold top-k via {@link TwoTierRetriever}. This is the
 * "take in data, hot lookup + cold lookup" surface, served — like the A2A gateway — from Quarkus.
 */
@ApplicationScoped
@Path("/rag")
public class RagResource {

  private static final Logger LOG = LoggerFactory.getLogger(RagResource.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  @Inject Embedder embedder;
  @Inject HotVectorIndex hot;
  @Inject VectorStore cold;

  @POST
  @Path("/ingest")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public String ingest(String body) {
    try {
      JsonNode req = JSON.readTree(body);
      String text = req.path("text").asText(null);
      if (text == null || text.isBlank()) {
        return error("ingest requires 'text'");
      }
      String id = req.path("id").asText(UUID.randomUUID().toString());
      Map<String, String> meta = new LinkedHashMap<>();
      JsonNode m = req.path("metadata");
      if (m.isObject()) {
        m.fields().forEachRemaining(e -> meta.put(e.getKey(), e.getValue().asText()));
      }
      float[] emb = embedder.embed(text);

      // Hot: immediately searchable. Cold: durable (text kept in metadata for retrieval).
      hot.upsert(id, emb, text, meta);
      Map<String, Object> coldMeta = new LinkedHashMap<>(meta);
      coldMeta.put("text", text);
      cold.storeEmbedding(id, emb, coldMeta);

      ObjectNode out = JSON.createObjectNode();
      out.put("id", id);
      out.put("ingested", true);
      out.put("dimension", emb.length);
      return JSON.writeValueAsString(out);
    } catch (Exception e) {
      LOG.warn("ingest failed", e);
      return error("ingest failed: " + e.getMessage());
    }
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public String query(String body) {
    try {
      JsonNode req = JSON.readTree(body);
      String q = req.path("query").asText(null);
      if (q == null || q.isBlank()) {
        return error("query requires 'query'");
      }
      int k = req.path("k").asInt(5);
      float[] emb = embedder.embed(q);

      TwoTierRetriever retriever = new TwoTierRetriever(hot, this::coldSearch, k, k);
      List<ScoredItem> hits = retriever.retrieve(emb, k);

      ObjectNode out = JSON.createObjectNode();
      out.put("query", q);
      ArrayNode passages = out.putArray("passages");
      for (ScoredItem si : hits) {
        ObjectNode p = passages.addObject();
        p.put("id", si.getId());
        p.put("score", si.getScore());
        p.put("text", si.getItem() == null ? "" : si.getItem().getContent());
      }
      return JSON.writeValueAsString(out);
    } catch (Exception e) {
      LOG.warn("query failed", e);
      return error("query failed: " + e.getMessage());
    }
  }

  /** Cold-tier adapter: VectorStore.searchSimilar -> ScoredItem (text recovered from metadata). */
  private List<ScoredItem> coldSearch(float[] query, int k) throws Exception {
    List<ScoredItem> out = new ArrayList<>();
    for (VectorStore.VectorSearchResult r : cold.searchSimilar(query, k)) {
      Object text = r.getMetadata() == null ? null : r.getMetadata().get("text");
      ContextItem item =
          new ContextItem(text == null ? "" : text.toString(), ContextPriority.SHOULD, MemoryType.LONG_TERM);
      out.add(new ScoredItem(r.getId(), r.getScore(), item));
    }
    return out;
  }

  private String error(String message) {
    try {
      ObjectNode out = JSON.createObjectNode();
      out.put("error", message);
      return JSON.writeValueAsString(out);
    } catch (Exception e) {
      return "{\"error\":\"internal\"}";
    }
  }
}

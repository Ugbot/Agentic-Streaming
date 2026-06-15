package org.jagentic.tools.rag;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.InMemoryVectorStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.VectorStore;
import org.jagentic.core.embedding.Embedder;
import org.jagentic.core.embedding.HashingEmbedder;
import org.jagentic.tools.ToolPack;
import org.jagentic.tools.ingest.Chunk;
import org.jagentic.tools.ingest.RecursiveTextChunker;

/** RAG / ingestion pack — chunk → embed → store, then semantic search and extractive answer,
 * built on jagentic-core's {@link Embedder} + {@link VectorStore} (the Flink RAG executors are
 * Flink-{@code RuntimeContext}-coupled, so this is rebuilt, not lifted). Model-free by default:
 * the hashing embedder + in-memory store run with no infra; swap the embedder/store (Ollama,
 * Qdrant, HNSW) for production. The store is shared across the pack's tools so {@code ingest}
 * then {@code search}/{@code answer} compose within one service instance. */
public final class RagPack implements ToolPack {

  private final Embedder embedder;
  private final VectorStore store;

  public RagPack() {
    this(new HashingEmbedder(256), new InMemoryVectorStore());
  }

  public RagPack(Embedder embedder, VectorStore store) {
    this.embedder = embedder;
    this.store = store;
  }

  @Override
  public String name() {
    return "rag";
  }

  @Override
  public List<String> register(org.jagentic.core.ToolRegistry registry) {
    registry.register("ingest_document",
        "Chunk a document, embed each chunk, and store it for retrieval.",
        ingestSchema(), this::ingest);
    registry.register("semantic_search",
        "Embed a query and return the most similar stored chunks.",
        searchSchema(), this::search);
    registry.register("rag_answer",
        "Retrieve the most relevant stored chunks for a query and return an extractive answer.",
        searchSchema(), this::answer);
    return List.of("ingest_document", "semantic_search", "rag_answer");
  }

  private Object ingest(Map<String, Object> p) {
    String content = str(p, "content", null);
    if (content == null || content.isBlank()) {
      throw new IllegalArgumentException("ingest_document requires 'content'");
    }
    int chunkSize = intVal(p, "chunk_size", 500);
    int overlap = intVal(p, "chunk_overlap", 50);
    String sourceId = str(p, "source_id", "doc");
    List<Chunk> chunks = new RecursiveTextChunker(chunkSize, overlap).chunk(sourceId, content);
    List<String> ids = new ArrayList<>();
    for (Chunk c : chunks) {
      store.upsert(c.getId(), embedder.embed(c.getText()), c.getText());
      ids.add(c.getId());
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("source_id", sourceId);
    result.put("chunks_indexed", ids.size());
    result.put("ids", ids);
    return result;
  }

  private Object search(Map<String, Object> p) {
    List<Retrieval.Scored> hits = doSearch(p);
    List<Map<String, Object>> results = new ArrayList<>();
    for (Retrieval.Scored s : hits) {
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("id", s.id());
      r.put("score", s.score());
      r.put("text", s.text());
      results.add(r);
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("query", str(p, "query", ""));
    out.put("result_count", results.size());
    out.put("results", results);
    return out;
  }

  private Object answer(Map<String, Object> p) {
    List<Retrieval.Scored> hits = doSearch(p);
    StringBuilder answer = new StringBuilder();
    List<Map<String, Object>> sources = new ArrayList<>();
    for (Retrieval.Scored s : hits) {
      if (answer.length() > 0) {
        answer.append("\n\n");
      }
      answer.append(s.text());
      Map<String, Object> src = new LinkedHashMap<>();
      src.put("id", s.id());
      src.put("score", s.score());
      sources.add(src);
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("query", str(p, "query", ""));
    out.put("answer", answer.length() == 0 ? "(no relevant context found)" : answer.toString());
    out.put("sources", sources);
    return out;
  }

  private List<Retrieval.Scored> doSearch(Map<String, Object> p) {
    String query = str(p, "query", null);
    if (query == null || query.isBlank()) {
      throw new IllegalArgumentException("requires 'query'");
    }
    int k = intVal(p, "k", 4);
    return store.search(embedder.embed(query), Math.max(1, k));
  }

  private static Map<String, Object> ingestSchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("content", Map.of("type", "string", "description", "Document text to ingest."));
    props.put("source_id", Map.of("type", "string", "description", "Identifier for the source (default 'doc')."));
    props.put("chunk_size", Map.of("type", "integer", "description", "Max chars per chunk (default 500)."));
    props.put("chunk_overlap", Map.of("type", "integer", "description", "Overlap chars (default 50)."));
    return objectSchema(props, List.of("content"));
  }

  private static Map<String, Object> searchSchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("query", Map.of("type", "string", "description", "The search query."));
    props.put("k", Map.of("type", "integer", "description", "Number of results (default 4)."));
    return objectSchema(props, List.of("query"));
  }

  private static Map<String, Object> objectSchema(Map<String, Object> properties, List<String> required) {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
    schema.put("required", required);
    return schema;
  }

  private static String str(Map<String, Object> m, String k, String def) {
    Object v = m == null ? null : m.get(k);
    return v == null ? def : String.valueOf(v);
  }

  private static int intVal(Map<String, Object> m, String k, int def) {
    Object v = m == null ? null : m.get(k);
    if (v instanceof Number n) {
      return n.intValue();
    }
    if (v != null) {
      try {
        return Integer.parseInt(String.valueOf(v));
      } catch (NumberFormatException ignored) {
        return def;
      }
    }
    return def;
  }
}

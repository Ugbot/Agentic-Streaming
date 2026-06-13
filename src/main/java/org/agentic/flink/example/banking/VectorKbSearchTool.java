package org.agentic.flink.example.banking;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.memory.vector.InMemoryHnswVectorMemory;
import org.agentic.flink.memory.vector.ScoredItem;
import org.agentic.flink.memory.vector.VectorEntry;
import org.agentic.flink.tools.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Semantic knowledge-base search for the CS agent: embeds the query and retrieves the nearest
 * documents from a local HNSW index ({@link InMemoryHnswVectorMemory}). Drop-in replacement for the
 * keyword {@link KbSearchTool} (same tool id {@code kb_search}), fixing the recall misses that
 * keyword search produced.
 *
 * <p>Embeddings are produced once at construction (the {@link EmbeddingConnection} chosen by
 * {@link BankingEmbeddings} — local DJL by default, {@code gemini-embedding-001} for marked runs)
 * and cached to disk keyed by {@code (model, dim, doc-set hash)} so restarts are instant: on a cache
 * hit only the HNSW graph is rebuilt (~ms), not the embeddings.
 */
public final class VectorKbSearchTool implements ToolExecutor {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorKbSearchTool.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_CONTENT = 1500;

  private final Map<String, Doc> docsById;
  private final transient InMemoryHnswVectorMemory index;
  private final transient EmbeddingClient embedder;
  private final EmbeddingSetup setup;

  private VectorKbSearchTool(
      Map<String, Doc> docsById,
      InMemoryHnswVectorMemory index,
      EmbeddingClient embedder,
      EmbeddingSetup setup) {
    this.docsById = docsById;
    this.index = index;
    this.embedder = embedder;
    this.setup = setup;
  }

  /**
   * Build the tool: load {@code kbDir}, embed every doc (or load the cache at {@code cacheDir}),
   * and populate an in-JVM HNSW index.
   */
  public static VectorKbSearchTool build(
      String kbDir, String cacheDir, BankingEmbeddings embeddings) {
    return build(kbDir, cacheDir, embeddings.connection(), embeddings.setup());
  }

  /** Build with an explicit embedding connection + setup (used by tests with hash embeddings). */
  public static VectorKbSearchTool build(
      String kbDir,
      String cacheDir,
      org.agentic.flink.embedding.EmbeddingConnection connection,
      EmbeddingSetup setup) {
    Map<String, Doc> docs = loadDocs(kbDir);
    EmbeddingClient embedder;
    try {
      embedder = connection.bind(null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to bind embedding model", e);
    }

    InMemoryHnswVectorMemory index = InMemoryHnswVectorMemory.of(setup.getDimension());
    Map<String, float[]> vectors = loadOrComputeVectors(docs, setup, embedder, cacheDir);
    for (Map.Entry<String, Doc> e : docs.entrySet()) {
      float[] vec = vectors.get(e.getKey());
      if (vec != null) {
        ContextItem item =
            new ContextItem(e.getValue().content, ContextPriority.SHOULD, MemoryType.LONG_TERM);
        index.put(new VectorEntry(e.getKey(), vec, item));
      }
    }
    LOG.info("VectorKbSearchTool ready: {} docs indexed (dim={})", index.size(), setup.getDimension());
    return new VectorKbSearchTool(docs, index, embedder, setup);
  }

  @Override
  public String getToolId() {
    return "kb_search";
  }

  @Override
  public String getDescription() {
    return "Semantic search over the Rho-Bank knowledge base. Args: query (string), top_k (int,"
        + " default 4). Returns the most relevant documents with title and content.";
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    String query = parameters == null ? "" : String.valueOf(parameters.getOrDefault("query", ""));
    int topK = 4;
    Object k = parameters == null ? null : parameters.get("top_k");
    if (k instanceof Number) {
      topK = ((Number) k).intValue();
    }
    topK = Math.max(1, Math.min(topK, 10));

    if (query.isBlank()) {
      return CompletableFuture.completedFuture(List.of());
    }
    float[] qvec = embedder.embed(query, setup);
    List<ScoredItem> hits = index.search(qvec, topK);
    List<Map<String, Object>> out = new ArrayList<>(hits.size());
    for (ScoredItem hit : hits) {
      Doc doc = docsById.get(hit.getId());
      if (doc == null) {
        continue;
      }
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("doc_id", doc.id);
      row.put("title", doc.title);
      row.put(
          "content",
          doc.content.length() > MAX_CONTENT ? doc.content.substring(0, MAX_CONTENT) : doc.content);
      out.add(row);
    }
    return CompletableFuture.completedFuture(out);
  }

  // ---- doc loading ----

  private static Map<String, Doc> loadDocs(String kbDir) {
    Map<String, Doc> docs = new LinkedHashMap<>();
    File dir = new File(kbDir);
    File[] files = dir.listFiles((d, n) -> n.endsWith(".json"));
    if (files != null) {
      java.util.Arrays.sort(files); // stable doc-set ordering for the cache hash
      for (File f : files) {
        try {
          Map<?, ?> m = JSON.readValue(Files.readAllBytes(Path.of(f.getPath())), Map.class);
          String id = str(m.get("id"));
          docs.put(id, new Doc(id, str(m.get("title")), str(m.get("content"))));
        } catch (IOException e) {
          LOG.warn("Skipping unreadable KB doc {}: {}", f.getName(), e.getMessage());
        }
      }
    }
    return docs;
  }

  // ---- embedding cache ----

  private static Map<String, float[]> loadOrComputeVectors(
      Map<String, Doc> docs, EmbeddingSetup setup, EmbeddingClient embedder, String cacheDir) {
    String key = setup.getModelName() + "_" + setup.getDimension() + "_" + docSetHash(docs);
    File cacheFile = cacheDir == null ? null : new File(cacheDir, "kb_vectors_" + key + ".bin");

    if (cacheFile != null && cacheFile.isFile()) {
      try {
        Map<String, float[]> cached = readCache(cacheFile, setup.getDimension());
        if (cached.size() == docs.size()) {
          LOG.info("Loaded {} KB embeddings from cache {}", cached.size(), cacheFile.getName());
          return cached;
        }
      } catch (Exception e) {
        LOG.warn("Ignoring unreadable embedding cache {}: {}", cacheFile, e.getMessage());
      }
    }

    LOG.info("Embedding {} KB docs with {} (one-time)…", docs.size(), setup.getModelName());
    List<String> ids = new ArrayList<>(docs.keySet());
    List<String> texts = new ArrayList<>(ids.size());
    for (String id : ids) {
      Doc d = docs.get(id);
      texts.add(d.title + "\n" + d.content);
    }
    List<float[]> embedded = embedder.embedBatch(texts, setup);
    Map<String, float[]> vectors = new LinkedHashMap<>();
    for (int i = 0; i < ids.size(); i++) {
      vectors.put(ids.get(i), embedded.get(i));
    }
    if (cacheFile != null) {
      try {
        cacheFile.getParentFile().mkdirs();
        writeCache(cacheFile, vectors);
        LOG.info("Wrote embedding cache {}", cacheFile.getName());
      } catch (Exception e) {
        LOG.warn("Could not write embedding cache {}: {}", cacheFile, e.getMessage());
      }
    }
    return vectors;
  }

  private static Map<String, float[]> readCache(File f, int dim) throws IOException {
    Map<String, float[]> out = new LinkedHashMap<>();
    try (DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(Files.newInputStream(f.toPath())))) {
      int storedDim = in.readInt();
      if (storedDim != dim) {
        throw new IOException("cache dim " + storedDim + " != " + dim);
      }
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        String id = in.readUTF();
        float[] v = new float[dim];
        for (int j = 0; j < dim; j++) {
          v[j] = in.readFloat();
        }
        out.put(id, v);
      }
    }
    return out;
  }

  private static void writeCache(File f, Map<String, float[]> vectors) throws IOException {
    try (DataOutputStream out = new DataOutputStream(new java.io.BufferedOutputStream(Files.newOutputStream(f.toPath())))) {
      int dim = vectors.values().iterator().next().length;
      out.writeInt(dim);
      out.writeInt(vectors.size());
      for (Map.Entry<String, float[]> e : vectors.entrySet()) {
        out.writeUTF(e.getKey());
        for (float v : e.getValue()) {
          out.writeFloat(v);
        }
      }
    }
  }

  private static String docSetHash(Map<String, Doc> docs) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      for (String id : docs.keySet()) {
        md.update(id.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }
      byte[] h = md.digest();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 6; i++) {
        sb.append(String.format("%02x", h[i]));
      }
      return sb.toString();
    } catch (Exception e) {
      return Integer.toHexString(docs.keySet().hashCode());
    }
  }

  private static String str(Object o) {
    return o == null ? "" : o.toString();
  }

  private static final class Doc {
    final String id;
    final String title;
    final String content;

    Doc(String id, String title, String content) {
      this.id = id;
      this.title = title;
      this.content = content;
    }
  }
}

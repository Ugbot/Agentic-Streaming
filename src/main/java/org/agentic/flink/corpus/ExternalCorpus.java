package org.agentic.flink.corpus;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.memory.vector.ScoredItem;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.VectorStore;
import org.agentic.flink.storage.VectorStore.VectorSearchResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Corpus flavour where vectors live in an external {@code VectorStore} — pgvector, Qdrant, or
 * any third-party impl registered via the framework's {@code ServiceLoader} path.
 *
 * <p>Operators that hold an {@link ExternalCorpus} are essentially stateless: reads and writes
 * round-trip to the configured store. This is the right flavour for large corpora that don't
 * fit in keyed state or that must be shared across multiple Flink jobs.
 */
public final class ExternalCorpus implements Corpus {

  private final String name;
  private final VectorStore store;
  private final int dimension;

  ExternalCorpus(String name, VectorStore store, int dimension) {
    this.name = name;
    this.store = store;
    this.dimension = dimension;
  }

  /** Build a spec backed by the named {@link VectorStore} (resolved via {@link StorageFactory}). */
  public static CorpusSpec spec(
      String name, String backend, Map<String, String> backendConfig, int dimension) {
    return new Spec(name, backend, backendConfig, dimension);
  }

  @Override
  public CompletableFuture<Void> upsert(String id, float[] embedding, ContextItem item) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("flow_id", item == null ? null : item.getItemId());
            metadata.put("content", item == null ? null : item.getContent());
            if (item != null && item.getMetadata() != null) {
              metadata.putAll(item.getMetadata());
            }
            store.storeEmbedding(id, embedding, metadata);
          } catch (Exception e) {
            throw new RuntimeException("ExternalCorpus.upsert failed: " + e.getMessage(), e);
          }
        });
  }

  @Override
  public CompletableFuture<List<ScoredItem>> search(float[] query, int k) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            List<VectorSearchResult> hits = store.searchSimilar(query, k);
            List<ScoredItem> out = new ArrayList<>(hits.size());
            for (VectorSearchResult h : hits) {
              ContextItem meta = reconstructContextItem(h);
              out.add(new ScoredItem(h.getId(), h.getScore(), meta));
            }
            return out;
          } catch (Exception e) {
            throw new RuntimeException("ExternalCorpus.search failed: " + e.getMessage(), e);
          }
        });
  }

  @Override
  public CorpusStats stats() {
    int size = -1;
    try {
      Map<String, Object> s = store.getStatistics();
      Object total = s == null ? null : s.get("total_embeddings");
      if (total instanceof Number) size = ((Number) total).intValue();
    } catch (Exception ignored) {
      // best-effort
    }
    return new CorpusStats(name, store.getProviderName(), size, dimension);
  }

  @Override
  public void close() throws Exception {
    store.close();
  }

  private static ContextItem reconstructContextItem(VectorSearchResult hit) {
    Map<String, Object> md = hit.getMetadata() == null ? Map.of() : hit.getMetadata();
    String content = md.get("content") == null ? "" : md.get("content").toString();
    ContextItem out = new ContextItem();
    out.setItemId(hit.getId() == null ? UUID.randomUUID().toString() : hit.getId());
    out.setContent(content);
    return out;
  }

  static final class Spec implements CorpusSpec {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final String backend;
    private final Map<String, String> backendConfig;
    private final int dimension;

    Spec(String name, String backend, Map<String, String> backendConfig, int dimension) {
      this.name = Objects.requireNonNull(name, "name");
      this.backend = Objects.requireNonNull(backend, "backend");
      this.backendConfig =
          backendConfig == null ? Map.of() : new HashMap<>(backendConfig);
      this.dimension = dimension;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Corpus bind(RuntimeContext rc) throws Exception {
      VectorStore store = StorageFactory.createVectorStore(backend, backendConfig);
      return new ExternalCorpus(name, store, dimension);
    }

    @Override
    public String providerName() {
      return "ExternalCorpus(" + backend + ")";
    }
  }
}

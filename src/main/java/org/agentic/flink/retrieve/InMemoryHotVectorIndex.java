package org.agentic.flink.retrieve;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.memory.vector.ScoredItem;

/**
 * In-JVM {@link HotVectorIndex}: a capacity-bounded, brute-force cosine index over the most-recent
 * documents — the default hot tier for the embedded single-JVM deployment, where the ingest operator
 * and the query operator share a process.
 *
 * <p>Backed by a <b>process-wide shared</b> window keyed by index name (like {@code
 * InMemoryConversationStore.shared()}), so a {@code new InMemoryHotVectorIndex("docs")} on the ingest
 * side and another on the query side address the same data after the config ships in the job graph
 * (see {@link #readResolve()}). Eviction is strict LRU by {@code maxEntries}: the hot tier is a small
 * moving window of fresh data, brute-forced exactly — accurate and fast while the window is small.
 */
public final class InMemoryHotVectorIndex implements HotVectorIndex {
  private static final long serialVersionUID = 1L;

  public static final int DEFAULT_MAX_ENTRIES = 2000;

  /** Process-wide windows, one per index name; transient (not serialized) and shared per JVM. */
  private static final Map<String, Window> WINDOWS = new ConcurrentHashMap<>();

  private final String name;
  private final int maxEntries;

  public InMemoryHotVectorIndex(String name) {
    this(name, DEFAULT_MAX_ENTRIES);
  }

  public InMemoryHotVectorIndex(String name, int maxEntries) {
    this.name = java.util.Objects.requireNonNull(name, "name");
    this.maxEntries = Math.max(1, maxEntries);
  }

  private Window window() {
    return WINDOWS.computeIfAbsent(name, n -> new Window(maxEntries));
  }

  @Override
  public void upsert(String id, float[] embedding, String text, Map<String, String> metadata) {
    if (id == null || embedding == null) {
      return;
    }
    window().upsert(id, embedding, text, metadata);
  }

  @Override
  public List<ScoredItem> search(float[] query, int k) {
    if (query == null) {
      return new ArrayList<>();
    }
    return window().search(query, Math.max(1, k));
  }

  @Override
  public int size() {
    return window().size();
  }

  @Override
  public void clear() {
    window().clear();
  }

  /** On the task side, resolve back to the same shared window for this index name. */
  private Object readResolve() {
    return new InMemoryHotVectorIndex(name, maxEntries);
  }

  // ==================== shared window ====================

  private static final class Entry {
    final float[] embedding;
    final String text;
    final Map<String, String> metadata;

    Entry(float[] embedding, String text, Map<String, String> metadata) {
      this.embedding = embedding;
      this.text = text;
      this.metadata = metadata;
    }
  }

  /** LRU-bounded id→entry map with brute-force cosine KNN. Thread-safe. */
  private static final class Window {
    private final int maxEntries;
    // accessOrder=false (insertion order); re-insert on upsert refreshes recency.
    private final LinkedHashMap<String, Entry> entries;

    Window(int maxEntries) {
      this.maxEntries = maxEntries;
      this.entries =
          new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Entry> eldest) {
              return size() > Window.this.maxEntries;
            }
          };
    }

    synchronized void upsert(String id, float[] embedding, String text, Map<String, String> meta) {
      entries.remove(id); // remove then put so the entry moves to the most-recent position
      entries.put(id, new Entry(embedding, text, meta == null ? Map.of() : new LinkedHashMap<>(meta)));
    }

    synchronized int size() {
      return entries.size();
    }

    synchronized void clear() {
      entries.clear();
    }

    synchronized List<ScoredItem> search(float[] query, int k) {
      double qNorm = norm(query);
      // Min-heap of size k by score.
      PriorityQueue<ScoredItem> heap = new PriorityQueue<>(k, (a, b) -> Double.compare(a.getScore(), b.getScore()));
      for (Map.Entry<String, Entry> e : entries.entrySet()) {
        double score = cosine(query, qNorm, e.getValue().embedding);
        if (heap.size() < k) {
          heap.offer(toScored(e.getKey(), e.getValue(), score));
        } else if (heap.peek() != null && score > heap.peek().getScore()) {
          heap.poll();
          heap.offer(toScored(e.getKey(), e.getValue(), score));
        }
      }
      List<ScoredItem> out = new ArrayList<>(heap);
      out.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));
      return out;
    }

    private static ScoredItem toScored(String id, Entry e, double score) {
      ContextItem item = new ContextItem(e.text == null ? "" : e.text, ContextPriority.SHOULD, MemoryType.SHORT_TERM);
      if (e.metadata != null) {
        for (Map.Entry<String, String> m : e.metadata.entrySet()) {
          item.addMetadata(m.getKey(), m.getValue());
        }
      }
      return new ScoredItem(id, score, item);
    }
  }

  static double norm(float[] v) {
    double s = 0;
    for (float x : v) {
      s += (double) x * x;
    }
    return Math.sqrt(s);
  }

  static double cosine(float[] q, double qNorm, float[] v) {
    if (v == null || v.length != q.length) {
      return -1.0;
    }
    double dot = 0;
    double vNorm = 0;
    for (int i = 0; i < q.length; i++) {
      dot += (double) q[i] * v[i];
      vNorm += (double) v[i] * v[i];
    }
    double denom = qNorm * Math.sqrt(vNorm);
    return denom == 0 ? 0 : dot / denom;
  }
}

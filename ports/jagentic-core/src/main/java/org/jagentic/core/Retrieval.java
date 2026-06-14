package org.jagentic.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Live hot+cold retrieval — portable analogue of {@code HotVectorIndex} +
 * {@code TwoTierRetriever} + the deterministic hashing embedder. Model-free so the
 * port runs without an embedding model; swap in a real embedder/cold store later.
 */
public final class Retrieval {
  private Retrieval() {}

  /** Deterministic bag-of-words hashing embedder (L2-normalized). */
  public static float[] embed(String text, int dim) {
    float[] v = new float[dim];
    if (text != null) {
      for (String tok : text.toLowerCase().split("[^a-z0-9]+")) {
        if (tok.isEmpty()) continue;
        int h = tok.hashCode();
        v[Math.floorMod(h, dim)] += (h >>> 31) == 0 ? 1.0f : -1.0f;
      }
    }
    double n = 0;
    for (float x : v) n += (double) x * x;
    n = Math.sqrt(n);
    if (n > 0) for (int i = 0; i < dim; i++) v[i] /= (float) n;
    return v;
  }

  public static double cosine(float[] a, float[] b) {
    if (a.length != b.length) return -1.0;
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return (na == 0 || nb == 0) ? 0 : dot / (Math.sqrt(na) * Math.sqrt(nb));
  }

  /** A scored hit. */
  public record Scored(String id, double score, String text) {}

  /** Recency-bounded vector index (the hot tier). */
  public interface HotVectorIndex {
    void upsert(String id, float[] embedding, String text);

    List<Scored> search(float[] query, int k);

    int size();
  }

  /** Capacity-bounded brute-force cosine window. */
  public static final class InMemoryHotVectorIndex implements HotVectorIndex {
    private record Entry(float[] vec, String text) {}

    private final int max;
    private final LinkedHashMap<String, Entry> entries =
        new LinkedHashMap<>(16, 0.75f, false) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<String, Entry> e) {
            return size() > max;
          }
        };

    public InMemoryHotVectorIndex() {
      this(2000);
    }

    public InMemoryHotVectorIndex(int max) {
      this.max = Math.max(1, max);
    }

    @Override
    public synchronized void upsert(String id, float[] embedding, String text) {
      entries.remove(id);
      entries.put(id, new Entry(embedding, text));
    }

    @Override
    public synchronized List<Scored> search(float[] query, int k) {
      List<Scored> all = new ArrayList<>(entries.size());
      for (Map.Entry<String, Entry> e : entries.entrySet()) {
        all.add(new Scored(e.getKey(), cosine(query, e.getValue().vec()), e.getValue().text()));
      }
      all.sort((x, y) -> Double.compare(y.score(), x.score()));
      return all.subList(0, Math.min(Math.max(1, k), all.size()));
    }

    @Override
    public synchronized int size() {
      return entries.size();
    }
  }

  /** The durable tier as a search seam: (query, k) -> hits. */
  public interface ColdSearch extends BiFunction<float[], Integer, List<Scored>> {}

  /** Merge hot + cold, de-dup by id keeping the higher score, return global top-k. */
  public static final class TwoTierRetriever {
    private final HotVectorIndex hot;
    private final ColdSearch cold;
    private final int hotK;
    private final int coldK;

    public TwoTierRetriever(HotVectorIndex hot, ColdSearch cold, int hotK, int coldK) {
      this.hot = hot;
      this.cold = cold;
      this.hotK = Math.max(1, hotK);
      this.coldK = Math.max(1, coldK);
    }

    public List<Scored> retrieve(float[] query, int k) {
      Map<String, Scored> best = new LinkedHashMap<>();
      if (hot != null) {
        try {
          for (Scored s : hot.search(query, hotK)) merge(best, s);
        } catch (RuntimeException ignored) {
          // degrade to cold-only
        }
      }
      if (cold != null) {
        try {
          List<Scored> c = cold.apply(query, coldK);
          if (c != null) for (Scored s : c) merge(best, s);
        } catch (RuntimeException ignored) {
          // degrade to hot-only
        }
      }
      List<Scored> out = new ArrayList<>(best.values());
      out.sort((x, y) -> Double.compare(y.score(), x.score()));
      return out.subList(0, Math.min(Math.max(1, k), out.size()));
    }

    private static void merge(Map<String, Scored> best, Scored s) {
      Scored prior = best.get(s.id());
      if (prior == null || s.score() > prior.score()) best.put(s.id(), s);
    }
  }
}

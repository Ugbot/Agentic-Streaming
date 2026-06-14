package org.agentic.flink.memory.vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Pure, Flink-free navigable-small-world (NSW) proximity graph — the graph-ANN core shared by
 * {@link FlinkStateHnswVectorMemory} (Flink-state-backed) and {@link InMemoryHnswVectorMemory}
 * (HashMap-backed). Holds vectors + the neighbor graph in memory and answers nearest-neighbour
 * queries by beam search; persistence of the originating entries is the caller's concern.
 *
 * <p>Each node keeps at most {@link HnswBuildConfig#getM()} neighbors selected by the
 * simple-neighbor heuristic; search is a beam search seeded at the most-recently inserted node. For
 * 10⁴–10⁵ vectors at d≈384 this is sub-millisecond per query at &gt;90% recall.
 *
 * <p>Not thread-safe; callers serialize access (each operator subtask / agent owns one graph).
 */
public final class HnswGraph {

  /** A graph hit: an id and its similarity score (higher = more similar). */
  public static final class Hit {
    public final String id;
    public final double score;

    public Hit(String id, double score) {
      this.id = id;
      this.score = score;
    }
  }

  private final int dimension;
  private final HnswBuildConfig config;

  private final Map<String, float[]> vectors = new HashMap<>();
  private final Map<String, Set<String>> neighbors = new HashMap<>();
  private final List<String> idOrder = new ArrayList<>();
  private String entryPoint;

  public HnswGraph(int dimension, HnswBuildConfig config) {
    this.dimension = dimension;
    this.config = config == null ? HnswBuildConfig.defaults() : config;
  }

  public int dimension() {
    return dimension;
  }

  public int size() {
    return vectors.size();
  }

  public void clear() {
    vectors.clear();
    neighbors.clear();
    idOrder.clear();
    entryPoint = null;
  }

  /** Drop a node's vector (tombstone): search skips it; its edges are left in place. */
  public void remove(String id) {
    vectors.remove(id);
    if (id.equals(entryPoint)) {
      entryPoint = vectors.keySet().stream().findFirst().orElse(null);
    }
  }

  /** Insert (or re-insert) a vector and link it into the graph. */
  public void insert(String id, float[] vec) {
    if (vec.length != dimension) {
      throw new IllegalArgumentException(
          "Embedding dimension " + vec.length + " does not match configured dimension " + dimension);
    }
    vectors.put(id, vec);
    idOrder.add(id);
    Set<String> myNeighbors = neighbors.computeIfAbsent(id, k -> new HashSet<>());

    if (vectors.size() == 1) {
      entryPoint = id;
      return;
    }

    List<Cand> beamHits = greedyBeamSearch(vec, Math.max(config.getBeamWidth(), config.getM()));
    beamHits.sort((a, b) -> Double.compare(b.score, a.score));
    int take = Math.min(config.getM(), beamHits.size());
    for (int i = 0; i < take; i++) {
      String n = beamHits.get(i).id;
      myNeighbors.add(n);
      neighbors.computeIfAbsent(n, k -> new HashSet<>()).add(id);
      Set<String> nlist = neighbors.get(n);
      if (nlist.size() > config.getM()) {
        float[] nv = vectors.get(n);
        String worst = null;
        double worstScore = Double.POSITIVE_INFINITY;
        for (String x : nlist) {
          if (x.equals(id)) continue;
          float[] xv = vectors.get(x);
          if (xv == null) continue;
          double s = similarity(nv, xv);
          if (s < worstScore) {
            worstScore = s;
            worst = x;
          }
        }
        if (worst != null) {
          nlist.remove(worst);
        }
      }
    }
    entryPoint = id;
  }

  /** Beam-search the graph for the top-{@code k} nearest neighbours of {@code query}. */
  public List<Hit> search(float[] query, int k) {
    if (query.length != dimension) {
      throw new IllegalArgumentException(
          "Query dimension " + query.length + " does not match configured dimension " + dimension);
    }
    if (k <= 0 || vectors.isEmpty()) {
      return Collections.emptyList();
    }

    Set<String> visited = new HashSet<>();
    PriorityQueue<Cand> candidates = new PriorityQueue<>(Comparator.comparingDouble(c -> -c.score));
    PriorityQueue<Cand> results = new PriorityQueue<>(Comparator.comparingDouble(c -> c.score));

    String seed = entryPoint != null ? entryPoint : vectors.keySet().iterator().next();
    double seedScore = similarity(query, vectors.get(seed));
    Cand seedCand = new Cand(seed, seedScore);
    candidates.add(seedCand);
    results.add(seedCand);
    visited.add(seed);

    int beam = Math.max(config.getSearchBeam(), k);
    while (!candidates.isEmpty()) {
      Cand best = candidates.poll();
      Cand worstInResults = results.peek();
      if (worstInResults != null && best.score < worstInResults.score && results.size() >= beam) {
        break;
      }
      Set<String> neigh = neighbors.getOrDefault(best.id, Collections.emptySet());
      for (String n : neigh) {
        if (!visited.add(n)) continue;
        float[] v = vectors.get(n);
        if (v == null) continue; // tombstone
        double sc = similarity(query, v);
        Cand c = new Cand(n, sc);
        if (results.size() < beam) {
          results.add(c);
          candidates.add(c);
        } else if (sc > results.peek().score) {
          results.poll();
          results.add(c);
          candidates.add(c);
        }
      }
    }

    List<Cand> all = new ArrayList<>(results);
    all.sort((a, b) -> Double.compare(b.score, a.score));
    int take = Math.min(k, all.size());
    List<Hit> out = new ArrayList<>(take);
    for (int i = 0; i < take; i++) {
      out.add(new Hit(all.get(i).id, all.get(i).score));
    }
    return out;
  }

  private List<Cand> greedyBeamSearch(float[] query, int beam) {
    Set<String> visited = new HashSet<>();
    PriorityQueue<Cand> frontier = new PriorityQueue<>(Comparator.comparingDouble(c -> -c.score));
    PriorityQueue<Cand> best = new PriorityQueue<>(Comparator.comparingDouble(c -> c.score));

    String seed = entryPoint != null ? entryPoint : (idOrder.isEmpty() ? null : idOrder.get(0));
    if (seed == null) return Collections.emptyList();
    double seedScore = similarity(query, vectors.get(seed));
    Cand seedCand = new Cand(seed, seedScore);
    frontier.add(seedCand);
    best.add(seedCand);
    visited.add(seed);

    while (!frontier.isEmpty()) {
      Cand b = frontier.poll();
      Cand worst = best.peek();
      if (worst != null && b.score < worst.score && best.size() >= beam) break;
      for (String n : neighbors.getOrDefault(b.id, Collections.emptySet())) {
        if (!visited.add(n)) continue;
        float[] v = vectors.get(n);
        if (v == null) continue;
        double sc = similarity(query, v);
        Cand c = new Cand(n, sc);
        if (best.size() < beam) {
          best.add(c);
          frontier.add(c);
        } else if (sc > best.peek().score) {
          best.poll();
          best.add(c);
          frontier.add(c);
        }
      }
    }
    return new ArrayList<>(best);
  }

  private double similarity(float[] a, float[] b) {
    switch (config.getSimilarity()) {
      case DOT_PRODUCT:
        return dot(a, b);
      case NEGATIVE_L2:
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
          double d = a[i] - b[i];
          sum += d * d;
        }
        return -Math.sqrt(sum);
      case COSINE:
      default:
        double dot = dot(a, b);
        double na = norm(a);
        double nb = norm(b);
        return (na == 0 || nb == 0) ? 0 : dot / (na * nb);
    }
  }

  private static double dot(float[] a, float[] b) {
    double s = 0;
    for (int i = 0; i < a.length; i++) s += a[i] * b[i];
    return s;
  }

  private static double norm(float[] a) {
    double s = 0;
    for (float v : a) s += v * v;
    return Math.sqrt(s);
  }

  private static final class Cand {
    final String id;
    final double score;

    Cand(String id, double score) {
      this.id = id;
      this.score = score;
    }
  }
}

package org.jagentic.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/** A hand-rolled HNSW (Hierarchical Navigable Small World) index — a real approximate
 * nearest-neighbour graph that runs in-process / in-JVM, so the core doesn't need an
 * external vector database. Implements Malkov &amp; Yashunin (2016): a multi-layer
 * navigable small-world graph with greedy descent through the upper layers and an
 * {@code ef}-bounded best-first search at layer 0. Distances are {@code 1 - cosine}
 * (smaller = nearer); scores returned are cosine similarity. The level-assignment RNG is
 * seeded so recall is reproducible. Mirrors the Python/Go cores and is the lean-core
 * analogue of the Flink in-JVM {@code FlinkStateHnswVectorMemory} (which can also be backed
 * by JVector). */
public final class HnswIndex {

  private record DistItem(double dist, String id) {}

  private final int m;
  private final int m0;
  private final int efConstruction;
  private final int efSearch;
  private final double ml;
  private final Random rng;

  private final Map<String, float[]> vecs = new HashMap<>();
  private final Map<String, String> text = new HashMap<>();
  private final Map<String, Integer> level = new HashMap<>();
  // graph.get(layer).get(nodeId) -> neighbour ids
  private final List<Map<String, List<String>>> graph = new ArrayList<>();
  private String entry = null;
  private int top = -1;
  private int count = 0;

  public HnswIndex(int m, int efConstruction, int efSearch, long seed) {
    this.m = m < 2 ? 16 : m;
    this.m0 = 2 * this.m;
    this.efConstruction = efConstruction <= 0 ? 200 : efConstruction;
    this.efSearch = efSearch <= 0 ? 50 : efSearch;
    this.ml = 1.0 / Math.log(this.m);
    this.rng = new Random(seed);
  }

  public HnswIndex() {
    this(16, 200, 50, 42L);
  }

  private double distance(float[] a, String id) {
    return 1.0 - Retrieval.cosine(a, vecs.get(id));
  }

  private List<String> neighbors(String id, int layer) {
    if (layer >= graph.size()) {
      return List.of();
    }
    return graph.get(layer).getOrDefault(id, List.of());
  }

  private void ensureLayers(int lvl) {
    while (graph.size() <= lvl) {
      graph.add(new HashMap<>());
    }
  }

  private int randomLevel() {
    double r = rng.nextDouble();
    while (r <= 0.0) {
      r = rng.nextDouble();
    }
    return (int) (-Math.log(r) * ml);
  }

  private List<DistItem> searchLayer(float[] query, List<String> entryPoints, int ef, int layer) {
    Set<String> visited = new HashSet<>(entryPoints);
    PriorityQueue<DistItem> candidates = new PriorityQueue<>(Comparator.comparingDouble(DistItem::dist));
    PriorityQueue<DistItem> results = new PriorityQueue<>(Comparator.comparingDouble(DistItem::dist).reversed());
    for (String ep : entryPoints) {
      DistItem it = new DistItem(distance(query, ep), ep);
      candidates.add(it);
      results.add(it);
    }
    while (!candidates.isEmpty()) {
      DistItem c = candidates.poll();
      double worst = results.peek().dist();
      if (c.dist() > worst && results.size() >= ef) {
        break;
      }
      for (String e : neighbors(c.id(), layer)) {
        if (visited.contains(e)) {
          continue;
        }
        visited.add(e);
        double d = distance(query, e);
        worst = results.peek().dist();
        if (results.size() < ef || d < worst) {
          DistItem it = new DistItem(d, e);
          candidates.add(it);
          results.add(it);
          if (results.size() > ef) {
            results.poll();
          }
        }
      }
    }
    List<DistItem> out = new ArrayList<>(results);
    out.sort(Comparator.comparingDouble(DistItem::dist));
    return out;
  }

  private List<String> selectNeighbors(List<DistItem> candidates, int keep) {
    List<DistItem> sorted = new ArrayList<>(candidates);
    sorted.sort(Comparator.comparingDouble(DistItem::dist));
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < Math.min(keep, sorted.size()); i++) {
      ids.add(sorted.get(i).id());
    }
    return ids;
  }

  private void prune(String id, int layer) {
    int mMax = layer == 0 ? m0 : m;
    List<String> neigh = graph.get(layer).get(id);
    if (neigh == null || neigh.size() <= mMax) {
      return;
    }
    List<DistItem> items = new ArrayList<>(neigh.size());
    for (String n : neigh) {
      items.add(new DistItem(distance(vecs.get(id), n), n));
    }
    graph.get(layer).put(id, new ArrayList<>(selectNeighbors(items, mMax)));
  }

  private String greedy(float[] query, String start, int layer) {
    String cur = start;
    double curD = distance(query, cur);
    boolean changed = true;
    while (changed) {
      changed = false;
      for (String e : neighbors(cur, layer)) {
        double d = distance(query, e);
        if (d < curD) {
          cur = e;
          curD = d;
          changed = true;
        }
      }
    }
    return cur;
  }

  private static void link(Map<String, List<String>> layer, String a, String b) {
    layer.computeIfAbsent(a, k -> new ArrayList<>());
    if (!layer.get(a).contains(b)) {
      layer.get(a).add(b);
    }
  }

  /** Insert (or refresh) a vector. Refreshing an existing id updates its vector/text and
   * keeps existing links (approximate); typical KB usage inserts unique ids once. */
  public synchronized void add(String id, float[] vector, String txt) {
    boolean update = vecs.containsKey(id);
    vecs.put(id, vector.clone());
    text.put(id, txt);
    if (update) {
      return;
    }
    count++;

    int lvl = randomLevel();
    level.put(id, lvl);
    ensureLayers(lvl);
    for (int lc = 0; lc <= lvl; lc++) {
      graph.get(lc).computeIfAbsent(id, k -> new ArrayList<>());
    }

    if (entry == null) {
      entry = id;
      top = lvl;
      return;
    }

    float[] v = vecs.get(id);
    String cur = entry;
    for (int lc = top; lc > lvl; lc--) {
      cur = greedy(v, cur, lc);
    }
    int start = Math.min(lvl, top);
    for (int lc = start; lc >= 0; lc--) {
      List<DistItem> found = searchLayer(v, List.of(cur), efConstruction, lc);
      int keep = lc == 0 ? m0 : m;
      Map<String, List<String>> layer = graph.get(lc);
      for (String nb : selectNeighbors(found, keep)) {
        link(layer, id, nb);
        link(layer, nb, id);
        prune(nb, lc);
      }
      if (!found.isEmpty()) {
        cur = found.get(0).id();
      }
    }

    if (lvl > top) {
      top = lvl;
      entry = id;
    }
  }

  /** Return the {@code k} nearest neighbours as Scored (score = cosine similarity). */
  public synchronized List<Retrieval.Scored> search(float[] query, int k) {
    if (entry == null) {
      return List.of();
    }
    String cur = entry;
    for (int lc = top; lc > 0; lc--) {
      cur = greedy(query, cur, lc);
    }
    int ef = Math.max(efSearch, k);
    List<DistItem> found = searchLayer(query, List.of(cur), ef, 0);
    int limit = Math.max(1, k);
    List<Retrieval.Scored> out = new ArrayList<>();
    for (int i = 0; i < Math.min(limit, found.size()); i++) {
      DistItem it = found.get(i);
      out.add(new Retrieval.Scored(it.id(), 1.0 - it.dist(), text.getOrDefault(it.id(), "")));
    }
    return out;
  }

  public synchronized int size() {
    return count;
  }
}

package org.jagentic.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Brute-force in-memory cold store (default). */
public final class InMemoryVectorStore implements VectorStore {
  private record Entry(float[] vec, String text) {}

  private final Map<String, Entry> docs = new LinkedHashMap<>();

  @Override
  public synchronized void upsert(String docId, float[] embedding, String text) {
    docs.put(docId, new Entry(embedding, text));
  }

  @Override
  public synchronized List<Retrieval.Scored> search(float[] query, int k) {
    List<Retrieval.Scored> hits = new ArrayList<>(docs.size());
    for (Map.Entry<String, Entry> e : docs.entrySet()) {
      hits.add(new Retrieval.Scored(e.getKey(), Retrieval.cosine(query, e.getValue().vec()), e.getValue().text()));
    }
    hits.sort((a, b) -> Double.compare(b.score(), a.score()));
    return hits.size() > Math.max(1, k) ? hits.subList(0, Math.max(1, k)) : hits;
  }
}

package org.agentic.flink.retrieve;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.agentic.flink.memory.vector.ScoredItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges a live RAG stack's two tiers into one ranked result: the {@link HotVectorIndex hot} tier
 * (recent, just-ingested documents — low latency, possibly not yet in the durable store) and a
 * {@link ColdSearch cold} tier (the durable {@link org.agentic.flink.corpus.Corpus} /
 * {@link org.agentic.flink.storage.vector.VectorStore} corpus). Both are queried, the union is
 * de-duplicated by id (keeping the higher score; a hot hit therefore supersedes a stale cold copy),
 * and the global top-{@code k} by score is returned.
 *
 * <p>This is what makes retrieval "live": a document is searchable the instant it lands in the hot
 * window, before the (slower, batch-friendly) cold index has caught up. A failure of either tier is
 * tolerated — the other tier's results are still returned — so a transient store hiccup degrades
 * recall rather than failing the query.
 */
public final class TwoTierRetriever implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(TwoTierRetriever.class);

  /** The durable tier as a simple search seam (adapt a Corpus/VectorStore to this). */
  @FunctionalInterface
  public interface ColdSearch extends Serializable {
    List<ScoredItem> search(float[] query, int k) throws Exception;
  }

  private final HotVectorIndex hot;
  private final ColdSearch cold;
  private final int hotK;
  private final int coldK;

  /**
   * @param hot recent-window index (may be null to disable the hot tier)
   * @param cold durable corpus search (may be null to disable the cold tier)
   * @param hotK candidates to pull from the hot tier before merge
   * @param coldK candidates to pull from the cold tier before merge
   */
  public TwoTierRetriever(HotVectorIndex hot, ColdSearch cold, int hotK, int coldK) {
    this.hot = hot;
    this.cold = cold;
    this.hotK = Math.max(1, hotK);
    this.coldK = Math.max(1, coldK);
  }

  /** Retrieve the global top-{@code k} across both tiers, de-duplicated by id. */
  public List<ScoredItem> retrieve(float[] query, int k) {
    if (query == null) {
      return new ArrayList<>();
    }
    // id -> best ScoredItem seen across tiers (keep the higher score).
    Map<String, ScoredItem> best = new LinkedHashMap<>();

    if (hot != null) {
      try {
        for (ScoredItem s : hot.search(query, hotK)) {
          merge(best, s);
        }
      } catch (Exception e) {
        LOG.warn("hot tier search failed (degrading to cold only): {}", e.toString());
      }
    }
    if (cold != null) {
      try {
        List<ScoredItem> coldHits = cold.search(query, coldK);
        if (coldHits != null) {
          for (ScoredItem s : coldHits) {
            merge(best, s);
          }
        }
      } catch (Exception e) {
        LOG.warn("cold tier search failed (degrading to hot only): {}", e.toString());
      }
    }

    List<ScoredItem> merged = new ArrayList<>(best.values());
    merged.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));
    int take = Math.min(Math.max(1, k), merged.size());
    return new ArrayList<>(merged.subList(0, take));
  }

  private static void merge(Map<String, ScoredItem> best, ScoredItem s) {
    if (s == null || s.getId() == null) {
      return;
    }
    ScoredItem prior = best.get(s.getId());
    if (prior == null || s.getScore() > prior.getScore()) {
      best.put(s.getId(), s);
    }
  }
}

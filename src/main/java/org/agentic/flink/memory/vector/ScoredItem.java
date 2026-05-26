package org.agentic.flink.memory.vector;

import org.agentic.flink.context.core.ContextItem;
import java.io.Serializable;

/**
 * Search result from {@link VectorMemory#search(float[], int)}.
 *
 * <p>{@link #score} follows the convention "higher is more similar" regardless of which similarity
 * function the underlying implementation uses (cosine, dot, negative-L2).
 */
public final class ScoredItem implements Serializable, Comparable<ScoredItem> {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final double score;
  private final ContextItem item;

  public ScoredItem(String id, double score, ContextItem item) {
    this.id = id;
    this.score = score;
    this.item = item;
  }

  public String getId() {
    return id;
  }

  public double getScore() {
    return score;
  }

  public ContextItem getItem() {
    return item;
  }

  /** Natural order is descending by score so {@code Collections.sort} yields top-k first. */
  @Override
  public int compareTo(ScoredItem other) {
    return Double.compare(other.score, this.score);
  }

  @Override
  public String toString() {
    return "ScoredItem[id=" + id + ", score=" + score + "]";
  }
}

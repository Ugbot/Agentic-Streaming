package org.jagentic.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Context-window management — MoSCoW prioritization + compaction so a transcript fits a
 * token budget. Keep MUST first, then SHOULD, then COULD; drop WON'T; stop at the budget.
 * Portable analogue of the Flink ContextWindowManager. */
public final class ContextWindowManager {
  private final int maxTokens;

  public ContextWindowManager(int maxTokens) {
    this.maxTokens = Math.max(1, maxTokens);
  }

  public List<ContextItem> compact(List<ContextItem> items) {
    List<ContextItem> cands = new ArrayList<>();
    for (ContextItem it : items) {
      if (it.priority() != Priority.WONT) {
        cands.add(it);
      }
    }
    Integer[] order = new Integer[cands.size()];
    for (int i = 0; i < order.length; i++) {
      order[i] = i;
    }
    // descending priority, stable on input order
    java.util.Arrays.sort(order, Comparator.comparingInt((Integer i) -> -cands.get(i).priority().ordinal())
        .thenComparingInt(i -> i));
    boolean[] keep = new boolean[cands.size()];
    int budget = maxTokens;
    for (int i : order) {
      if (cands.get(i).tokens() <= budget) {
        keep[i] = true;
        budget -= cands.get(i).tokens();
      }
    }
    List<ContextItem> out = new ArrayList<>();
    for (int i = 0; i < cands.size(); i++) {
      if (keep[i]) {
        out.add(cands.get(i));
      }
    }
    return out;
  }

  public int totalTokens(List<ContextItem> items) {
    return items.stream().mapToInt(ContextItem::tokens).sum();
  }
}

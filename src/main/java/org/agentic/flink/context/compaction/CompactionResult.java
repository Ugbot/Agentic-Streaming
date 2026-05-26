package org.agentic.flink.context.compaction;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Result of compaction operation */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompactionResult implements Serializable {

  private String requestId;
  private String flowId;

  private AgentContext compactedContext;
  private List<ContextItem> removedItems;
  private List<ContextItem> summarizedItems;
  private List<ContextItem> promotedToLongTerm;

  private int originalTokenCount;
  private int compactedTokenCount;
  private int tokensSaved;

  private long compactionTimeMs;

  public CompactionResult(String requestId, String flowId) {
    this.requestId = requestId;
    this.flowId = flowId;
    this.removedItems = new ArrayList<>();
    this.summarizedItems = new ArrayList<>();
    this.promotedToLongTerm = new ArrayList<>();
  }

  public void addRemovedItem(ContextItem item) {
    removedItems.add(item);
  }

  public void addSummarizedItem(ContextItem item) {
    summarizedItems.add(item);
  }

  public void addPromotedItem(ContextItem item) {
    promotedToLongTerm.add(item);
  }

  public double getCompressionRatio() {
    if (originalTokenCount == 0) return 0.0;
    return 1.0 - ((double) compactedTokenCount / originalTokenCount);
  }

  @Override
  public String toString() {
    return String.format(
        "CompactionResult[tokens: %d→%d (saved %d, %.1f%%), "
            + "removed=%d, summarized=%d, promoted=%d, time=%dms]",
        originalTokenCount,
        compactedTokenCount,
        tokensSaved,
        getCompressionRatio() * 100,
        removedItems.size(),
        summarizedItems.size(),
        promotedToLongTerm.size(),
        compactionTimeMs);
  }
}

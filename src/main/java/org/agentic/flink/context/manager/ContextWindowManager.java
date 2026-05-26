package org.agentic.flink.context.manager;

import org.agentic.flink.context.compaction.CompactionRequest;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import java.io.Serializable;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages context windows for agents Checks limits, triggers compaction, manages memory hierarchy
 */
@Data
public class ContextWindowManager implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ContextWindowManager.class);

  private final ContextWindowConfig config;

  public ContextWindowManager(ContextWindowConfig config) {
    this.config = config;
  }

  /**
   * Check if context needs compaction
   *
   * @param context Agent context to check
   * @return true if compaction needed
   */
  public boolean needsCompaction(AgentContext context) {
    if (context == null || context.getContextWindow() == null) {
      return false;
    }

    double usage = context.getContextWindow().getUsageRatio();
    boolean exceeded = usage >= config.getCompactionThreshold();

    if (exceeded) {
      LOG.info(
          "Context window compaction needed for agent {}: usage={:.1f}%",
          context.getAgentId(), usage * 100);
    }

    return exceeded;
  }

  /**
   * Check if new item can fit in context
   *
   * @param context Agent context
   * @param item Item to add
   * @return true if item can fit
   */
  public boolean canFit(AgentContext context, ContextItem item) {
    return context.getContextWindow().canFit(item);
  }

  /**
   * Add item to context, triggering compaction if needed
   *
   * @param context Agent context
   * @param item Item to add
   * @return true if added successfully, false if compaction needed first
   */
  public boolean tryAddItem(AgentContext context, ContextItem item) {
    if (!canFit(context, item)) {
      LOG.warn(
          "Cannot fit item in context for agent {}, compaction required",
          context.getAgentId());
      return false;
    }

    context.addContext(item);
    LOG.debug(
        "Added item to context for agent {}: {}",
        context.getAgentId(), context.getContextWindow());
    return true;
  }

  /**
   * Create compaction request for context
   *
   * @param context Agent context to compact
   * @param originalIntent Current agent intent/goal
   * @return Compaction request
   */
  public CompactionRequest createCompactionRequest(
      AgentContext context, String originalIntent) {
    CompactionRequest.CompactionReason reason;

    if (context.getContextWindow().getCurrentTokens() >= config.getMaxTokens()) {
      reason = CompactionRequest.CompactionReason.TOKEN_LIMIT_EXCEEDED;
    } else if (context.getContextWindow().size() >= config.getMaxItems()) {
      reason = CompactionRequest.CompactionReason.ITEM_COUNT_EXCEEDED;
    } else {
      reason = CompactionRequest.CompactionReason.SCHEDULED_COMPACTION;
    }

    LOG.info(
        "Creating compaction request for agent {} due to {}",
        context.getAgentId(), reason);

    return new CompactionRequest(context, originalIntent, reason);
  }

  /**
   * Get available space in context window
   *
   * @param context Agent context
   * @return Available tokens
   */
  public int getAvailableTokens(AgentContext context) {
    return context.getContextWindow().getAvailableTokens();
  }

  /**
   * Calculate context priority score Combines MoSCoW priority with temporal relevancy
   *
   * @param item Context item
   * @return Priority score (0.0 - 1.0)
   */
  public double calculatePriorityScore(ContextItem item) {
    double moscowScore = item.getPriority().getRetentionScore();
    double temporalScore = item.getTemporalRelevancy(System.currentTimeMillis());
    double accessScore = Math.min(1.0, item.getAccessCount() / 10.0); // Normalize by 10

    // Weighted combination
    return (moscowScore * 0.6) + (temporalScore * 0.25) + (accessScore * 0.15);
  }

  /** Configuration for context window management */
  @Data
  public static class ContextWindowConfig implements Serializable {
    private int maxTokens = 4000;
    private int maxItems = 50;
    private double compactionThreshold = 0.8; // Compact at 80% full
    private double relevancyThreshold = 0.5;
    private int longTermRetentionDays = 90;

    // MoSCoW thresholds
    private double mustKeepThreshold = 1.0;
    private double shouldKeepThreshold = 0.7;
    private double couldKeepThreshold = 0.5;

    public ContextWindowConfig() {}

    public ContextWindowConfig(int maxTokens, int maxItems, double compactionThreshold) {
      this.maxTokens = maxTokens;
      this.maxItems = maxItems;
      this.compactionThreshold = compactionThreshold;
    }
  }
}

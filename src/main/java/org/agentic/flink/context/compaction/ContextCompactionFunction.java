package org.agentic.flink.context.compaction;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.ContextWindow;
import org.agentic.flink.context.relevancy.RelevancyScorer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compacts agent context using MoSCoW prioritization
 *
 * <p>Compaction strategy: 1. Remove WONT items immediately 2. Remove low-relevancy COULD items 3.
 * Summarize SHOULD items if needed 4. Always keep MUST items 5. Promote high-relevancy items to
 * long-term storage
 */
public class ContextCompactionFunction
    extends ProcessFunction<CompactionRequest, CompactionResult> {

  private static final Logger LOG = LoggerFactory.getLogger(ContextCompactionFunction.class);
  public static final String UID = ContextCompactionFunction.class.getSimpleName();

  private final double relevancyThreshold;
  private final double longTermPromotionThreshold;
  private transient RelevancyScorer relevancyScorer;

  public ContextCompactionFunction(
      double relevancyThreshold, double longTermPromotionThreshold) {
    this.relevancyThreshold = relevancyThreshold;
    this.longTermPromotionThreshold = longTermPromotionThreshold;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    // Initialize relevancy scorer
    Map<String, String> config = new HashMap<>();
    config.put("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    config.put("modelName", "nomic-embed-text:latest");
    this.relevancyScorer = new RelevancyScorer(config);
  }

  @Override
  public void processElement(
      CompactionRequest request, Context ctx, Collector<CompactionResult> out) throws Exception {

    long startTime = System.currentTimeMillis();
    AgentContext context = request.getContext();
    String intent = request.getOriginalIntent();

    LOG.info(
        "Starting compaction for agent {}, current context: {}",
        context.getAgentId(), context.getContextWindow());

    CompactionResult result = new CompactionResult(request.getRequestId(), request.getFlowId());
    result.setOriginalTokenCount(context.getContextWindow().getCurrentTokens());

    // Phase 1: Remove WONT items immediately
    List<ContextItem> wontItems =
        context.getContextWindow().getItemsByPriority(ContextPriority.WONT);
    wontItems.forEach(
        item -> {
          context.removeContext(item);
          result.addRemovedItem(item);
        });
    LOG.debug("Phase 1: Removed {} WONT items", wontItems.size());

    // Phase 2: Score remaining items for relevancy
    Map<String, Double> relevancyScores =
        relevancyScorer.scoreAll(context.getContextWindow().getItems(), intent).get();

    // Phase 3: Remove low-relevancy COULD items
    List<ContextItem> couldItems =
        context.getContextWindow().getItemsByPriority(ContextPriority.COULD);
    for (ContextItem item : couldItems) {
      double score = relevancyScores.getOrDefault(item.getItemId(), 0.0);
      if (score < relevancyThreshold) {
        context.removeContext(item);
        result.addRemovedItem(item);
      }
    }
    LOG.debug("Phase 3: Removed {} low-relevancy COULD items", result.getRemovedItems().size());

    // Phase 4: Compress SHOULD items if still over limit
    if (context.needsCompaction()) {
      List<ContextItem> shouldItems =
          context.getContextWindow().getItemsByPriority(ContextPriority.SHOULD);

      // Sort by relevancy, keep top items
      List<ContextItem> sortedShould =
          shouldItems.stream()
              .sorted(
                  (a, b) ->
                      Double.compare(
                          relevancyScores.getOrDefault(b.getItemId(), 0.0),
                          relevancyScores.getOrDefault(a.getItemId(), 0.0)))
              .collect(Collectors.toList());

      // Remove bottom 50% of SHOULD items
      int toRemove = sortedShould.size() / 2;
      for (int i = sortedShould.size() - toRemove; i < sortedShould.size(); i++) {
        ContextItem item = sortedShould.get(i);
        context.removeContext(item);
        result.addSummarizedItem(item);
      }
      LOG.debug("Phase 4: Compressed {} SHOULD items", toRemove);
    }

    // Phase 5: Identify items for long-term promotion
    for (ContextItem item : context.getContextWindow().getItems()) {
      double score = relevancyScores.getOrDefault(item.getItemId(), 0.0);
      if (score >= longTermPromotionThreshold && item.getPriority() == ContextPriority.MUST) {
        result.addPromotedItem(item);
      }
    }
    LOG.debug(
        "Phase 5: Identified {} items for long-term promotion", result.getPromotedToLongTerm().size());

    // Finalize result
    result.setCompactedContext(context);
    result.setCompactedTokenCount(context.getContextWindow().getCurrentTokens());
    result.setTokensSaved(result.getOriginalTokenCount() - result.getCompactedTokenCount());
    result.setCompactionTimeMs(System.currentTimeMillis() - startTime);

    LOG.info(
        "Compaction complete for agent {}: {}",
        context.getAgentId(), result);

    out.collect(result);
  }
}

package org.agentic.flink.plugins.flintagents.action;
import org.apache.flink.api.common.functions.OpenContext;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.context.relevancy.RelevancyScorer;
import org.agentic.flink.memory.FlinkStateShortTermMemory;
import org.agentic.flink.memory.ShortTermMemory;
import org.agentic.flink.memory.ShortTermMemorySpec;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.config.StorageConfiguration;
import org.agentic.flink.storage.metrics.MetricsWrapper;
import org.agentic.flink.storage.metrics.StorageMetrics;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink-state-first context management action.
 *
 * <p>Short-term memory lives in Flink keyed state through a {@link ShortTermMemory} bound from
 * the supplied {@link ShortTermMemorySpec}; long-term memory is an optional external {@link
 * LongTermMemoryStore} used solely for resumption across job lifetimes and for archival of
 * long-term facts. The previous "three-tier" Flink → HOT → WARM fallback has been collapsed: HOT
 * is now the Flink state itself.
 *
 * <p>Sync to the long-term store is write-behind, triggered either by event-count interval or
 * by a successful MoSCoW compaction. Checkpoint barriers do not block on long-term writes.
 */
public class ContextManagementActionWithStorage
    extends KeyedProcessFunction<String, Event, Event> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContextManagementActionWithStorage.class);

  private final String agentId;
  private final int maxTokens;
  private final int maxItems;
  private final double compactionThreshold;
  private final double relevancyThreshold;
  private final double longTermPromotionThreshold;
  private final StorageConfiguration storageConfig;
  private final ShortTermMemorySpec shortTermSpec;
  private final int warmTierSyncInterval;

  // Flink keyed state holding the per-key event counter — drives sync-interval logic.
  private transient org.apache.flink.api.common.state.ValueState<Integer> eventCountState;

  // Operator-scoped memory bound from spec in open().
  private transient ShortTermMemory memory;

  // Optional long-term store (warm tier).
  private transient LongTermMemoryStore warmStore;
  private transient StorageMetrics warmMetrics;

  private transient RelevancyScorer relevancyScorer;

  public ContextManagementActionWithStorage(String agentId, StorageConfiguration storageConfig) {
    this(agentId, storageConfig, FlinkStateShortTermMemory.spec(), 4000, 50, 0.8, 0.5, 0.7, 50);
  }

  public ContextManagementActionWithStorage(
      String agentId,
      StorageConfiguration storageConfig,
      ShortTermMemorySpec shortTermSpec,
      int maxTokens,
      int maxItems,
      double compactionThreshold,
      double relevancyThreshold,
      double longTermPromotionThreshold,
      int warmTierSyncInterval) {
    this.agentId = agentId;
    this.storageConfig = storageConfig;
    this.shortTermSpec =
        shortTermSpec == null
            ? FlinkStateShortTermMemory.spec(Duration.ZERO)
            : shortTermSpec;
    this.maxTokens = maxTokens;
    this.maxItems = maxItems;
    this.compactionThreshold = compactionThreshold;
    this.relevancyThreshold = relevancyThreshold;
    this.longTermPromotionThreshold = longTermPromotionThreshold;
    this.warmTierSyncInterval = warmTierSyncInterval;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    org.apache.flink.api.common.state.ValueStateDescriptor<Integer> eventCountDescriptor =
        new org.apache.flink.api.common.state.ValueStateDescriptor<>(
            "shortterm.eventCount", Integer.class);
    eventCountState = getRuntimeContext().getState(eventCountDescriptor);

    memory = shortTermSpec.bind(getRuntimeContext());
    LOG.info("Short-term memory bound: provider={}", shortTermSpec.providerName());

    if (storageConfig != null
        && storageConfig.isTierConfigured(org.agentic.flink.storage.StorageTier.WARM)) {
      LongTermMemoryStore raw = storageConfig.createLongTermStore();
      MetricsWrapper<String, AgentContext> wrapped = new MetricsWrapper<>(raw);
      this.warmStore = (LongTermMemoryStore) wrapped;
      this.warmMetrics = wrapped.getMetrics();
      LOG.info("WARM tier storage initialized: {}", raw.getProviderName());
    } else {
      LOG.warn("WARM tier not configured - conversation resumption disabled");
    }

    Map<String, String> scorerConfig = new HashMap<>();
    scorerConfig.put("baseUrl", "http://localhost:11434");
    scorerConfig.put("modelName", "nomic-embed-text:latest");
    this.relevancyScorer = new RelevancyScorer(scorerConfig);

    LOG.info(
        "ContextManagementActionWithStorage initialized: agent={}, maxTokens={}, maxItems={}, "
            + "warmTierSync={}",
        agentId, maxTokens, maxItems, warmTierSyncInterval);
  }

  @Override
  public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
    String flowId = (String) event.getAttr("flowId");
    if (flowId == null) {
      flowId = "unknown";
    }

    AgentContext context = memory.getContext().orElse(null);
    if (context == null) {
      context = hydrateContext(flowId, event);
      memory.putContext(context);
      LOG.info("Hydrated context for flow {}", flowId);
    }

    Object messageContent = event.getAttributes().get("message");
    if (messageContent == null) {
      out.collect(event);
      return;
    }

    ContextItem item =
        new ContextItem(
            messageContent.toString(), ContextPriority.SHOULD, MemoryType.SHORT_TERM);
    memory.putItem(item);

    context.updateLastAccess();
    memory.putContext(context);

    Integer eventCount = eventCountState.value();
    if (eventCount == null) {
      eventCount = 0;
    }
    eventCount++;
    eventCountState.update(eventCount);

    if (warmStore != null && eventCount % warmTierSyncInterval == 0) {
      syncToWarmTier(flowId, context);
    }

    int currentTokens = memory.totalTokens();
    int currentItems = memory.size();
    double usageRatio = (double) currentTokens / maxTokens;

    if (usageRatio >= compactionThreshold || currentItems >= maxItems) {
      LOG.info(
          "Context overflow for flow {}: usage={}, triggering compaction",
          flowId, String.format("%.1f%%", usageRatio * 100));
      performCompaction(event, ctx, out, context, flowId);
      if (warmStore != null) {
        syncToWarmTier(flowId, context);
      }
    }

    out.collect(event);
  }

  private AgentContext hydrateContext(String flowId, Event event) throws Exception {
    if (warmStore != null) {
      Optional<AgentContext> loadedContext = warmStore.loadContext(flowId);
      if (loadedContext.isPresent()) {
        LOG.info("Hydrated context from WARM tier for flow {}", flowId);
        Map<String, ContextItem> facts = warmStore.loadFacts(flowId);
        for (ContextItem fact : facts.values()) {
          memory.putItem(fact);
        }
        return loadedContext.get();
      }
    }
    LOG.info("Creating new context for flow {}", flowId);
    String userId = (String) event.getAttr("userId");
    return new AgentContext(agentId, flowId, userId, maxTokens, maxItems);
  }

  private void syncToWarmTier(String flowId, AgentContext context) throws Exception {
    if (warmStore == null) return;
    warmStore.saveContext(flowId, context);

    Map<String, ContextItem> facts = new HashMap<>();
    for (ContextItem item : memory.items()) {
      if (item.getMemoryType() == MemoryType.LONG_TERM) {
        facts.put(item.getItemId(), item);
      }
    }
    if (!facts.isEmpty()) {
      warmStore.saveFacts(flowId, facts);
    }
    LOG.debug("Synced context and {} facts to WARM tier for flow {}", facts.size(), flowId);
  }

  private void performCompaction(
      Event event, Context ctx, Collector<Event> out, AgentContext context, String flowId)
      throws Exception {

    long startTime = System.currentTimeMillis();
    int originalTokens = memory.totalTokens();
    int originalItems = memory.size();

    List<ContextItem> items = memory.items();

    String currentIntent = (String) event.getAttr("currentIntent");

    List<ContextItem> wontItems =
        items.stream()
            .filter(i -> i.getPriority() == ContextPriority.WONT)
            .collect(Collectors.toList());
    items.removeAll(wontItems);
    int removedCount = wontItems.size();

    Map<String, Double> scores = new HashMap<>();
    for (ContextItem i : items) {
      double score = relevancyScorer.scoreRelevancy(i, currentIntent).get();
      scores.put(i.getItemId(), score);
    }

    List<ContextItem> couldItems =
        items.stream()
            .filter(i -> i.getPriority() == ContextPriority.COULD)
            .collect(Collectors.toList());
    List<ContextItem> lowRelevancyCould = new ArrayList<>();
    for (ContextItem i : couldItems) {
      if (scores.getOrDefault(i.getItemId(), 0.0) < relevancyThreshold) {
        lowRelevancyCould.add(i);
      }
    }
    items.removeAll(lowRelevancyCould);
    removedCount += lowRelevancyCould.size();

    int compressedCount = 0;
    if (calculateTokensForItems(items) > maxTokens * compactionThreshold) {
      List<ContextItem> shouldItems =
          items.stream()
              .filter(i -> i.getPriority() == ContextPriority.SHOULD)
              .sorted(
                  (a, b) ->
                      Double.compare(
                          scores.getOrDefault(b.getItemId(), 0.0),
                          scores.getOrDefault(a.getItemId(), 0.0)))
              .collect(Collectors.toList());
      int toRemove = shouldItems.size() / 2;
      List<ContextItem> toCompact =
          shouldItems.subList(Math.max(0, shouldItems.size() - toRemove), shouldItems.size());
      items.removeAll(toCompact);
      compressedCount = toCompact.size();
    }

    int promotedCount = 0;
    for (ContextItem i : items) {
      double score = scores.getOrDefault(i.getItemId(), 0.0);
      if (score >= longTermPromotionThreshold && i.getPriority() == ContextPriority.MUST) {
        i.setMemoryType(MemoryType.LONG_TERM);
        promotedCount++;
      }
    }

    memory.clearItems();
    for (ContextItem i : items) {
      memory.putItem(i);
    }

    int compactedTokens = memory.totalTokens();
    long compactionTimeMs = System.currentTimeMillis() - startTime;

    LOG.info(
        "Compaction complete for flow {}: {}→{} tokens, {}→{} items, removed={}, "
            + "compressed={}, promoted={}, time={}ms",
        flowId,
        originalTokens,
        compactedTokens,
        originalItems,
        items.size(),
        removedCount,
        compressedCount,
        promotedCount,
        compactionTimeMs);

    Map<String, Object> data = new HashMap<>();
    data.put("eventType", "ContextCompacted");
    data.put("originalTokens", originalTokens);
    data.put("compactedTokens", compactedTokens);
    data.put("tokensSaved", originalTokens - compactedTokens);
    data.put("removedCount", removedCount);
    data.put("compressedCount", compressedCount);
    data.put("promotedCount", promotedCount);
    data.put("compactionTimeMs", compactionTimeMs);

    OutputEvent compactionEvent = new OutputEvent(data);
    compactionEvent.setAttr("agentId", agentId);
    compactionEvent.setAttr("flowId", flowId);
    compactionEvent.setSourceTimestamp(System.currentTimeMillis());
    out.collect(compactionEvent);
  }

  private int calculateTokensForItems(List<ContextItem> items) {
    int total = 0;
    for (ContextItem i : items) {
      Integer tc = i.getTokenCount();
      if (tc != null) total += tc;
    }
    return total;
  }

  @Override
  public void close() throws Exception {
    if (warmStore != null) {
      warmStore.close();
      if (warmMetrics != null) {
        LOG.info("WARM tier metrics: {}", warmMetrics);
      }
    }
    super.close();
  }

  public String getAgentId() {
    return agentId;
  }

  public StorageMetrics getWarmMetrics() {
    return warmMetrics;
  }
}

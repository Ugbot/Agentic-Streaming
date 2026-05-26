package org.agentic.flink.plugins.flintagents.action;

import org.agentic.flink.context.compaction.CompactionResult;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.context.relevancy.RelevancyScorer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful ProcessFunction for managing agent context with fault-tolerant state.
 *
 * <p>This function integrates custom context management (MoSCoW prioritization, 5-phase compaction,
 * inverse RAG) with Apache Flink Agents event-driven architecture and Flink's state backend.
 *
 * <p><b>Key Features:</b>
 *
 * <ul>
 *   <li>✅ Fault-tolerant context storage using Flink state (ValueState, ListState, MapState)
 *   <li>✅ MoSCoW prioritization (MUST, SHOULD, COULD, WONT)
 *   <li>✅ 5-phase compaction with relevancy scoring
 *   <li>✅ Automatic overflow detection and compaction triggering
 *   <li>✅ Temporal relevancy decay
 *   <li>✅ Integration with Inverse RAG for long-term storage
 * </ul>
 *
 * <p><b>State Management:</b>
 *
 * <ul>
 *   <li>ValueState&lt;AgentContext&gt; - Core context with metadata
 *   <li>ListState&lt;ContextItem&gt; - Short-term working memory
 *   <li>MapState&lt;String, ContextItem&gt; - Long-term persistent facts
 * </ul>
 *
 * <p><b>Event Flow:</b>
 *
 * <pre>{@code
 * Event → processElement() → check overflow → emit ContextOverflowEvent
 *                                                      ↓
 *                                      compactContext() → 5-phase compaction
 *                                                      ↓
 *                                           emit ContextCompactedEvent
 *                                                      ↓
 *                                           (optional) Inverse RAG
 * }</pre>
 *
 * <p><b>Integration with Flink Agents:</b>
 *
 * This function processes Flink Agents events and emits new events. It can be used
 * alongside Flink Agents ReAct agents to provide intelligent context management.
 *
 * @author Agentic Flink Team
 * @see AgentContext
 * @see ContextItem
 * @see ContextPriority
 */
public class ContextManagementAction extends KeyedProcessFunction<String, Event, Event> {

  private static final Logger LOG = LoggerFactory.getLogger(ContextManagementAction.class);

  private final String agentId;
  private final int maxTokens;
  private final int maxItems;
  private final double compactionThreshold;
  private final double relevancyThreshold;
  private final double longTermPromotionThreshold;

  // Flink State - Fault Tolerant!
  private transient ValueState<AgentContext> contextState;
  private transient ListState<ContextItem> shortTermMemoryState;
  private transient MapState<String, ContextItem> longTermMemoryState;

  // Relevancy scorer for compaction
  private transient RelevancyScorer relevancyScorer;

  /**
   * Creates context management action with default configuration.
   *
   * @param agentId Unique identifier for this agent
   */
  public ContextManagementAction(String agentId) {
    this(agentId, 4000, 50, 0.8, 0.5, 0.7);
  }

  /**
   * Creates context management action with custom configuration.
   *
   * @param agentId Unique identifier for this agent
   * @param maxTokens Maximum tokens in context window
   * @param maxItems Maximum items in context
   * @param compactionThreshold Trigger compaction at this usage ratio (0.8 = 80%)
   * @param relevancyThreshold Minimum relevancy score to keep items
   * @param longTermPromotionThreshold Minimum score to promote to long-term
   */
  public ContextManagementAction(
      String agentId,
      int maxTokens,
      int maxItems,
      double compactionThreshold,
      double relevancyThreshold,
      double longTermPromotionThreshold) {
    this.agentId = agentId;
    this.maxTokens = maxTokens;
    this.maxItems = maxItems;
    this.compactionThreshold = compactionThreshold;
    this.relevancyThreshold = relevancyThreshold;
    this.longTermPromotionThreshold = longTermPromotionThreshold;
  }

  /**
   * Initialize Flink state when action is opened.
   *
   * <p>This method is called by Flink when the action is initialized. It sets up the state
   * descriptors for fault-tolerant context storage.
   *
   * @param parameters Configuration parameters
   * @throws Exception if state initialization fails
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // Initialize context state (core metadata)
    ValueStateDescriptor<AgentContext> contextDescriptor =
        new ValueStateDescriptor<>("agentContext", AgentContext.class);
    contextState = getRuntimeContext().getState(contextDescriptor);

    // Initialize short-term memory state (working memory)
    ListStateDescriptor<ContextItem> shortTermDescriptor =
        new ListStateDescriptor<>("shortTermMemory", ContextItem.class);
    shortTermMemoryState = getRuntimeContext().getListState(shortTermDescriptor);

    // Initialize long-term memory state (persistent facts)
    MapStateDescriptor<String, ContextItem> longTermDescriptor =
        new MapStateDescriptor<>("longTermMemory", String.class, ContextItem.class);
    longTermMemoryState = getRuntimeContext().getMapState(longTermDescriptor);

    // Initialize relevancy scorer
    Map<String, String> scorerConfig = new HashMap<>();
    scorerConfig.put("baseUrl", "http://localhost:11434");
    scorerConfig.put("modelName", "nomic-embed-text:latest");
    this.relevancyScorer = new RelevancyScorer(scorerConfig);

    LOG.info(
        "ContextManagementAction initialized for agent {}: maxTokens={}, maxItems={}, "
            + "compactionThreshold={}, relevancyThreshold={}, longTermPromotionThreshold={}",
        agentId,
        maxTokens,
        maxItems,
        compactionThreshold,
        relevancyThreshold,
        longTermPromotionThreshold);
  }

  /**
   * Processes incoming Flink Agents events.
   *
   * <p>Adds the message to context and checks if compaction is needed.
   *
   * @param event Flink Agents event
   * @param ctx Process context for accessing state and emitting events
   * @param out Collector for output events
   * @throws Exception if context update fails
   */
  @Override
  public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
    // Get or create context from state
    AgentContext context = contextState.value();
    if (context == null) {
      context = createNewContext(event);
    }

    // Extract message content from event
    Object messageContent = event.getAttributes().get("message");
    if (messageContent == null) {
      // Not a message event, skip
      return;
    }

    // Create context item from message
    ContextItem item =
        new ContextItem(
            messageContent.toString(),
            ContextPriority.SHOULD, // Default priority
            MemoryType.SHORT_TERM);

    // Add to short-term memory state
    shortTermMemoryState.add(item);

    // Update context metadata
    context.updateLastAccess();
    int currentTokens = calculateTotalTokens();
    int currentItems = countItems();

    LOG.debug(
        "Added message to context for agent {}: tokens={}/{}, items={}/{}",
        agentId,
        currentTokens,
        maxTokens,
        currentItems,
        maxItems);

    // Check if compaction needed
    double usageRatio = (double) currentTokens / maxTokens;
    if (usageRatio >= compactionThreshold || currentItems >= maxItems) {
      LOG.info(
          "Context overflow detected for agent {}: usage={:.1f}%, triggering compaction",
          agentId,
          usageRatio * 100);

      // Emit context overflow event
      Event overflowEvent = createContextOverflowEvent(context, currentTokens, currentItems);
      out.collect(overflowEvent);

      // Immediately perform compaction
      performCompaction(event, ctx, out);
    }

    // Update state
    contextState.update(context);

    // Pass through the event
    out.collect(event);
  }

  /**
   * Performs 5-phase MoSCoW compaction to reduce context size.
   *
   * @param event Original event
   * @param ctx Process context
   * @param out Collector for output events
   * @throws Exception if compaction fails
   */
  private void performCompaction(Event event, Context ctx, Collector<Event> out) throws Exception {
    long startTime = System.currentTimeMillis();
    AgentContext context = contextState.value();
    String currentIntent = (String) event.getAttr("currentIntent");

    LOG.info("Starting 5-phase MoSCoW compaction for agent {}", agentId);

    int originalTokens = calculateTotalTokens();
    int originalItems = countItems();

    // Get all short-term items
    List<ContextItem> items = new ArrayList<>();
    for (ContextItem item : shortTermMemoryState.get()) {
      items.add(item);
    }

    // Phase 1: Remove WONT items immediately
    List<ContextItem> wontItems =
        items.stream()
            .filter(item -> item.getPriority() == ContextPriority.WONT)
            .collect(Collectors.toList());

    int removedCount = wontItems.size();
    items.removeAll(wontItems);
    LOG.debug("Phase 1: Removed {} WONT items", removedCount);

    // Phase 2: Score remaining items for relevancy
    Map<String, Double> relevancyScores = new HashMap<>();
    for (ContextItem item : items) {
      double score = relevancyScorer.scoreRelevancy(item, currentIntent).get();
      relevancyScores.put(item.getItemId(), score);
    }
    LOG.debug("Phase 2: Scored {} items for relevancy", items.size());

    // Phase 3: Remove low-relevancy COULD items
    List<ContextItem> couldItems =
        items.stream()
            .filter(item -> item.getPriority() == ContextPriority.COULD)
            .collect(Collectors.toList());

    List<ContextItem> lowRelevancyCould = new ArrayList<>();
    for (ContextItem item : couldItems) {
      double score = relevancyScores.getOrDefault(item.getItemId(), 0.0);
      if (score < relevancyThreshold) {
        lowRelevancyCould.add(item);
      }
    }

    items.removeAll(lowRelevancyCould);
    removedCount += lowRelevancyCould.size();
    LOG.debug("Phase 3: Removed {} low-relevancy COULD items", lowRelevancyCould.size());

    // Phase 4: Compress SHOULD items if still over limit
    int compressedCount = 0;
    if (calculateTokensForItems(items) > maxTokens * compactionThreshold) {
      List<ContextItem> shouldItems =
          items.stream()
              .filter(item -> item.getPriority() == ContextPriority.SHOULD)
              .sorted(
                  (a, b) ->
                      Double.compare(
                          relevancyScores.getOrDefault(b.getItemId(), 0.0),
                          relevancyScores.getOrDefault(a.getItemId(), 0.0)))
              .collect(Collectors.toList());

      int toRemove = shouldItems.size() / 2;
      List<ContextItem> toCompact =
          shouldItems.subList(Math.max(0, shouldItems.size() - toRemove), shouldItems.size());

      items.removeAll(toCompact);
      compressedCount = toCompact.size();
    }
    LOG.debug("Phase 4: Compressed {} SHOULD items", compressedCount);

    // Phase 5: Identify items for long-term promotion
    List<ContextItem> promotedItems = new ArrayList<>();
    for (ContextItem item : items) {
      double score = relevancyScores.getOrDefault(item.getItemId(), 0.0);
      if (score >= longTermPromotionThreshold && item.getPriority() == ContextPriority.MUST) {
        promotedItems.add(item);
        // Add to long-term state
        longTermMemoryState.put(item.getItemId(), item);
      }
    }
    LOG.debug("Phase 5: Promoted {} items to long-term memory", promotedItems.size());

    // Update short-term memory state with compacted items
    shortTermMemoryState.clear();
    for (ContextItem item : items) {
      shortTermMemoryState.add(item);
    }

    int compactedTokens = calculateTotalTokens();
    int compactedItems = countItems();
    long compactionTimeMs = System.currentTimeMillis() - startTime;

    LOG.info(
        "Compaction complete for agent {}: {}→{} tokens ({} saved), {}→{} items, "
            + "removed={}, compressed={}, promoted={}, time={}ms",
        agentId,
        originalTokens,
        compactedTokens,
        originalTokens - compactedTokens,
        originalItems,
        compactedItems,
        removedCount,
        compressedCount,
        promotedItems.size(),
        compactionTimeMs);

    // Emit compaction result event
    Event resultEvent =
        createCompactionResultEvent(
            context,
            originalTokens,
            compactedTokens,
            removedCount,
            compressedCount,
            promotedItems,
            compactionTimeMs);
    out.collect(resultEvent);

    // Update context state
    context.updateLastAccess();
    contextState.update(context);
  }

  /** Creates new agent context when none exists. */
  private AgentContext createNewContext(Event event) {
    String flowId = (String) event.getAttr("flowId");
    String userId = (String) event.getAttr("userId");
    return new AgentContext(agentId, flowId != null ? flowId : "unknown", userId, maxTokens, maxItems);
  }

  /** Calculates total tokens across all context items in state. */
  private int calculateTotalTokens() throws Exception {
    int total = 0;
    for (ContextItem item : shortTermMemoryState.get()) {
      total += item.getTokenCount();
    }
    return total;
  }

  /** Counts total items in short-term memory state. */
  private int countItems() throws Exception {
    int count = 0;
    for (ContextItem item : shortTermMemoryState.get()) {
      count++;
    }
    return count;
  }

  /** Calculates tokens for a list of items. */
  private int calculateTokensForItems(List<ContextItem> items) {
    return items.stream().mapToInt(ContextItem::getTokenCount).sum();
  }

  /** Creates context overflow event. */
  private Event createContextOverflowEvent(AgentContext context, int tokens, int items) {
    Map<String, Object> data = new HashMap<>();
    data.put("eventType", "ContextOverflow");
    data.put("currentTokens", tokens);
    data.put("maxTokens", maxTokens);
    data.put("currentItems", items);
    data.put("maxItems", maxItems);
    data.put("currentIntent", context.getCurrentIntent());

    OutputEvent event = new OutputEvent(data);
    event.setAttr("agentId", agentId);
    event.setAttr("flowId", context.getFlowId());
    event.setSourceTimestamp(System.currentTimeMillis());
    return event;
  }

  /** Creates compaction result event. */
  private Event createCompactionResultEvent(
      AgentContext context,
      int originalTokens,
      int compactedTokens,
      int removedCount,
      int compressedCount,
      List<ContextItem> promotedItems,
      long compactionTimeMs) {
    Map<String, Object> data = new HashMap<>();
    data.put("eventType", "ContextCompacted");
    data.put("originalTokens", originalTokens);
    data.put("compactedTokens", compactedTokens);
    data.put("tokensSaved", originalTokens - compactedTokens);
    data.put("removedCount", removedCount);
    data.put("compressedCount", compressedCount);
    data.put("promotedCount", promotedItems.size());
    data.put("compactionTimeMs", compactionTimeMs);

    OutputEvent event = new OutputEvent(data);
    event.setAttr("agentId", agentId);
    event.setAttr("flowId", context.getFlowId());
    event.setSourceTimestamp(System.currentTimeMillis());
    return event;
  }

  public String getAgentId() {
    return agentId;
  }
}

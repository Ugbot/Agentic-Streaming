package org.agentic.flink.job;

import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.ShortTermMemoryStore;
import org.agentic.flink.storage.StorageFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async sink function that persists {@link AgentEvent}s to tiered storage as a side-effect,
 * passing every event through unchanged.
 *
 * <p>Storage strategy:
 * <ul>
 *   <li><b>All events</b> are stored in the short-term (HOT) store keyed by flowId.
 *       This supports active conversation context and recent event lookups.</li>
 *   <li><b>Terminal events</b> ({@code FLOW_COMPLETED}, {@code FLOW_FAILED}) are additionally
 *       stored in the long-term (WARM) store for post-mortem analysis and conversation
 *       resumption.</li>
 * </ul>
 *
 * <p>Backend selection is driven by the presence of configuration keys:
 * <ul>
 *   <li>If {@code redis.host} is explicitly configured, Redis is used for the short-term store;
 *       otherwise an in-memory store is created.</li>
 *   <li>If {@code postgres.url} is explicitly configured, PostgreSQL is used for the long-term
 *       store; otherwise an in-memory store is created.</li>
 * </ul>
 *
 * <p>If no {@link AgenticFlinkConfig} is provided (null), both stores default to in-memory
 * implementations suitable for testing and development.
 *
 * <p>On timeout the event is passed through without loss -- storage persistence is best-effort
 * and must never block or discard pipeline data.
 *
 * @author Agentic Flink Team
 * @see StorageFactory
 * @see ShortTermMemoryStore
 * @see LongTermMemoryStore
 */
public class StorageSinkFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(StorageSinkFunction.class);

  private final AgenticFlinkConfig config;

  private transient ShortTermMemoryStore shortTermStore;
  private transient LongTermMemoryStore longTermStore;

  /**
   * Creates a StorageSinkFunction.
   *
   * @param config Agentic Flink configuration, or {@code null} for in-memory fallback
   */
  public StorageSinkFunction(AgenticFlinkConfig config) {
    this.config = config;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    Map<String, String> configMap;
    if (config != null) {
      configMap = config.toMap();
    } else {
      configMap = new HashMap<>();
    }

    // Short-term storage in this sink is now legacy: only the in-memory backend is supported,
    // because Flink keyed state is the canonical short-term memory for the agent operators.
    // This sink is kept for archival/observability flows that want a non-keyed view.
    String shortTermBackend = "memory";

    // Select long-term backend based on whether PostgreSQL is explicitly configured
    String longTermBackend = "memory";
    if (config != null) {
      String postgresUrl = config.get(ConfigKeys.POSTGRES_URL);
      if (postgresUrl != null && !postgresUrl.equals(ConfigKeys.DEFAULT_POSTGRES_URL)) {
        longTermBackend = "postgresql";
      }
    }

    LOG.info(
        "Initializing StorageSinkFunction: shortTerm={}, longTerm={}",
        shortTermBackend,
        longTermBackend);

    this.shortTermStore = StorageFactory.createShortTermStore(shortTermBackend, configMap);
    this.longTermStore = StorageFactory.createLongTermStore(longTermBackend, configMap);

    LOG.info("StorageSinkFunction initialized successfully");
  }

  @Override
  public void asyncInvoke(AgentEvent event, ResultFuture<AgentEvent> resultFuture) {
    String flowId = event.getFlowId();
    AgentEventType eventType = event.getEventType();

    LOG.debug(
        "Persisting event: flowId={}, type={}, agent={}",
        flowId,
        eventType,
        event.getAgentId());

    try {
      // Store every event in the short-term (HOT) store keyed by flowId
      ContextItem item = buildContextItem(event);
      shortTermStore.addItem(flowId, item);

      // Terminal events also go to the long-term (WARM) store
      if (eventType == AgentEventType.FLOW_COMPLETED || eventType == AgentEventType.FLOW_FAILED) {
        LOG.debug("Terminal event -- persisting to long-term store: flowId={}, type={}", flowId, eventType);
        longTermStore.addFact(
            flowId,
            "terminal_" + eventType.name().toLowerCase() + "_" + event.getTimestamp(),
            item);
      }
    } catch (Exception e) {
      LOG.warn(
          "Storage persistence failed for flowId={}, type={}: {}. Event will still pass through.",
          flowId,
          eventType,
          e.getMessage(),
          e);
    }

    // Always pass the event through regardless of storage success/failure
    resultFuture.complete(Collections.singletonList(event));
  }

  @Override
  public void timeout(AgentEvent event, ResultFuture<AgentEvent> resultFuture) {
    LOG.warn(
        "Storage operation timed out for flowId={}, type={}. Passing event through.",
        event.getFlowId(),
        event.getEventType());

    // Never lose data on timeout -- pass the event through unchanged
    resultFuture.complete(Collections.singletonList(event));
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing StorageSinkFunction");

    if (shortTermStore != null) {
      try {
        shortTermStore.close();
      } catch (Exception e) {
        LOG.warn("Error closing short-term store: {}", e.getMessage(), e);
      }
    }

    if (longTermStore != null) {
      try {
        longTermStore.close();
      } catch (Exception e) {
        LOG.warn("Error closing long-term store: {}", e.getMessage(), e);
      }
    }

    super.close();
  }

  /**
   * Builds a {@link ContextItem} from an {@link AgentEvent} for storage persistence.
   *
   * <p>Terminal events (FLOW_COMPLETED, FLOW_FAILED) are tagged as MUST priority since they
   * represent final state. All other events are tagged as SHOULD priority.
   */
  private ContextItem buildContextItem(AgentEvent event) {
    AgentEventType eventType = event.getEventType();

    boolean isTerminal =
        eventType == AgentEventType.FLOW_COMPLETED || eventType == AgentEventType.FLOW_FAILED;

    ContextPriority priority = isTerminal ? ContextPriority.MUST : ContextPriority.SHOULD;
    MemoryType memoryType = isTerminal ? MemoryType.LONG_TERM : MemoryType.SHORT_TERM;

    String content = buildEventSummary(event);
    ContextItem item = new ContextItem(content, priority, memoryType);

    item.addMetadata("event_type", eventType.name());
    item.addMetadata("agent_id", event.getAgentId() != null ? event.getAgentId() : "unknown");
    item.addMetadata("flow_id", event.getFlowId() != null ? event.getFlowId() : "unknown");

    if (event.getUserId() != null) {
      item.addMetadata("user_id", event.getUserId());
    }
    if (event.getCurrentStage() != null) {
      item.addMetadata("current_stage", event.getCurrentStage());
    }
    if (event.getErrorMessage() != null) {
      item.addMetadata("error_message", event.getErrorMessage());
    }
    if (event.getCorrelationId() != null) {
      item.addMetadata("correlation_id", event.getCorrelationId());
    }

    return item;
  }

  /**
   * Builds a human-readable summary string for an event, used as the ContextItem content.
   */
  private String buildEventSummary(AgentEvent event) {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(event.getEventType().name()).append("] ");
    sb.append("flow=").append(event.getFlowId());

    if (event.getAgentId() != null) {
      sb.append(", agent=").append(event.getAgentId());
    }
    if (event.getCurrentStage() != null) {
      sb.append(", stage=").append(event.getCurrentStage());
    }
    if (event.getIterationNumber() != null && event.getIterationNumber() > 0) {
      sb.append(", iteration=").append(event.getIterationNumber());
    }
    if (event.getErrorMessage() != null) {
      sb.append(", error=").append(event.getErrorMessage());
    }

    return sb.toString();
  }
}

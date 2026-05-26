package org.agentic.flink.stream;

import org.agentic.flink.compensation.CompensationAction;
import org.agentic.flink.compensation.CompensationHandler;
import org.agentic.flink.compensation.CompensationResult;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.tool.ToolRegistry;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async Flink function for handling compensation (saga rollback).
 *
 * <p>When an agent flow fails, this function extracts compensation actions
 * from the event history and executes them in reverse order.
 *
 * <p><b>Usage in a stream:</b>
 * <pre>{@code
 * DataStream<AgentEvent> compensatedEvents = failedEvents
 *     .keyBy(AgentEvent::getFlowId)
 *     .flatMap(new CompensationFunction(toolRegistry));
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class CompensationFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CompensationFunction.class);

  private final ToolRegistry toolRegistry;
  private transient CompensationHandler compensationHandler;

  public CompensationFunction(ToolRegistry toolRegistry) {
    this.toolRegistry = toolRegistry;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    LOG.info("Initializing CompensationHandler");
    compensationHandler = new CompensationHandler(toolRegistry);
    LOG.info("CompensationHandler initialized");
  }

  @Override
  public void asyncInvoke(AgentEvent failedEvent, ResultFuture<AgentEvent> resultFuture) {
    LOG.info("Processing compensation for flow: {}", failedEvent.getFlowId());

    // Check if this is a failed event that needs compensation
    if (failedEvent.getEventType() != AgentEventType.FLOW_FAILED) {
      LOG.warn("Event is not a FLOW_FAILED event, skipping compensation: {}",
          failedEvent.getEventType());
      resultFuture.complete(java.util.Collections.singleton(failedEvent));
      return;
    }

    // Extract compensation actions from the event's compensation data
    List<CompensationAction> actions = extractCompensationActions(failedEvent);

    if (actions.isEmpty()) {
      LOG.info("No compensation actions found for flow: {}", failedEvent.getFlowId());
      resultFuture.complete(java.util.Collections.singleton(failedEvent));
      return;
    }

    LOG.info("Found {} compensation actions for flow: {}", actions.size(), failedEvent.getFlowId());

    // Execute compensation
    CompletableFuture<CompensationResult> future = compensationHandler.compensate(failedEvent, actions);

    // Handle result
    future.whenComplete((result, error) -> {
      if (error != null) {
        LOG.error("Compensation execution error for flow: {}", failedEvent.getFlowId(), error);

        // Create error event
        AgentEvent errorEvent = failedEvent.withEventType(AgentEventType.COMPENSATION_FAILED);
        errorEvent.setErrorMessage("Compensation execution error: " + error.getMessage());
        resultFuture.complete(java.util.Collections.singleton(errorEvent));

      } else {
        LOG.info("Compensation completed for flow: {} - success: {}",
            failedEvent.getFlowId(), result.isSuccess());

        // Create compensation event
        AgentEvent compensationEvent = compensationHandler.createCompensationEvent(failedEvent, result);
        resultFuture.complete(java.util.Collections.singleton(compensationEvent));
      }
    });
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.error("Compensation timed out for flow: {}", input.getFlowId());

    AgentEvent timeoutEvent = input.withEventType(AgentEventType.COMPENSATION_FAILED);
    timeoutEvent.setErrorMessage("Compensation timed out");
    resultFuture.complete(java.util.Collections.singleton(timeoutEvent));
  }

  /**
   * Extracts compensation actions from the failed event.
   *
   * <p>This method looks for compensation data in the event's metadata.
   * The event should contain a list of compensation actions that were
   * accumulated during the flow execution.
   */
  private List<CompensationAction> extractCompensationActions(AgentEvent failedEvent) {
    // Check if event has compensation data directly
    if (failedEvent.getCompensationData() != null && !failedEvent.getCompensationData().isEmpty()) {
      // Single action case
      return java.util.Collections.singletonList(
          parseCompensationAction(failedEvent.getCompensationData()));
    }

    // Check metadata for compensation actions list
    if (failedEvent.getMetadata() != null) {
      Object actionsObj = failedEvent.getMetadata().get("compensation_actions");
      if (actionsObj instanceof List) {
        @SuppressWarnings("unchecked")
        List<java.util.Map<String, Object>> actionsList =
            (List<java.util.Map<String, Object>>) actionsObj;

        return actionsList.stream()
            .map(this::parseCompensationAction)
            .collect(java.util.stream.Collectors.toList());
      }
    }

    return java.util.Collections.emptyList();
  }

  /**
   * Parses a single compensation action from a map.
   */
  private CompensationAction parseCompensationAction(java.util.Map<String, Object> data) {
    String actionName = (String) data.get("action_name");
    String toolName = (String) data.get("tool_name");

    @SuppressWarnings("unchecked")
    java.util.Map<String, Object> parameters =
        (java.util.Map<String, Object>) data.get("parameters");

    CompensationAction action = new CompensationAction();
    action.setActionName(actionName != null ? actionName : toolName);
    action.setToolName(toolName);
    action.setParameters(parameters);

    return action;
  }
}

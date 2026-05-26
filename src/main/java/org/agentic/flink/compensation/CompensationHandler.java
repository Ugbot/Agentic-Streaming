package org.agentic.flink.compensation;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles compensation (rollback) for failed agent operations.
 *
 * <p>Implements saga-style compensation pattern where each successful operation
 * can be undone if a later operation fails.
 *
 * <p><b>Compensation Flow:</b>
 * <pre>
 * 1. Operation succeeds → Store compensation data
 * 2. Later operation fails → Trigger compensation
 * 3. Execute compensation actions in reverse order
 * 4. Emit compensation events
 * </pre>
 *
 * <p><b>Example:</b>
 * <pre>
 * Operation: database-insert(user_id=123)
 * Compensation: database-delete(user_id=123)
 *
 * If workflow fails after insert, compensation automatically deletes the user.
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class CompensationHandler implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CompensationHandler.class);

  private final ToolRegistry toolRegistry;

  public CompensationHandler(ToolRegistry toolRegistry) {
    this.toolRegistry = toolRegistry;
  }

  /**
   * Compensates a failed flow by executing compensation actions.
   *
   * @param failedEvent The event that triggered compensation
   * @param compensationActions List of compensation actions to execute (in reverse order)
   * @return CompletableFuture with compensation result
   */
  public CompletableFuture<CompensationResult> compensate(
      AgentEvent failedEvent,
      List<CompensationAction> compensationActions) {

    LOG.info("Starting compensation for flow: {} with {} actions",
        failedEvent.getFlowId(), compensationActions.size());

    return CompletableFuture.supplyAsync(() -> {
      CompensationResult result = new CompensationResult();
      result.setFlowId(failedEvent.getFlowId());
      result.setTriggerEvent(failedEvent);

      // Execute compensation actions in reverse order (LIFO)
      List<CompensationAction> reversedActions = new ArrayList<>(compensationActions);
      Collections.reverse(reversedActions);

      List<CompensationActionResult> actionResults = new ArrayList<>();
      int successCount = 0;
      int failureCount = 0;

      for (CompensationAction action : reversedActions) {
        LOG.info("Executing compensation action: {}", action.getActionName());

        try {
          CompensationActionResult actionResult = executeCompensationAction(action);
          actionResults.add(actionResult);

          if (actionResult.isSuccess()) {
            successCount++;
            LOG.info("Compensation action succeeded: {}", action.getActionName());
          } else {
            failureCount++;
            LOG.warn("Compensation action failed: {} - {}",
                action.getActionName(), actionResult.getErrorMessage());
          }

        } catch (Exception e) {
          LOG.error("Compensation action threw exception: {}", action.getActionName(), e);
          failureCount++;

          CompensationActionResult errorResult = new CompensationActionResult();
          errorResult.setActionName(action.getActionName());
          errorResult.setSuccess(false);
          errorResult.setErrorMessage(e.getMessage());
          actionResults.add(errorResult);
        }
      }

      result.setActionResults(actionResults);
      result.setSuccessCount(successCount);
      result.setFailureCount(failureCount);
      result.setSuccess(failureCount == 0);

      LOG.info("Compensation completed for flow: {} - success: {}/{}, failed: {}",
          failedEvent.getFlowId(), successCount, compensationActions.size(), failureCount);

      return result;
    });
  }

  /**
   * Executes a single compensation action.
   */
  private CompensationActionResult executeCompensationAction(CompensationAction action) {
    CompensationActionResult result = new CompensationActionResult();
    result.setActionName(action.getActionName());

    try {
      String toolName = action.getToolName();
      Map<String, Object> parameters = action.getParameters();

      // Check if tool exists
      if (!toolRegistry.hasTool(toolName)) {
        result.setSuccess(false);
        result.setErrorMessage("Compensation tool not found: " + toolName);
        return result;
      }

      // Get executor
      ToolExecutor executor = toolRegistry.getExecutor(toolName).orElse(null);
      if (executor == null) {
        result.setSuccess(false);
        result.setErrorMessage("No executor for compensation tool: " + toolName);
        return result;
      }

      // Execute compensation tool
      Object toolResult = executor.execute(parameters).get();

      result.setSuccess(true);
      result.setResult(toolResult);
      return result;

    } catch (Exception e) {
      LOG.error("Error executing compensation action: {}", action.getActionName(), e);
      result.setSuccess(false);
      result.setErrorMessage(e.getMessage());
      return result;
    }
  }

  /**
   * Extracts compensation actions from event history.
   *
   * @param events List of events in the flow
   * @return List of compensation actions
   */
  public List<CompensationAction> extractCompensationActions(List<AgentEvent> events) {
    List<CompensationAction> actions = new ArrayList<>();

    for (AgentEvent event : events) {
      if (event.getCompensationData() != null && !event.getCompensationData().isEmpty()) {
        CompensationAction action = parseCompensationData(event);
        if (action != null) {
          actions.add(action);
          LOG.debug("Extracted compensation action: {} from event: {}",
              action.getActionName(), event.getEventType());
        }
      }
    }

    return actions;
  }

  /**
   * Parses compensation data from an event.
   */
  private CompensationAction parseCompensationData(AgentEvent event) {
    Map<String, Object> compensationData = event.getCompensationData();

    String actionName = (String) compensationData.get("action_name");
    String toolName = (String) compensationData.get("tool_name");

    @SuppressWarnings("unchecked")
    Map<String, Object> parameters = (Map<String, Object>) compensationData.get("parameters");

    if (toolName == null) {
      LOG.warn("Compensation data missing tool_name for event: {}", event.getEventType());
      return null;
    }

    CompensationAction action = new CompensationAction();
    action.setActionName(actionName != null ? actionName : toolName);
    action.setToolName(toolName);
    action.setParameters(parameters);
    action.setOriginalEvent(event);

    return action;
  }

  /**
   * Creates a compensation event for emission.
   */
  public AgentEvent createCompensationEvent(
      AgentEvent originalEvent,
      CompensationResult result) {

    AgentEvent compensationEvent = originalEvent.withEventType(
        result.isSuccess()
            ? AgentEventType.FLOW_COMPENSATED
            : AgentEventType.COMPENSATION_FAILED);

    compensationEvent.putData("compensation_result", result);
    compensationEvent.putData("success_count", result.getSuccessCount());
    compensationEvent.putData("failure_count", result.getFailureCount());
    compensationEvent.putData("total_actions", result.getActionResults().size());

    if (!result.isSuccess()) {
      List<String> failedActions = new ArrayList<>();
      for (CompensationActionResult actionResult : result.getActionResults()) {
        if (!actionResult.isSuccess()) {
          failedActions.add(actionResult.getActionName());
        }
      }
      compensationEvent.setErrorMessage("Compensation failed for actions: " + failedActions);
    }

    return compensationEvent;
  }
}

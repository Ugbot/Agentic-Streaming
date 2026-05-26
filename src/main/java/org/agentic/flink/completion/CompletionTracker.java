package org.agentic.flink.completion;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;

/**
 * Tracks completion of agent workflows using event aggregation and task lists.
 *
 * <p>This class implements the "tell me when N things are done" pattern from the Saga kit. It
 * monitors a stream of AgentEvents and determines when all required tasks/goals have been
 * completed.
 *
 * <p><b>Saga Kit Inspiration:</b> Similar to how the saga kit's SagaPatternProcessFunction
 * aggregates TradeExecuted events and checks if totalExecutedQuantity >= requestedQuantity, the
 * CompletionTracker aggregates agent events and checks if all required tasks are complete.
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * // Define tasks for a research workflow
 * TaskList researchTasks = TaskList.builder()
 *     .addTask("web-search", "Search for relevant sources", true)
 *     .addTask("document-analysis", "Analyze retrieved documents", true)
 *     .addTask("synthesis", "Synthesize findings", true)
 *     .addTask("peer-review", "Get peer feedback", false)  // Optional
 *     .build();
 *
 * CompletionTracker tracker = new CompletionTracker("research-flow-001", researchTasks);
 *
 * // Process events
 * for (AgentEvent event : eventStream) {
 *     tracker.processEvent(event);
 *
 *     if (tracker.isComplete()) {
 *         System.out.println("Research complete!");
 *         break;
 *     }
 * }
 * }</pre>
 *
 * <p><b>Integration with Flink:</b>
 * <pre>{@code
 * // Use in a ProcessFunction
 * public class CompletionProcessFunction extends ProcessFunction<AgentEvent, AgentEvent> {
 *     private ValueState<CompletionTracker> trackerState;
 *
 *     @Override
 *     public void processElement(AgentEvent event, Context ctx, Collector<AgentEvent> out) {
 *         CompletionTracker tracker = trackerState.value();
 *         tracker.processEvent(event);
 *
 *         if (tracker.isComplete()) {
 *             out.collect(createCompletionEvent(tracker));
 *         }
 *
 *         trackerState.update(tracker);
 *     }
 * }
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see TaskList
 * @see GoalPredicate
 */
public class CompletionTracker implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String flowId;
  private final TaskList taskList;
  private final List<AgentEvent> eventHistory;
  private final Map<String, Object> accumulatedState;
  private final Set<String> taskEventMappings; // Maps event types to task IDs
  private boolean isComplete;
  private long startTime;
  private long completionTime;

  /**
   * Creates a new completion tracker.
   *
   * @param flowId The flow ID to track
   * @param taskList The task list defining what needs to be completed
   */
  public CompletionTracker(String flowId, TaskList taskList) {
    this.flowId = flowId;
    this.taskList = taskList;
    this.eventHistory = new ArrayList<>();
    this.accumulatedState = new HashMap<>();
    this.taskEventMappings = new HashSet<>();
    this.isComplete = false;
    this.startTime = System.currentTimeMillis();
    this.completionTime = 0;
  }

  /**
   * Processes an incoming agent event.
   *
   * <p>This method:
   * <ul>
   *   <li>Adds the event to history</li>
   *   <li>Updates accumulated state</li>
   *   <li>Checks if any tasks completed based on the event</li>
   *   <li>Evaluates overall completion status</li>
   * </ul>
   *
   * @param event The agent event to process
   */
  public void processEvent(AgentEvent event) {
    // Add to history
    eventHistory.add(event);

    // Update accumulated state from event data
    if (event.getData() != null) {
      accumulatedState.putAll(event.getData());
    }

    // Check if this event completes any tasks
    checkTaskCompletion(event);

    // Check overall completion
    if (!isComplete && taskList.isComplete()) {
      this.isComplete = true;
      this.completionTime = System.currentTimeMillis();
    }
  }

  /**
   * Checks if a specific event completes any tasks.
   *
   * <p>Default mapping:
   * <ul>
   *   <li>TOOL_CALL_COMPLETED → task named after the tool</li>
   *   <li>VALIDATION_PASSED → task named "validation"</li>
   *   <li>SUPERVISOR_APPROVED → task named "supervisor-review"</li>
   * </ul>
   *
   * @param event The event to check
   */
  private void checkTaskCompletion(AgentEvent event) {
    AgentEventType eventType = event.getEventType();
    String taskId = null;

    // Map event types to task IDs
    switch (eventType) {
      case TOOL_CALL_COMPLETED:
        // Task ID = tool name
        taskId = (String) event.getData().get("tool_name");
        break;

      case VALIDATION_PASSED:
        taskId = "validation";
        break;

      case SUPERVISOR_APPROVED:
        taskId = "supervisor-review";
        break;

      case CORRECTION_COMPLETED:
        taskId = "correction";
        break;

      case LOOP_ITERATION_COMPLETED:
        taskId = "loop-iteration-" + event.getData().get("iteration");
        break;

      default:
        // Check if event data explicitly specifies a completed task
        taskId = (String) event.getData().get("completed_task_id");
        break;
    }

    if (taskId != null) {
      taskList.markCompleted(taskId);
      taskEventMappings.add(taskId);
    }
  }

  /**
   * Manually marks a task as completed.
   *
   * <p>Use this when task completion doesn't correspond to a single event.
   *
   * @param taskId The task ID to mark complete
   * @return true if the task was successfully marked
   */
  public boolean markTaskCompleted(String taskId) {
    boolean result = taskList.markCompleted(taskId);
    if (result && !isComplete && taskList.isComplete()) {
      this.isComplete = true;
      this.completionTime = System.currentTimeMillis();
    }
    return result;
  }

  /**
   * Manually marks a task as failed.
   *
   * @param taskId The task ID to mark failed
   * @return true if the task was successfully marked as failed
   */
  public boolean markTaskFailed(String taskId) {
    return taskList.markFailed(taskId);
  }

  /**
   * Checks if all required tasks are complete.
   *
   * @return true if complete
   */
  public boolean isComplete() {
    return isComplete;
  }

  /**
   * Gets the completion percentage (0.0 to 1.0).
   *
   * @return completion percentage
   */
  public double getCompletionPercentage() {
    return taskList.getCompletionPercentage();
  }

  /**
   * Gets the list of pending required tasks.
   *
   * @return set of pending task IDs
   */
  public Set<String> getPendingRequiredTasks() {
    return taskList.getPendingRequiredTasks();
  }

  /**
   * Gets the list of completed tasks.
   *
   * @return set of completed task IDs
   */
  public Set<String> getCompletedTasks() {
    return taskList.getCompletedTasks();
  }

  /**
   * Gets the accumulated state from all processed events.
   *
   * @return map of state variables
   */
  public Map<String, Object> getAccumulatedState() {
    return Collections.unmodifiableMap(accumulatedState);
  }

  /**
   * Gets the full event history.
   *
   * @return list of all processed events
   */
  public List<AgentEvent> getEventHistory() {
    return Collections.unmodifiableList(eventHistory);
  }

  /**
   * Gets events matching a specific predicate.
   *
   * @param predicate The condition to match
   * @return list of matching events
   */
  public List<AgentEvent> getMatchingEvents(Predicate<AgentEvent> predicate) {
    List<AgentEvent> matching = new ArrayList<>();
    for (AgentEvent event : eventHistory) {
      if (predicate.test(event)) {
        matching.add(event);
      }
    }
    return matching;
  }

  /**
   * Gets the duration of the workflow (in milliseconds).
   *
   * @return duration in milliseconds (or time so far if not complete)
   */
  public long getDurationMs() {
    if (isComplete) {
      return completionTime - startTime;
    } else {
      return System.currentTimeMillis() - startTime;
    }
  }

  /**
   * Aggregates a numeric value across all events.
   *
   * <p>Example: Sum all "tokens_used" values from events.
   *
   * @param key The metadata key to aggregate
   * @return sum of all numeric values
   */
  public double aggregateSum(String key) {
    double sum = 0.0;
    for (AgentEvent event : eventHistory) {
      Object valueObj = event.getData().get(key);
      if (valueObj instanceof Number) {
        sum += ((Number) valueObj).doubleValue();
      }
    }
    return sum;
  }

  /**
   * Counts events matching a specific type.
   *
   * @param eventType The event type to count
   * @return count of matching events
   */
  public int countEvents(AgentEventType eventType) {
    int count = 0;
    for (AgentEvent event : eventHistory) {
      if (event.getEventType() == eventType) {
        count++;
      }
    }
    return count;
  }

  /**
   * Checks if a specific event type has occurred.
   *
   * @param eventType The event type to check
   * @return true if at least one event of this type occurred
   */
  public boolean hasEventOccurred(AgentEventType eventType) {
    return countEvents(eventType) > 0;
  }

  /**
   * Creates a summary map for logging/debugging.
   *
   * @return map with completion summary
   */
  public Map<String, Object> getSummary() {
    Map<String, Object> summary = new HashMap<>();
    summary.put("flow_id", flowId);
    summary.put("is_complete", isComplete);
    summary.put("completion_percentage", getCompletionPercentage());
    summary.put("pending_tasks", getPendingRequiredTasks());
    summary.put("completed_tasks", getCompletedTasks());
    summary.put("duration_ms", getDurationMs());
    summary.put("event_count", eventHistory.size());
    return summary;
  }

  @Override
  public String toString() {
    return String.format(
        "CompletionTracker[flow=%s, complete=%s, progress=%.1f%%, duration=%dms, events=%d]",
        flowId,
        isComplete,
        getCompletionPercentage() * 100,
        getDurationMs(),
        eventHistory.size());
  }

  // ==================== Future: Goal-Based Completion ====================

  /**
   * Checks if a goal predicate is satisfied against this tracker's accumulated state and event
   * history.
   *
   * @param goal The goal predicate to evaluate
   * @return true if the goal is satisfied
   */
  public boolean isGoalSatisfied(GoalPredicate goal) {
    return goal.isSatisfied(accumulatedState, eventHistory);
  }

  /**
   * Returns the confidence score (0.0 to 1.0) for a goal predicate against this tracker's
   * accumulated state and event history.
   *
   * @param goal The goal predicate to evaluate
   * @return confidence score between 0.0 and 1.0
   */
  public double getGoalConfidence(GoalPredicate goal) {
    return goal.getConfidence(accumulatedState, eventHistory);
  }
}

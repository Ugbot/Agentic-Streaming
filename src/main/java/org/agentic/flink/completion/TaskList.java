package org.agentic.flink.completion;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Tracks a list of tasks to be completed, inspired by the Saga kit's completion tracking.
 *
 * <p>This class implements the "tell me when N things are done" pattern. It maintains a list of
 * tasks (identified by task IDs) and tracks which ones have been completed. When all required tasks
 * are complete, it triggers a completion event.
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * TaskList tasks = TaskList.builder()
 *     .addTask("fetch-user-data", true)      // Required task
 *     .addTask("fetch-preferences", true)    // Required task
 *     .addTask("fetch-history", false)       // Optional task
 *     .build();
 *
 * tasks.markCompleted("fetch-user-data");
 * tasks.markCompleted("fetch-preferences");
 *
 * if (tasks.isComplete()) {
 *     System.out.println("All required tasks completed!");
 * }
 * }</pre>
 *
 * <p><b>Saga Kit Pattern:</b> Similar to how the saga kit aggregates TradeExecuted events to check
 * if total quantity >= requested quantity, TaskList aggregates task completions to check if all
 * required tasks are done.
 *
 * @author Agentic Flink Team
 * @see CompletionTracker
 */
public class TaskList implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String taskListId;
  private final Map<String, Task> tasks;
  private final Set<String> completedTasks;
  private final int totalTasks;
  private final int requiredTasks;
  private boolean isComplete;

  private TaskList(Builder builder) {
    this.taskListId = builder.taskListId;
    this.tasks = Collections.unmodifiableMap(builder.tasks);
    this.completedTasks = new HashSet<>();
    this.totalTasks = tasks.size();
    this.requiredTasks = (int) tasks.values().stream().filter(Task::isRequired).count();
    this.isComplete = false;
  }

  /**
   * Marks a task as completed.
   *
   * @param taskId The task ID to mark complete
   * @return true if the task was successfully marked (existed and wasn't already complete)
   */
  public boolean markCompleted(String taskId) {
    if (!tasks.containsKey(taskId)) {
      return false; // Unknown task
    }
    if (completedTasks.contains(taskId)) {
      return false; // Already completed
    }

    completedTasks.add(taskId);
    checkCompletion();
    return true;
  }

  /**
   * Marks a task as failed (removes from completion tracking).
   *
   * @param taskId The task ID to mark failed
   * @return true if the task was successfully marked as failed
   */
  public boolean markFailed(String taskId) {
    if (!tasks.containsKey(taskId)) {
      return false; // Unknown task
    }

    Task task = tasks.get(taskId);
    if (task.isRequired()) {
      // Required task failed - entire task list fails
      isComplete = false;
      return true;
    } else {
      // Optional task failed - just remove from tracking
      completedTasks.remove(taskId);
      return true;
    }
  }

  /**
   * Checks if all required tasks are complete.
   *
   * @return true if all required tasks are completed
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
    if (requiredTasks == 0) {
      return 1.0;
    }
    long completedRequired = completedTasks.stream()
        .filter(taskId -> tasks.get(taskId).isRequired())
        .count();
    return (double) completedRequired / requiredTasks;
  }

  /**
   * Gets the number of completed required tasks.
   *
   * @return count of completed required tasks
   */
  public int getCompletedRequiredCount() {
    return (int) completedTasks.stream()
        .filter(taskId -> tasks.get(taskId).isRequired())
        .count();
  }

  /**
   * Gets the list of pending (incomplete) required tasks.
   *
   * @return set of pending required task IDs
   */
  public Set<String> getPendingRequiredTasks() {
    return tasks.values().stream()
        .filter(Task::isRequired)
        .map(Task::getTaskId)
        .filter(taskId -> !completedTasks.contains(taskId))
        .collect(Collectors.toSet());
  }

  /**
   * Gets the list of all completed tasks.
   *
   * @return set of completed task IDs
   */
  public Set<String> getCompletedTasks() {
    return Collections.unmodifiableSet(completedTasks);
  }

  /**
   * Gets a task by ID.
   *
   * @param taskId The task ID
   * @return Optional containing the task, or empty if not found
   */
  public Optional<Task> getTask(String taskId) {
    return Optional.ofNullable(tasks.get(taskId));
  }

  /** Private method to check if all required tasks are complete. */
  private void checkCompletion() {
    long completedRequired = completedTasks.stream()
        .filter(taskId -> tasks.get(taskId).isRequired())
        .count();
    this.isComplete = (completedRequired == requiredTasks);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating TaskList instances. */
  public static class Builder {
    private String taskListId = UUID.randomUUID().toString();
    private Map<String, Task> tasks = new HashMap<>();

    public Builder withId(String taskListId) {
      this.taskListId = taskListId;
      return this;
    }

    /**
     * Adds a task to the task list.
     *
     * @param taskId Unique task identifier
     * @param required Whether this task must be completed
     * @return this builder
     */
    public Builder addTask(String taskId, boolean required) {
      tasks.put(taskId, new Task(taskId, null, required));
      return this;
    }

    /**
     * Adds a task with a description.
     *
     * @param taskId Unique task identifier
     * @param description Human-readable task description
     * @param required Whether this task must be completed
     * @return this builder
     */
    public Builder addTask(String taskId, String description, boolean required) {
      tasks.put(taskId, new Task(taskId, description, required));
      return this;
    }

    /**
     * Adds multiple required tasks.
     *
     * @param taskIds Task IDs to add (all required)
     * @return this builder
     */
    public Builder addRequiredTasks(String... taskIds) {
      for (String taskId : taskIds) {
        tasks.put(taskId, new Task(taskId, null, true));
      }
      return this;
    }

    /**
     * Adds multiple optional tasks.
     *
     * @param taskIds Task IDs to add (all optional)
     * @return this builder
     */
    public Builder addOptionalTasks(String... taskIds) {
      for (String taskId : taskIds) {
        tasks.put(taskId, new Task(taskId, null, false));
      }
      return this;
    }

    public TaskList build() {
      if (tasks.isEmpty()) {
        throw new IllegalStateException("TaskList must have at least one task");
      }
      return new TaskList(this);
    }
  }

  /** Represents an individual task within the task list. */
  public static class Task implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String taskId;
    private final String description;
    private final boolean required;

    public Task(String taskId, String description, boolean required) {
      this.taskId = taskId;
      this.description = description;
      this.required = required;
    }

    public String getTaskId() {
      return taskId;
    }

    public String getDescription() {
      return description != null ? description : taskId;
    }

    public boolean isRequired() {
      return required;
    }

    @Override
    public String toString() {
      return String.format(
          "Task[%s, %s, %s]", taskId, required ? "required" : "optional", description);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "TaskList[id=%s, total=%d, required=%d, completed=%d/%d, complete=%s]",
        taskListId,
        totalTasks,
        requiredTasks,
        getCompletedRequiredCount(),
        requiredTasks,
        isComplete);
  }
}

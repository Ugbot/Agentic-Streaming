package org.agentic.flink.tools;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry for tool executors Manages the mapping between tool IDs and their executor
 * implementations
 */
public class ToolExecutorRegistry implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ToolExecutorRegistry.class);

  private final Map<String, ToolExecutor> executors;

  public ToolExecutorRegistry() {
    this.executors = new HashMap<>();
  }

  /**
   * Register a tool executor
   *
   * @param executor The executor to register
   */
  public void register(ToolExecutor executor) {
    String toolId = executor.getToolId();
    if (executors.containsKey(toolId)) {
      LOG.warn("Overwriting existing executor for tool: {}", toolId);
    }
    executors.put(toolId, executor);
    LOG.info("Registered tool executor: {} - {}", toolId, executor.getDescription());
  }

  /**
   * Get executor for a tool ID
   *
   * @param toolId The tool ID
   * @return Optional containing the executor if found
   */
  public Optional<ToolExecutor> getExecutor(String toolId) {
    return Optional.ofNullable(executors.get(toolId));
  }

  /**
   * Check if executor exists for tool
   *
   * @param toolId The tool ID
   * @return true if executor exists
   */
  public boolean hasExecutor(String toolId) {
    return executors.containsKey(toolId);
  }

  /**
   * Remove executor for tool
   *
   * @param toolId The tool ID
   */
  public void unregister(String toolId) {
    ToolExecutor removed = executors.remove(toolId);
    if (removed != null) {
      LOG.info("Unregistered tool executor: {}", toolId);
    }
  }

  /**
   * Get all registered tool IDs
   *
   * @return Map of tool IDs to executors
   */
  public Map<String, ToolExecutor> getAllExecutors() {
    return new HashMap<>(executors);
  }

  /**
   * Get count of registered executors
   *
   * @return Number of registered executors
   */
  public int size() {
    return executors.size();
  }

  /**
   * Clear all executors
   */
  public void clear() {
    LOG.info("Clearing all tool executors");
    executors.clear();
  }
}

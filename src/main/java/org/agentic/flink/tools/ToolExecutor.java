package org.agentic.flink.tools;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Base interface for tool executors. Each tool implementation should provide a concrete executor
 * that can be invoked asynchronously.
 */
public interface ToolExecutor extends Serializable {

  /**
   * Execute the tool with given parameters
   *
   * @param parameters Input parameters for the tool
   * @return CompletableFuture with the result
   */
  CompletableFuture<Object> execute(Map<String, Object> parameters);

  /**
   * Get the tool ID this executor handles
   *
   * @return Tool identifier
   */
  String getToolId();

  /**
   * Validate parameters before execution
   *
   * @param parameters Parameters to validate
   * @return true if valid, false otherwise
   */
  default boolean validateParameters(Map<String, Object> parameters) {
    return parameters != null;
  }

  /**
   * Get human-readable description of what this tool does
   *
   * @return Description
   */
  String getDescription();
}

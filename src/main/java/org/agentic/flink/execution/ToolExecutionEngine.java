package org.agentic.flink.execution;

import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async engine for executing tool calls.
 *
 * <p>Handles:
 * <ul>
 *   <li>Async tool execution with CompletableFuture</li>
 *   <li>Tool validation and error handling</li>
 *   <li>Execution time tracking</li>
 *   <li>Integration with ToolRegistry</li>
 * </ul>
 *
 * @author Agentic Flink Team
 */
public class ToolExecutionEngine implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ToolExecutionEngine.class);

  private final ToolRegistry toolRegistry;
  private final LLMClient llmClient;

  public ToolExecutionEngine(ToolRegistry toolRegistry, LLMClient llmClient) {
    this.toolRegistry = toolRegistry;
    this.llmClient = llmClient;
  }

  /**
   * Executes a tool call asynchronously.
   *
   * @param toolCall The tool call to execute
   * @param context Execution context
   * @return Future with tool result
   */
  public CompletableFuture<ToolCallResult> executeTool(ToolCall toolCall, ExecutionContext context) {
    LOG.info("Executing tool: {} for flow: {}", toolCall.getToolName(), context.getFlowId());

    long startTime = System.currentTimeMillis();

    // Check if tool exists
    if (!toolRegistry.hasTool(toolCall.getToolName())) {
      LOG.error("Tool not found: {}", toolCall.getToolName());
      return CompletableFuture.completedFuture(
          ToolCallResult.failure(
              toolCall.getToolCallId(),
              toolCall.getToolName(),
              "Tool not found in registry"));
    }

    // Get executor
    ToolExecutor executor = toolRegistry.getExecutor(toolCall.getToolName()).orElse(null);
    if (executor == null) {
      LOG.error("No executor found for tool: {}", toolCall.getToolName());
      return CompletableFuture.completedFuture(
          ToolCallResult.failure(
              toolCall.getToolCallId(),
              toolCall.getToolName(),
              "Tool has no executor implementation"));
    }

    // Validate parameters
    if (!executor.validateParameters(toolCall.getParameters())) {
      LOG.error("Invalid parameters for tool: {}", toolCall.getToolName());
      return CompletableFuture.completedFuture(
          ToolCallResult.failure(
              toolCall.getToolCallId(),
              toolCall.getToolName(),
              "Invalid tool parameters"));
    }

    // Execute tool
    return executor.execute(toolCall.getParameters())
        .handle((result, error) -> {
          long executionTime = System.currentTimeMillis() - startTime;

          if (error != null) {
            LOG.error("Tool execution failed: {}", toolCall.getToolName(), error);
            return ToolCallResult.failure(
                toolCall.getToolCallId(),
                toolCall.getToolName(),
                error.getMessage());
          }

          LOG.info("Tool {} completed in {}ms", toolCall.getToolName(), executionTime);

          ToolCallResult toolResult = ToolCallResult.success(
              toolCall.getToolCallId(),
              toolCall.getToolName(),
              result);
          toolResult.setExecutionTimeMs(executionTime);

          return toolResult;
        });
  }
}

package org.agentic.flink.function;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.serde.ToolCallRequest;
import org.agentic.flink.serde.ToolCallResponse;
import org.agentic.flink.tools.ToolExecutor;
import org.agentic.flink.tools.ToolExecutorRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enhanced Tool Call Async Function that uses actual tool executors Supports real tool
 * implementations instead of just LLM prompts
 */
public class ToolCallAsyncFunctionV2 extends RichAsyncFunction<ToolCallRequest, ToolCallResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(ToolCallAsyncFunctionV2.class);
  public static final String UID = "ToolCallAsyncFunctionV2";

  private final Map<String, ToolDefinition> toolRegistry;
  private final ToolExecutorRegistry executorRegistry;

  public ToolCallAsyncFunctionV2(
      Map<String, ToolDefinition> toolRegistry, ToolExecutorRegistry executorRegistry) {
    this.toolRegistry = toolRegistry;
    this.executorRegistry = executorRegistry;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    LOG.info("ToolCallAsyncFunctionV2 opened with {} executors", executorRegistry.size());
  }

  @Override
  public void asyncInvoke(
      ToolCallRequest request, ResultFuture<ToolCallResponse> resultFuture) {

    LOG.info(
        "Executing tool call for flow {}, tool: {}", request.getFlowId(), request.getToolId());

    // Get tool definition
    ToolDefinition toolDef = toolRegistry.get(request.getToolId());
    if (toolDef == null) {
      ToolCallResponse response = createErrorResponse(request, "Tool not found in registry", "TOOL_NOT_FOUND");
      resultFuture.complete(Collections.singleton(response));
      return;
    }

    // Get executor for this tool
    Optional<ToolExecutor> executorOpt = executorRegistry.getExecutor(request.getToolId());
    if (!executorOpt.isPresent()) {
      ToolCallResponse response = createErrorResponse(request, "No executor found for tool: " + request.getToolId(), "EXECUTOR_NOT_FOUND");
      resultFuture.complete(Collections.singleton(response));
      return;
    }

    ToolExecutor executor = executorOpt.get();

    // Validate parameters
    if (!executor.validateParameters(request.getParameters())) {
      ToolCallResponse response = createErrorResponse(request, "Invalid parameters for tool", "INVALID_PARAMETERS");
      resultFuture.complete(Collections.singleton(response));
      return;
    }

    // Execute tool
    CompletableFuture<Object> executionFuture = executor.execute(request.getParameters());

    // Process result
    executionFuture.whenComplete(
        (result, throwable) -> {
          ToolCallResponse response =
              new ToolCallResponse(
                  request.getRequestId(),
                  request.getFlowId(),
                  request.getUserId(),
                  request.getAgentId());
          response.setToolId(request.getToolId());
          response.setToolName(toolDef.getName());

          if (throwable != null) {
            LOG.error(
                "Tool execution failed for flow {}, tool: {}",
                request.getFlowId(),
                request.getToolId(),
                throwable);
            response.fail(throwable.getMessage(), "EXECUTION_ERROR");
          } else {
            response.complete(result);
            LOG.info(
                "Tool execution completed for flow {}, tool: {}",
                request.getFlowId(),
                request.getToolId());
          }

          resultFuture.complete(Collections.singleton(response));
        });
  }

  @Override
  public void timeout(ToolCallRequest input, ResultFuture<ToolCallResponse> resultFuture) {
    LOG.warn("Tool call timed out for flow {}, tool: {}", input.getFlowId(), input.getToolId());

    ToolCallResponse response = createErrorResponse(input, "Tool execution timed out", "TIMEOUT");
    resultFuture.complete(Collections.singleton(response));
  }

  private ToolCallResponse createErrorResponse(
      ToolCallRequest request, String errorMessage, String errorCode) {
    ToolCallResponse response =
        new ToolCallResponse(
            request.getRequestId(),
            request.getFlowId(),
            request.getUserId(),
            request.getAgentId());
    response.setToolId(request.getToolId());
    response.setToolName(request.getToolName());
    response.fail(errorMessage, errorCode);
    return response;
  }

  public static AgentEvent responseToEvent(ToolCallResponse response) {
    AgentEvent event = new AgentEvent();
    event.setFlowId(response.getFlowId());
    event.setUserId(response.getUserId());
    event.setAgentId(response.getAgentId());
    event.setEventType(
        response.isSuccess()
            ? AgentEventType.TOOL_CALL_COMPLETED
            : AgentEventType.TOOL_CALL_FAILED);
    event.setTimestamp(System.currentTimeMillis());
    event.putData("toolId", response.getToolId());
    event.putData("result", response.getResult());
    event.putData("success", response.isSuccess());
    if (!response.isSuccess()) {
      event.setErrorMessage(response.getErrorMessage());
      event.setErrorCode(response.getErrorCode());
    }
    return event;
  }
}

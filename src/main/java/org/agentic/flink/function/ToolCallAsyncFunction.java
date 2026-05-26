package org.agentic.flink.function;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.langchain.LangChainToolAdapter;
import org.agentic.flink.langchain.ToolAnnotationRegistry;
import org.agentic.flink.serde.ToolCallRequest;
import org.agentic.flink.serde.ToolCallResponse;
import org.agentic.flink.langchain.client.LangChainAsyncClient;
import org.agentic.flink.langchain.model.AiModel;
import org.agentic.flink.langchain.model.language.LangChainLanguageModel;
import org.agentic.flink.langchain.model.language.OllamaLanguageModel;
import org.agentic.flink.langchain.model.language.OpenAiLanguageModel;
import org.agentic.flink.langchain.config.LLMConfig;
import org.agentic.flink.tools.ToolExecutor;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.output.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToolCallAsyncFunction extends RichAsyncFunction<ToolCallRequest, ToolCallResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(ToolCallAsyncFunction.class);
  public static final String UID = ToolCallAsyncFunction.class.getSimpleName();

  private transient LangChainAsyncClient langChainAsyncClient;
  private final Map<String, ToolDefinition> toolRegistry;
  private final ToolAnnotationRegistry annotationRegistry;

  /**
   * Creates a ToolCallAsyncFunction with only a manual tool registry.
   *
   * @param toolRegistry Map of manually registered tools
   */
  public ToolCallAsyncFunction(Map<String, ToolDefinition> toolRegistry) {
    this(toolRegistry, null);
  }

  /**
   * Creates a ToolCallAsyncFunction with both manual and annotation-based registries.
   *
   * @param toolRegistry Map of manually registered tools
   * @param annotationRegistry Registry of @Tool annotated methods (can be null)
   */
  public ToolCallAsyncFunction(
      Map<String, ToolDefinition> toolRegistry, ToolAnnotationRegistry annotationRegistry) {
    this.toolRegistry = toolRegistry;
    this.annotationRegistry = annotationRegistry;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    this.langChainAsyncClient =
        new LangChainAsyncClient(
            List.of(
                LangChainLanguageModel.DEFAULT_MODEL,
                new OllamaLanguageModel(),
                new OpenAiLanguageModel()));
  }

  @Override
  public void asyncInvoke(
      ToolCallRequest request, ResultFuture<ToolCallResponse> resultFuture) {

    LOG.info(
        "Executing tool call for flow {}, tool: {}", request.getFlowId(), request.getToolId());

    // Try to get tool definition from annotation registry first, then manual registry
    ToolDefinition toolDef = findToolDefinition(request.getToolId());

    if (toolDef == null) {
      ToolCallResponse response = new ToolCallResponse();
      response.setRequestId(request.getRequestId());
      response.setFlowId(request.getFlowId());
      response.setUserId(request.getUserId());
      response.setAgentId(request.getAgentId());
      response.setToolId(request.getToolId());
      response.fail("Tool not found in registry: " + request.getToolId(), "TOOL_NOT_FOUND");
      resultFuture.complete(Collections.singleton(response));
      return;
    }

    // Check if this is an annotation-based tool (has LangChainToolAdapter executor)
    if (isAnnotationBasedTool(toolDef)) {
      executeAnnotationBasedTool(request, toolDef, resultFuture);
    } else {
      // Fall back to LLM-based execution for legacy tools
      executeLLMBasedTool(request, toolDef, resultFuture);
    }
  }

  /**
   * Finds a tool definition, checking annotation registry first, then manual registry.
   *
   * @param toolId The tool ID to find
   * @return ToolDefinition or null if not found
   */
  private ToolDefinition findToolDefinition(String toolId) {
    // Check annotation registry first
    if (annotationRegistry != null && annotationRegistry.hasTool(toolId)) {
      return annotationRegistry.getToolDefinition(toolId);
    }

    // Fall back to manual registry
    return toolRegistry.get(toolId);
  }

  /**
   * Checks if a tool is annotation-based (uses LangChainToolAdapter).
   *
   * @param toolDef The tool definition
   * @return true if annotation-based
   */
  private boolean isAnnotationBasedTool(ToolDefinition toolDef) {
    String executorClass = toolDef.getExecutorClass();
    return executorClass != null
        && executorClass.contains("LangChainToolAdapter");
  }

  /**
   * Executes a @Tool annotated method directly.
   *
   * @param request The tool call request
   * @param toolDef The tool definition
   * @param resultFuture The result future to complete
   */
  private void executeAnnotationBasedTool(
      ToolCallRequest request, ToolDefinition toolDef, ResultFuture<ToolCallResponse> resultFuture) {

    LOG.info("Executing @Tool annotated method for: {}", request.getToolId());

    try {
      // Create adapter and execute
      ToolExecutor executor = new LangChainToolAdapter(request.getToolId(), annotationRegistry);

      // Validate parameters
      if (!executor.validateParameters(request.getParameters())) {
        ToolCallResponse response = createErrorResponse(request, toolDef,
            "Invalid parameters", "INVALID_PARAMETERS");
        resultFuture.complete(Collections.singleton(response));
        return;
      }

      // Execute async
      CompletableFuture<Object> executionFuture = executor.execute(request.getParameters());

      // Process result
      executionFuture.whenComplete(
          (result, throwable) -> {
            ToolCallResponse response = createToolCallResponse(request, toolDef);

            if (throwable != null) {
              LOG.error(
                  "Annotation-based tool execution failed for flow {}, tool: {}",
                  request.getFlowId(),
                  request.getToolId(),
                  throwable);
              response.fail(throwable.getMessage(), "EXECUTION_ERROR");
            } else {
              // Convert result to string
              String resultStr = result != null ? result.toString() : "null";
              response.complete(resultStr);
              LOG.info(
                  "Annotation-based tool execution completed for flow {}, tool: {}",
                  request.getFlowId(),
                  request.getToolId());
            }

            resultFuture.complete(Collections.singleton(response));
          });

    } catch (Exception e) {
      LOG.error("Failed to create tool adapter for: {}", request.getToolId(), e);
      ToolCallResponse response = createErrorResponse(request, toolDef,
          "Tool adapter creation failed: " + e.getMessage(), "ADAPTER_ERROR");
      resultFuture.complete(Collections.singleton(response));
    }
  }

  /**
   * Executes a tool using LLM-based execution (legacy approach).
   *
   * @param request The tool call request
   * @param toolDef The tool definition
   * @param resultFuture The result future to complete
   */
  private void executeLLMBasedTool(
      ToolCallRequest request, ToolDefinition toolDef, ResultFuture<ToolCallResponse> resultFuture) {

    LOG.info("Executing LLM-based tool for: {}", request.getToolId());

    // Build prompt for tool execution
    String toolPrompt = buildToolExecutionPrompt(toolDef, request.getParameters());
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new UserMessage(toolPrompt));

    // Create LLM config from tool definition
    LLMConfig llmConfig = createLLMConfig(request, toolDef);

    // Execute async
    CompletableFuture<Response<AiMessage>> asyncResponse =
        langChainAsyncClient.generate(messages, llmConfig);

    processAsyncResponse(request, toolDef, resultFuture, asyncResponse);
  }

  /**
   * Creates a ToolCallResponse with common fields populated.
   *
   * @param request The request
   * @param toolDef The tool definition
   * @return A new ToolCallResponse
   */
  private ToolCallResponse createToolCallResponse(ToolCallRequest request, ToolDefinition toolDef) {
    ToolCallResponse response = new ToolCallResponse(
        request.getRequestId(),
        request.getFlowId(),
        request.getUserId(),
        request.getAgentId());
    response.setToolId(request.getToolId());
    response.setToolName(toolDef.getName());
    return response;
  }

  /**
   * Creates an error response.
   *
   * @param request The request
   * @param toolDef The tool definition
   * @param errorMessage The error message
   * @param errorCode The error code
   * @return A new ToolCallResponse with error
   */
  private ToolCallResponse createErrorResponse(
      ToolCallRequest request, ToolDefinition toolDef, String errorMessage, String errorCode) {
    ToolCallResponse response = createToolCallResponse(request, toolDef);
    response.fail(errorMessage, errorCode);
    return response;
  }

  @Override
  public void timeout(ToolCallRequest input, ResultFuture<ToolCallResponse> resultFuture) {
    LOG.warn("Tool call timed out for flow {}, tool: {}", input.getFlowId(), input.getToolId());

    ToolCallResponse response =
        new ToolCallResponse(
            input.getRequestId(), input.getFlowId(), input.getUserId(), input.getAgentId());
    response.setToolId(input.getToolId());
    response.setToolName(input.getToolName());
    response.fail("Tool execution timed out", "TIMEOUT");

    resultFuture.complete(Collections.singleton(response));
  }

  private String buildToolExecutionPrompt(
      ToolDefinition toolDef, Map<String, Object> parameters) {
    StringBuilder prompt = new StringBuilder();
    prompt.append("Execute the following tool:\n\n");
    prompt.append("Tool: ").append(toolDef.getName()).append("\n");
    prompt.append("Description: ").append(toolDef.getDescription()).append("\n");
    prompt.append("Parameters:\n");

    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      prompt.append("  - ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
    }

    prompt.append("\nProvide the result of executing this tool.");
    return prompt.toString();
  }

  private LLMConfig createLLMConfig(ToolCallRequest request, ToolDefinition toolDef) {
    LLMConfig config = new LLMConfig();
    config.setUserId(Long.valueOf(request.getUserId().hashCode()));
    config.setAiModel(AiModel.OLLAMA); // Default, should be configurable
    config.setSystemMessage("You are a tool execution assistant. Execute tools precisely and return structured results.");

    // Copy executor config from tool definition
    if (toolDef.getExecutorConfig() != null) {
      config.setProperties(toolDef.getExecutorConfig());
    }

    return config;
  }

  private void processAsyncResponse(
      ToolCallRequest request,
      ToolDefinition toolDef,
      ResultFuture<ToolCallResponse> resultFuture,
      CompletableFuture<Response<AiMessage>> asyncResponse) {

    asyncResponse.whenComplete(
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
            response.complete(result.content().text());
            LOG.info(
                "Tool execution completed for flow {}, tool: {}",
                request.getFlowId(),
                request.getToolId());
          }

          resultFuture.complete(Collections.singleton(response));
        });
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

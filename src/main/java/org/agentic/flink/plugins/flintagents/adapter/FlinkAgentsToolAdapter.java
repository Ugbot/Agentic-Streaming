package org.agentic.flink.plugins.flintagents.adapter;

import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.tools.ToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.agents.api.Agent;
import org.apache.flink.agents.api.annotation.Action;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.event.ToolRequestEvent;
import org.apache.flink.agents.api.event.ToolResponseEvent;
import org.apache.flink.agents.api.tools.ToolResponse;

/**
 * Adapter for wrapping our ToolExecutor implementations as Flink Agents Actions.
 *
 * <p>This class provides integration between:
 *
 * <ul>
 *   <li>Our custom {@link ToolExecutor} interface (async CompletableFuture-based)
 *   <li>Flink Agents {@code @Action} annotation and event-driven execution model
 * </ul>
 *
 * <p><b>Key Integration Points:</b>
 *
 * <ul>
 *   <li>Our tools execute asynchronously returning CompletableFuture
 *   <li>Flink Agents actions are event-driven methods listening to events
 *   <li>This adapter creates wrapper agents that bridge the two models
 * </ul>
 *
 * <p><b>Example Usage:</b>
 *
 * <pre>{@code
 * // Register our tools with Flink Agents runtime
 * ToolExecutor calculator = new CalculatorToolExecutor();
 * ToolDefinition calculatorDef = ToolDefinition.builder()
 *     .toolId("calculator")
 *     .description("Performs calculations")
 *     .build();
 *
 * // Create wrapper agent
 * Agent toolAgent = FlinkAgentsToolAdapter.createToolWrapperAgent(
 *     "calculator-agent",
 *     Map.of("calculator", calculator),
 *     Map.of("calculator", calculatorDef)
 * );
 *
 * // Use with Flink Agents runtime
 * AgentRuntime runtime = new AgentRuntime(env);
 * DataStream<Event> results = runtime.execute(toolAgent, events);
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see ToolExecutor
 * @see ToolDefinition
 */
public class FlinkAgentsToolAdapter {

  /**
   * Creates a Flink Agents Agent that wraps multiple tool executors.
   *
   * <p>This agent listens for ToolRequestEvents, executes the appropriate tool from our registry,
   * and emits ToolResponseEvents with the results.
   *
   * @param agentId Unique identifier for this tool wrapper agent
   * @param tools Map of tool ID to ToolExecutor
   * @param definitions Map of tool ID to ToolDefinition
   * @return Flink Agents Agent that wraps our tools
   */
  public static Agent createToolWrapperAgent(
      String agentId, Map<String, ToolExecutor> tools, Map<String, ToolDefinition> definitions) {
    return new ToolWrapperAgent(agentId, tools, definitions);
  }

  /**
   * Wrapper agent that exposes our tools as Flink Agents actions.
   *
   * <p>This agent:
   *
   * <ol>
   *   <li>Listens for ToolRequestEvent
   *   <li>Extracts tool calls and parameters
   *   <li>Executes our ToolExecutor implementations
   *   <li>Emits ToolResponseEvent with results
   * </ol>
   */
  public static class ToolWrapperAgent extends Agent {
    private final String agentId;
    private final Map<String, ToolExecutor> tools;
    private final Map<String, ToolDefinition> definitions;

    public ToolWrapperAgent(
        String agentId, Map<String, ToolExecutor> tools, Map<String, ToolDefinition> definitions) {
      this.agentId = agentId;
      this.tools = tools;
      this.definitions = definitions;
    }

    /**
     * Action that handles tool execution requests.
     *
     * <p>When a ToolRequestEvent arrives:
     *
     * <ol>
     *   <li>Extract tool calls from the event
     *   <li>For each tool call, lookup our ToolExecutor
     *   <li>Execute the tool asynchronously
     *   <li>Collect results and emit ToolResponseEvent
     * </ol>
     */
    @Action(listenEvents = {ToolRequestEvent.class})
    public void executeTool(ToolRequestEvent event, RunnerContext ctx) {
      UUID requestId = event.getId();
      Map<String, ToolResponse> responses = new HashMap<>();
      Map<String, Boolean> successMap = new HashMap<>();
      Map<String, String> errorMap = new HashMap<>();

      // Process each tool call
      for (Map<String, Object> toolCall : event.getToolCalls()) {
        String toolName = (String) toolCall.get("name");
        @SuppressWarnings("unchecked")
        Map<String, Object> parameters =
            (Map<String, Object>) toolCall.getOrDefault("parameters", new HashMap<>());
        String toolCallId = (String) toolCall.getOrDefault("id", toolName);

        // Lookup our tool
        ToolExecutor executor = tools.get(toolName);
        if (executor == null) {
          // Tool not found
          errorMap.put(toolCallId, "Tool not found: " + toolName);
          successMap.put(toolCallId, false);
          responses.put(toolCallId, ToolResponse.error("Tool not found: " + toolName));
          continue;
        }

        try {
          // Validate parameters
          if (!executor.validateParameters(parameters)) {
            errorMap.put(toolCallId, "Invalid parameters for tool: " + toolName);
            successMap.put(toolCallId, false);
            responses.put(toolCallId, ToolResponse.error("Invalid parameters for tool: " + toolName));
            continue;
          }

          // Execute our tool (synchronously for now - we're in an action handler)
          CompletableFuture<Object> resultFuture = executor.execute(parameters);

          try {
            // Get result (blocks, but action handlers can block)
            Object result = resultFuture.get();

            // Convert result to ToolResponse
            ToolResponse toolResponse = ToolResponse.success(result);

            responses.put(toolCallId, toolResponse);
            successMap.put(toolCallId, true);

          } catch (Exception executionError) {
            // Tool execution failed
            errorMap.put(toolCallId, executionError.getMessage());
            successMap.put(toolCallId, false);
            responses.put(toolCallId, ToolResponse.error(executionError));
          }

        } catch (Exception e) {
          // Validation or other error
          errorMap.put(toolCallId, e.getMessage());
          successMap.put(toolCallId, false);
          responses.put(toolCallId, ToolResponse.error(e));
        }
      }

      // Emit response event
      ToolResponseEvent responseEvent =
          new ToolResponseEvent(requestId, responses, successMap, errorMap);
      ctx.sendEvent(responseEvent);
    }

    public String getAgentId() {
      return agentId;
    }
  }

  /**
   * Creates a single-tool wrapper agent.
   *
   * <p>Convenience method for wrapping a single tool executor.
   *
   * @param toolId Tool identifier
   * @param executor Tool executor implementation
   * @param definition Tool definition
   * @return Flink Agents Agent wrapping the single tool
   */
  public static Agent wrapSingleTool(
      String toolId, ToolExecutor executor, ToolDefinition definition) {
    return createToolWrapperAgent(
        toolId + "-agent", Map.of(toolId, executor), Map.of(toolId, definition));
  }

  /**
   * Helper to convert our ToolDefinition to MCP Tool schema format.
   *
   * <p>Model Context Protocol (MCP) is the standard tool definition format in Flink Agents. This
   * method converts our tool definitions to that format.
   *
   * @param toolDefinition Our tool definition
   * @return MCP-compatible tool schema as Map
   */
  public static Map<String, Object> toMCPToolSchema(ToolDefinition toolDefinition) {
    Map<String, Object> mcpSchema = new HashMap<>();

    mcpSchema.put("name", toolDefinition.getToolId());
    mcpSchema.put("description", toolDefinition.getDescription());

    // Convert input schema
    if (toolDefinition.getInputSchema() != null) {
      mcpSchema.put("inputSchema", toolDefinition.getInputSchema());
    }

    // Convert output schema
    if (toolDefinition.getOutputSchema() != null) {
      mcpSchema.put("outputSchema", toolDefinition.getOutputSchema());
    }

    return mcpSchema;
  }

  /**
   * Example: Integration with our RAG tools.
   *
   * <p>Shows how to wrap our advanced RAG tools (semantic search, document ingestion) as Flink
   * Agents actions.
   *
   * <pre>{@code
   * // Wrap RAG tools
   * ToolExecutor semanticSearch = new SemanticSearchToolExecutor(qdrantClient);
   * ToolExecutor docIngestion = new DocumentIngestionToolExecutor(embeddingModel);
   *
   * Map<String, ToolExecutor> ragTools = Map.of(
   *     "semantic_search", semanticSearch,
   *     "ingest_document", docIngestion
   * );
   *
   * Map<String, ToolDefinition> ragDefs = Map.of(
   *     "semantic_search", semanticSearchDef,
   *     "ingest_document", docIngestionDef
   * );
   *
   * // Create RAG agent
   * Agent ragAgent = FlinkAgentsToolAdapter.createToolWrapperAgent(
   *     "rag-agent",
   *     ragTools,
   *     ragDefs
   * );
   * }</pre>
   */
  public static class RagToolsIntegrationExample {
    // Documentation placeholder
  }

  /**
   * Example: Integration with our validation/correction framework.
   *
   * <p>Shows how our multi-attempt validation can be integrated as Flink Agents actions.
   *
   * <pre>{@code
   * // Create validation agent
   * public class ValidationAgent extends Agent {
   *
   *     @Action(listenEvents = {ToolResponseEvent.class})
   *     public void validateToolResponse(ToolResponseEvent event, RunnerContext ctx) {
   *         // Use our ValidationFunction
   *         ValidationFunction validator = new ValidationFunction(...);
   *         ValidationResult result = validator.validate(event.getResponses());
   *
   *         if (result.isValid()) {
   *             ctx.sendEvent(new OutputEvent(event.getResponses()));
   *         } else {
   *             // Trigger correction using our correction mechanism
   *             ctx.sendEvent(new CorrectionRequestEvent(result.getFeedback()));
   *         }
   *     }
   * }
   * }</pre>
   */
  public static class ValidationIntegrationExample {
    // Documentation placeholder
  }
}

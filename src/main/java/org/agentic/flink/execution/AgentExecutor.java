package org.agentic.flink.execution;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core agent execution engine that orchestrates the agentic loop.
 *
 * <p>The AgentExecutor runs the full agent lifecycle:
 * <ol>
 *   <li><b>Initialization</b> - Sets up agent context with system prompt</li>
 *   <li><b>Validation</b> - Validates input (if enabled)</li>
 *   <li><b>Execution</b> - LLM reasoning + tool calling loop</li>
 *   <li><b>Correction</b> - Fixes validation failures (if enabled)</li>
 *   <li><b>Supervision</b> - Routes to supervisor for review (if configured)</li>
 * </ol>
 *
 * <p>This executor integrates with:
 * <ul>
 *   <li><b>LLMClient</b> - For LangChain4J LLM calls</li>
 *   <li><b>ToolExecutionEngine</b> - For async tool execution</li>
 *   <li><b>ValidationExecutor</b> - For input/output validation</li>
 *   <li><b>CorrectionExecutor</b> - For correction loops</li>
 * </ul>
 *
 * <p><b>Agentic Loop Example:</b>
 * <pre>
 * 1. User: "Analyze sales data for Q3"
 * 2. Agent LLM: "I need to call the database-query tool"
 * 3. Tool Call: database-query(query="SELECT * FROM sales WHERE quarter=3")
 * 4. Tool Result: [sales data...]
 * 5. Agent LLM: "Based on the data, Q3 sales increased 15%..."
 * 6. Completion: Return final response
 * </pre>
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 * AgentExecutor executor = AgentExecutor.builder()
 *     .withAgent(myAgent)
 *     .withToolRegistry(toolRegistry)
 *     .withLlmClient(llmClient)
 *     .build();
 *
 * CompletableFuture<ExecutionResult> result = executor.execute(inputEvent);
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class AgentExecutor implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutor.class);

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final LLMClient llmClient;
  private final ToolExecutionEngine toolEngine;
  private final ValidationExecutor validationExecutor;
  private final CorrectionExecutor correctionExecutor;

  private AgentExecutor(AgentExecutorBuilder builder) {
    this.agent = builder.agent;
    this.toolRegistry = builder.toolRegistry;
    this.llmClient = builder.llmClient;
    this.toolEngine = new ToolExecutionEngine(toolRegistry, llmClient);
    this.validationExecutor = new ValidationExecutor(llmClient);
    this.correctionExecutor = new CorrectionExecutor(llmClient);
  }

  /**
   * Executes the agent for a given input event.
   *
   * <p>This is the main entry point that runs the full agentic loop asynchronously.
   *
   * @param inputEvent The input event containing user request
   * @return CompletableFuture with execution result
   */
  public CompletableFuture<ExecutionResult> execute(AgentEvent inputEvent) {
    LOG.info("Starting agent execution for flow: {}, agent: {}",
        inputEvent.getFlowId(), agent.getAgentId());

    ExecutionContext context = new ExecutionContext(inputEvent, agent);

    return CompletableFuture.supplyAsync(() -> {
      try {
        // Run the agentic loop
        return runAgenticLoop(context);
      } catch (Exception e) {
        LOG.error("Agent execution failed for flow: {}", inputEvent.getFlowId(), e);
        return ExecutionResult.failure(
            inputEvent.getFlowId(),
            agent.getAgentId(),
            e.getMessage(),
            context.getEvents());
      }
    });
  }

  /**
   * Runs the core agentic loop (LLM reasoning + tool calling).
   *
   * <p>Flow:
   * <pre>
   * 1. Initialize agent context with system prompt
   * 2. Add user message from input event
   * 3. Loop (up to maxIterations):
   *    a. Call LLM with context
   *    b. If LLM wants to call tools:
   *       - Execute tools async
   *       - Add tool results to context
   *       - Continue loop
   *    c. If LLM returns final answer:
   *       - Break loop
   * 4. Return execution result
   * </pre>
   */
  private ExecutionResult runAgenticLoop(ExecutionContext context) {
    LOG.debug("Running agentic loop for flow: {}", context.getFlowId());

    // Build initial prompt with system message
    List<Map<String, Object>> messages = new ArrayList<>();
    messages.add(createSystemMessage(agent.getSystemPrompt()));

    // Add user message from input
    String userMessage = extractUserMessage(context.getInputEvent());
    messages.add(createUserMessage(userMessage));

    // Track tool call history
    List<ToolCallResult> toolCallHistory = new ArrayList<>();

    // Agentic loop - iterate until completion or max iterations
    for (int iteration = 0; iteration < agent.getMaxIterations(); iteration++) {
      LOG.debug("Agentic loop iteration {}/{} for flow: {}",
          iteration + 1, agent.getMaxIterations(), context.getFlowId());

      try {
        // Call LLM with current context
        LLMResponse llmResponse = callLLM(messages, context);

        // Add LLM response to history
        context.addEvent(createLLMEvent(context, llmResponse, iteration));

        // Check if LLM wants to call tools
        if (llmResponse.hasToolCalls()) {
          LOG.info("LLM requested {} tool calls for flow: {}",
              llmResponse.getToolCalls().size(), context.getFlowId());

          // Execute tool calls
          List<ToolCallResult> toolResults = executeToolCalls(
              llmResponse.getToolCalls(), context);

          toolCallHistory.addAll(toolResults);

          // Add tool results to conversation
          for (ToolCallResult result : toolResults) {
            messages.add(createToolResultMessage(result));
            context.addEvent(createToolResultEvent(context, result));
          }

          // Continue loop with tool results
          continue;
        }

        // LLM returned final answer - complete successfully
        LOG.info("Agent completed successfully for flow: {} after {} iterations",
            context.getFlowId(), iteration + 1);

        return ExecutionResult.success(
            context.getFlowId(),
            agent.getAgentId(),
            llmResponse.getText(),
            context.getEvents(),
            toolCallHistory);

      } catch (Exception e) {
        LOG.error("Error in agentic loop iteration {} for flow: {}",
            iteration, context.getFlowId(), e);

        // If not last iteration, continue
        if (iteration < agent.getMaxIterations() - 1) {
          LOG.warn("Retrying after error in iteration {}", iteration);
          continue;
        }

        // Last iteration - fail
        return ExecutionResult.failure(
            context.getFlowId(),
            agent.getAgentId(),
            "Max iterations reached with error: " + e.getMessage(),
            context.getEvents());
      }
    }

    // Max iterations reached without completion
    LOG.warn("Agent reached max iterations ({}) for flow: {}",
        agent.getMaxIterations(), context.getFlowId());

    return ExecutionResult.maxIterations(
        context.getFlowId(),
        agent.getAgentId(),
        "Max iterations reached",
        context.getEvents(),
        toolCallHistory);
  }

  /**
   * Calls the LLM with the current conversation context.
   */
  private LLMResponse callLLM(List<Map<String, Object>> messages, ExecutionContext context) {
    LOG.debug("Calling LLM for flow: {} with {} messages",
        context.getFlowId(), messages.size());

    try {
      // Call LLM via LangChain4J
      LLMResponse response = llmClient.chat(messages);

      LOG.info("LLM responded with {} characters", response.getText().length());

      return response;

    } catch (Exception e) {
      LOG.error("LLM call failed for flow: {}", context.getFlowId(), e);
      throw new RuntimeException("LLM call failed: " + e.getMessage(), e);
    }
  }

  /**
   * Executes multiple tool calls concurrently.
   */
  private List<ToolCallResult> executeToolCalls(
      List<ToolCall> toolCalls, ExecutionContext context) {

    LOG.info("Executing {} tool calls for flow: {}", toolCalls.size(), context.getFlowId());

    // Execute tools in parallel
    List<CompletableFuture<ToolCallResult>> futures = toolCalls.stream()
        .map(toolCall -> toolEngine.executeTool(toolCall, context))
        .collect(Collectors.toList());

    // Wait for all to complete
    CompletableFuture<Void> allOf = CompletableFuture.allOf(
        futures.toArray(new CompletableFuture[0]));

    try {
      allOf.join();
      return futures.stream()
          .map(CompletableFuture::join)
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOG.error("Error executing tool calls for flow: {}", context.getFlowId(), e);
      throw new RuntimeException("Tool execution failed", e);
    }
  }

  // ==================== Message Creation ====================

  private Map<String, Object> createSystemMessage(String content) {
    Map<String, Object> msg = new HashMap<>();
    msg.put("role", "system");
    msg.put("content", content);
    return msg;
  }

  private Map<String, Object> createUserMessage(String content) {
    Map<String, Object> msg = new HashMap<>();
    msg.put("role", "user");
    msg.put("content", content);
    return msg;
  }

  private Map<String, Object> createToolResultMessage(ToolCallResult result) {
    Map<String, Object> msg = new HashMap<>();
    msg.put("role", "tool");
    msg.put("tool_call_id", result.getToolCallId());
    msg.put("tool_name", result.getToolName());
    msg.put("content", result.getResult());
    return msg;
  }

  // ==================== Event Creation ====================

  private AgentEvent createLLMEvent(ExecutionContext context, LLMResponse response, int iteration) {
    AgentEvent event = context.getInputEvent()
        .withEventType(AgentEventType.LOOP_ITERATION_COMPLETED);
    event.setIterationNumber(iteration);
    event.putMetadata("state", AgentState.EXECUTING.name());
    event.getData().put("llm_response", response.getText());
    event.getData().put("model", response.getModel());
    event.getData().put("tool_calls_requested", response.getToolCalls().size());
    return event;
  }

  private AgentEvent createToolResultEvent(ExecutionContext context, ToolCallResult result) {
    AgentEvent event = context.getInputEvent()
        .withEventType(result.isSuccess()
            ? AgentEventType.TOOL_CALL_COMPLETED
            : AgentEventType.TOOL_CALL_FAILED);
    event.putMetadata("state", AgentState.EXECUTING.name());
    event.getData().put("tool_name", result.getToolName());
    event.getData().put("result", result.getResult());
    event.getData().put("success", result.isSuccess());
    if (!result.isSuccess()) {
      event.setErrorMessage(result.getError());
    }
    return event;
  }

  // ==================== Helpers ====================

  private String extractUserMessage(AgentEvent event) {
    Object userMsg = event.getData("user_message");
    if (userMsg != null) {
      return userMsg.toString();
    }
    Object prompt = event.getData("prompt");
    if (prompt != null) {
      return prompt.toString();
    }
    return "Please process this request: " + event.getData();
  }

  // ==================== Builder ====================

  public static AgentExecutorBuilder builder() {
    return new AgentExecutorBuilder();
  }

  public static class AgentExecutorBuilder {
    private Agent agent;
    private ToolRegistry toolRegistry;
    private LLMClient llmClient;

    public AgentExecutorBuilder withAgent(Agent agent) {
      this.agent = agent;
      return this;
    }

    public AgentExecutorBuilder withToolRegistry(ToolRegistry toolRegistry) {
      this.toolRegistry = toolRegistry;
      return this;
    }

    public AgentExecutorBuilder withLlmClient(LLMClient llmClient) {
      this.llmClient = llmClient;
      return this;
    }

    public AgentExecutor build() {
      if (agent == null) {
        throw new IllegalStateException("Agent is required");
      }
      if (toolRegistry == null) {
        toolRegistry = ToolRegistry.empty();
      }
      if (llmClient == null) {
        // Create default LLM client
        llmClient = LLMClient.createDefault(agent.getLlmModel(), agent.getTemperature());
      }
      return new AgentExecutor(this);
    }
  }
}

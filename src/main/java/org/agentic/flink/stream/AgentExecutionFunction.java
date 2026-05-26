package org.agentic.flink.stream;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.tool.ToolRegistry;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async Flink function for executing agents in a streaming context.
 *
 * <p>This function integrates the AgentExecutor with Flink's async I/O,
 * allowing agents to process events asynchronously without blocking the stream.
 *
 * <p><b>Features:</b>
 * <ul>
 *   <li>Async agent execution with non-blocking I/O</li>
 *   <li>Real LLM calls via LangChain4J</li>
 *   <li>Real tool execution</li>
 *   <li>Error handling and event enrichment</li>
 * </ul>
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 * DataStream<AgentEvent> results = inputStream.
 *     .flatMap(new AgentExecutionFunction(agent, toolRegistry, llmClient));
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class AgentExecutionFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutionFunction.class);

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final LLMClient llmClient;

  private transient AgentExecutor executor;

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry, LLMClient llmClient) {
    this.agent = agent;
    this.toolRegistry = toolRegistry;
    this.llmClient = llmClient;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    LOG.info("Initializing AgentExecutor for agent: {}", agent.getAgentId());

    // Create agent executor
    executor = AgentExecutor.builder()
        .withAgent(agent)
        .withToolRegistry(toolRegistry)
        .withLlmClient(llmClient)
        .build();

    LOG.info("AgentExecutor initialized successfully");
  }

  @Override
  public void asyncInvoke(AgentEvent inputEvent, ResultFuture<AgentEvent> resultFuture) {
    LOG.debug("Processing event: flow={}, agent={}, type={}",
        inputEvent.getFlowId(), inputEvent.getAgentId(), inputEvent.getEventType());

    // Execute agent asynchronously
    CompletableFuture<ExecutionResult> future = executor.execute(inputEvent);

    // Handle completion
    future.whenComplete((result, error) -> {
      if (error != null) {
        LOG.error("Agent execution failed for flow: {}", inputEvent.getFlowId(), error);

        // Create failure event
        AgentEvent failureEvent = inputEvent.withEventType(AgentEventType.FLOW_FAILED);
        failureEvent.setErrorMessage(error.getMessage());
        resultFuture.complete(java.util.Collections.singleton(failureEvent));

      } else if (result.isSuccess()) {
        LOG.info("Agent execution succeeded for flow: {}", inputEvent.getFlowId());

        // Create success event with output
        AgentEvent successEvent = inputEvent.withEventType(AgentEventType.FLOW_COMPLETED);
        successEvent.putData("result", result.getOutput());
        successEvent.putData("tool_calls", result.getToolCalls() != null ? result.getToolCalls().size() : 0);
        successEvent.putData("events_generated", result.getEvents().size());

        resultFuture.complete(java.util.Collections.singleton(successEvent));

      } else {
        LOG.warn("Agent execution completed with failure for flow: {}", inputEvent.getFlowId());

        // Create failure event
        AgentEvent failureEvent = inputEvent.withEventType(AgentEventType.FLOW_FAILED);
        failureEvent.setErrorMessage(result.getErrorMessage());
        resultFuture.complete(java.util.Collections.singleton(failureEvent));
      }
    });
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.error("Agent execution timed out for flow: {}", input.getFlowId());

    AgentEvent timeoutEvent = input.withEventType(AgentEventType.FLOW_FAILED);
    timeoutEvent.setErrorMessage("Agent execution timed out");
    resultFuture.complete(java.util.Collections.singleton(timeoutEvent));
  }
}

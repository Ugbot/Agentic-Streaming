package org.agentic.flink.stream;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous Flink function for executing agents in a streaming context.
 *
 * <p>This is a simpler alternative to AgentExecutionFunction that executes
 * agents synchronously. Use this when async I/O is not required.
 *
 * <p><b>Features:</b>
 * <ul>
 *   <li>Synchronous agent execution</li>
 *   <li>Real LLM calls via LangChain4J</li>
 *   <li>Real tool execution</li>
 *   <li>Error handling and event enrichment</li>
 * </ul>
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 * DataStream<AgentEvent> results = inputStream
 *     .flatMap(new AgentFlatMapFunction(agent, toolRegistry, llmClient));
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class AgentFlatMapFunction extends RichFlatMapFunction<AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentFlatMapFunction.class);

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final LLMClient llmClient;

  private transient AgentExecutor executor;

  public AgentFlatMapFunction(Agent agent, ToolRegistry toolRegistry, LLMClient llmClient) {
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
  public void flatMap(AgentEvent inputEvent, Collector<AgentEvent> out) throws Exception {
    LOG.debug("Processing event: flow={}, agent={}, type={}",
        inputEvent.getFlowId(), inputEvent.getAgentId(), inputEvent.getEventType());

    try {
      // Execute agent synchronously
      ExecutionResult result = executor.execute(inputEvent).get();

      if (result.isSuccess()) {
        LOG.info("Agent execution succeeded for flow: {}", inputEvent.getFlowId());

        // Create success event with output
        AgentEvent successEvent = inputEvent.withEventType(AgentEventType.FLOW_COMPLETED);
        successEvent.putData("result", result.getOutput());
        successEvent.putData("tool_calls", result.getToolCalls() != null ? result.getToolCalls().size() : 0);
        successEvent.putData("events_generated", result.getEvents().size());

        out.collect(successEvent);

        // Optionally emit intermediate events
        if (result.getEvents() != null) {
          for (AgentEvent intermediateEvent : result.getEvents()) {
            out.collect(intermediateEvent);
          }
        }

      } else {
        LOG.warn("Agent execution failed for flow: {}", inputEvent.getFlowId());

        // Create failure event
        AgentEvent failureEvent = inputEvent.withEventType(AgentEventType.FLOW_FAILED);
        failureEvent.setErrorMessage(result.getErrorMessage());
        out.collect(failureEvent);
      }

    } catch (Exception e) {
      LOG.error("Agent execution threw exception for flow: {}", inputEvent.getFlowId(), e);

      // Create failure event
      AgentEvent failureEvent = inputEvent.withEventType(AgentEventType.FLOW_FAILED);
      failureEvent.setErrorMessage(e.getMessage());
      out.collect(failureEvent);
    }
  }
}

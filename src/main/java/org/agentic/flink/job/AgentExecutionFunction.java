package org.agentic.flink.job;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CEP PatternProcessFunction that executes an agent when a pattern match occurs.
 *
 * <p>Bridges the declarative CEP pipeline to the {@link AgentExecutor} which handles
 * the full agentic loop: LLM reasoning, tool calling, validation, and correction.
 *
 * <p>The executor is lazily initialized on first invocation because
 * {@link PatternProcessFunction} does not provide an {@code open()} lifecycle hook.
 *
 * @see AgentExecutor
 * @see AgentJobGenerator
 */
public class AgentExecutionFunction extends PatternProcessFunction<AgentEvent, AgentEvent>
    implements TimedOutPartialMatchHandler<AgentEvent> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutionFunction.class);

  private final Agent agent;
  private final ToolRegistry toolRegistry;

  private static final OutputTag<AgentEvent> VALIDATION_FAILURES_TAG =
      AgentJobGenerator.VALIDATION_FAILURES_TAG;
  private static final OutputTag<AgentEvent> TIMEOUT_TAG =
      AgentJobGenerator.TIMEOUT_TAG;

  private transient AgentExecutor executor;

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry) {
    this.agent = agent;
    this.toolRegistry = toolRegistry;
  }

  private AgentExecutor getOrCreateExecutor() {
    if (executor == null) {
      executor = AgentExecutor.builder()
          .withAgent(agent)
          .withToolRegistry(toolRegistry)
          .build();
    }
    return executor;
  }

  @Override
  public void processMatch(
      Map<String, List<AgentEvent>> match, Context ctx, Collector<AgentEvent> out)
      throws Exception {

    // Pattern names come from AgentStateMachine.generateCepPattern()
    List<AgentEvent> startEvents = match.get("initial");

    if (startEvents == null || startEvents.isEmpty()) {
      LOG.warn("No initial event in pattern match for agent {}, skipping", agent.getAgentId());
      return;
    }

    AgentEvent startEvent = startEvents.get(0);
    String flowId = startEvent.getFlowId();

    LOG.info("Executing agent {} for flow: {}", agent.getAgentId(), flowId);

    try {
      long timeoutMs = agent.getTimeout() != null
          ? agent.getTimeout().toMillis()
          : 300_000L; // 5 minute default

      ExecutionResult result = getOrCreateExecutor()
          .execute(startEvent)
          .get(timeoutMs, TimeUnit.MILLISECONDS);

      if (result.isSuccess()) {
        AgentEvent completionEvent = startEvent.withEventType(AgentEventType.FLOW_COMPLETED);
        completionEvent.incrementIteration();
        completionEvent.putMetadata("state", AgentState.COMPLETED.name());
        completionEvent.getData().put("agent_id", agent.getAgentId());
        completionEvent.getData().put("output", result.getOutput());
        completionEvent.getData().put("tool_call_count", result.getEvents().size());
        completionEvent.getData().put("completion_timestamp", System.currentTimeMillis());
        out.collect(completionEvent);
        LOG.info("Agent {} completed flow: {}", agent.getAgentId(), flowId);
      } else {
        AgentEvent failureEvent = startEvent.withEventType(AgentEventType.FLOW_FAILED);
        failureEvent.incrementIteration();
        failureEvent.putMetadata("state", AgentState.FAILED.name());
        failureEvent.getData().put("agent_id", agent.getAgentId());
        failureEvent.getData().put("error", result.getOutput());
        ctx.output(VALIDATION_FAILURES_TAG, failureEvent);
        LOG.warn("Agent {} failed for flow: {}: {}", agent.getAgentId(), flowId, result.getOutput());
      }

    } catch (TimeoutException e) {
      LOG.error("Agent {} timed out for flow: {}", agent.getAgentId(), flowId);
      AgentEvent timeoutEvent = startEvent.withEventType(AgentEventType.FLOW_FAILED);
      timeoutEvent.incrementIteration();
      timeoutEvent.putMetadata("state", AgentState.FAILED.name());
      timeoutEvent.getData().put("error", "Agent execution timed out");
      timeoutEvent.getData().put("agent_id", agent.getAgentId());
      ctx.output(TIMEOUT_TAG, timeoutEvent);

    } catch (Exception e) {
      LOG.error("Agent {} execution error for flow: {}", agent.getAgentId(), flowId, e);
      AgentEvent failureEvent = startEvent.withEventType(AgentEventType.FLOW_FAILED);
      failureEvent.incrementIteration();
      failureEvent.putMetadata("state", AgentState.FAILED.name());
      failureEvent.getData().put("error", e.getMessage());
      failureEvent.getData().put("agent_id", agent.getAgentId());
      ctx.output(VALIDATION_FAILURES_TAG, failureEvent);
    }
  }

  @Override
  public void processTimedOutMatch(
      Map<String, List<AgentEvent>> match, Context ctx) throws Exception {

    List<AgentEvent> startEvents = match.get("initial");
    if (startEvents == null || startEvents.isEmpty()) {
      return;
    }

    AgentEvent startEvent = startEvents.get(0);
    LOG.warn("Pattern match timed out for agent {} flow: {}",
        agent.getAgentId(), startEvent.getFlowId());

    AgentEvent timeoutEvent = startEvent.withEventType(AgentEventType.TIMEOUT_OCCURRED);
    timeoutEvent.incrementIteration();
    timeoutEvent.putMetadata("state", AgentState.FAILED.name());
    timeoutEvent.getData().put("timeout_reason", "CEP pattern match exceeded time window");
    timeoutEvent.getData().put("agent_id", agent.getAgentId());
    ctx.output(TIMEOUT_TAG, timeoutEvent);

    if (agent.isCompensationEnabled()) {
      AgentEvent compensationEvent = startEvent.createCompensationEvent();
      compensationEvent.putMetadata("state", AgentState.COMPENSATING.name());
      ctx.output(AgentJobGenerator.COMPENSATION_TAG, compensationEvent);
    }
  }
}

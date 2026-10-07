package org.agentic.flink.job;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CEP {@link PatternProcessFunction} that turns a pattern match into an agent execution request.
 *
 * <p>This function does not run the LLM or tools itself. It emits the matched start event as a
 * request that {@link AgentJobGenerator} feeds into an Flink async operator running {@link
 * org.agentic.flink.stream.AgentExecutionFunction}, so the keyed CEP operator never blocks on model
 * or tool latency and checkpoints are not stalled by agent execution.
 *
 * <p>Flink CEP gives a {@code PatternProcessFunction} no keyed state, so this function is
 * stateless. The request is tagged with {@link #REQUEST_TURN_ID} and the keyed {@link
 * TurnDispatchDedupFunction} returned by {@link #dedup()} runs next in the job graph: it records
 * dispatched turns in checkpointed keyed state ({@code legacy.dispatched-turns}, keyed by flow id,
 * TTL {@link #DEFAULT_DEDUP_TTL} by default) so that a match redelivered after a restore does not
 * dispatch the same turn twice. Duplicates are dropped and counted by the {@code
 * duplicate_turns_dropped} metric.
 *
 * <p>Pattern timeouts and compensation requests are still emitted through side outputs here; {@link
 * AgentResultRouter} routes execution results and these events to the same tags at the end of the
 * pipeline.
 *
 * @see AgentExecutor
 * @see AgentJobGenerator
 * @see AgentResultRouter
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction} with {@code KeyedConversationLog}.
 */
@Deprecated
public class AgentExecutionFunction extends PatternProcessFunction<AgentEvent, AgentEvent>
    implements TimedOutPartialMatchHandler<AgentEvent> {

  private static final long serialVersionUID = 2L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutionFunction.class);

  public static final Duration DEFAULT_DEDUP_TTL = TurnDispatchDedupFunction.DEFAULT_DEDUP_TTL;
  public static final String DISPATCHED_TURNS_STATE =
      TurnDispatchDedupFunction.DISPATCHED_TURNS_STATE;
  public static final String DUPLICATES_METRIC = TurnDispatchDedupFunction.DUPLICATES_METRIC;
  public static final String DISPATCHED_METRIC = TurnDispatchDedupFunction.DISPATCHED_METRIC;

  /** Data key marking an emitted event as an execution request for the async operator. */
  public static final String REQUEST_TURN_ID = "request_turn_id";

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final long dedupTtlMillis;

  private static final OutputTag<AgentEvent> TIMEOUT_TAG = AgentJobGenerator.TIMEOUT_TAG;

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry) {
    this(agent, toolRegistry, DEFAULT_DEDUP_TTL);
  }

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry, Duration dedupTtl) {
    this.agent = Objects.requireNonNull(agent, "agent");
    this.toolRegistry = toolRegistry;
    if (dedupTtl == null || dedupTtl.isZero() || dedupTtl.isNegative()) {
      throw new IllegalArgumentException("dedupTtl must be positive, got " + dedupTtl);
    }
    this.dedupTtlMillis = dedupTtl.toMillis();
  }

  public Agent getAgent() {
    return agent;
  }

  public ToolRegistry getToolRegistry() {
    return toolRegistry;
  }

  public Duration getDedupTtl() {
    return Duration.ofMillis(dedupTtlMillis);
  }

  /** The keyed dedup operator that follows this function in the job graph. */
  public TurnDispatchDedupFunction dedup() {
    return new TurnDispatchDedupFunction(agent, getDedupTtl());
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
    String turnId = AgentExecutor.turnIdOf(startEvent);

    AgentEvent request = startEvent.withEventType(startEvent.getEventType());
    request.setAgentId(agent.getAgentId());
    request.putData(REQUEST_TURN_ID, turnId);
    request.putMetadata("state", AgentState.EXECUTING.name());
    out.collect(request);
  }

  @Override
  public void processTimedOutMatch(Map<String, List<AgentEvent>> match, Context ctx)
      throws Exception {

    List<AgentEvent> startEvents = match.get("initial");
    if (startEvents == null || startEvents.isEmpty()) {
      return;
    }

    AgentEvent startEvent = startEvents.get(0);
    LOG.warn(
        "Pattern match timed out for agent {} flow: {}",
        agent.getAgentId(),
        startEvent.getFlowId());

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

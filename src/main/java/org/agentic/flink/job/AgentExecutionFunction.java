package org.agentic.flink.job;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CEP {@link PatternProcessFunction} that turns a pattern match into an agent execution request.
 *
 * <p>This function does not run the LLM or tools itself. It emits the matched start event as a
 * request that {@link AgentJobGenerator} feeds into an Flink async operator running
 * {@link org.agentic.flink.stream.AgentExecutionFunction}, so the keyed CEP operator never
 * blocks on model or tool latency and checkpoints are not stalled by agent execution.
 *
 * <p>Dispatched turns are recorded in keyed state ({@code legacy.dispatched-turns}, keyed by
 * flow id, TTL {@link #DEFAULT_DEDUP_TTL} by default) so that a match redelivered after a
 * restore does not dispatch the same turn twice. Duplicates are dropped and counted by the
 * {@code duplicate_turns_dropped} metric.
 *
 * <p>Pattern timeouts and compensation requests are still emitted through side outputs here;
 * {@link AgentResultRouter} routes execution results and these events to the same tags at the
 * end of the pipeline.
 *
 * @see AgentExecutor
 * @see AgentJobGenerator
 * @see AgentResultRouter
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction} with
 *     {@code KeyedConversationLog}.
 */
@Deprecated
public class AgentExecutionFunction extends PatternProcessFunction<AgentEvent, AgentEvent>
    implements TimedOutPartialMatchHandler<AgentEvent> {

  private static final long serialVersionUID = 2L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutionFunction.class);

  public static final Duration DEFAULT_DEDUP_TTL = Duration.ofHours(24);
  public static final String DISPATCHED_TURNS_STATE = "legacy.dispatched-turns";
  public static final String DUPLICATES_METRIC = "duplicate_turns_dropped";
  public static final String DISPATCHED_METRIC = "turns_dispatched";
  /** Data key marking an emitted event as an execution request for the async operator. */
  public static final String REQUEST_TURN_ID = "request_turn_id";

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final long dedupTtlMillis;

  private static final OutputTag<AgentEvent> TIMEOUT_TAG =
      AgentJobGenerator.TIMEOUT_TAG;

  private transient MapState<String, Long> dispatchedTurns;
  private transient Counter duplicatesDropped;
  private transient Counter dispatched;

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

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    MapStateDescriptor<String, Long> descriptor =
        new MapStateDescriptor<>(DISPATCHED_TURNS_STATE, String.class, Long.class);
    descriptor.enableTimeToLive(
        StateTtlConfig.newBuilder(Duration.ofMillis(dedupTtlMillis))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
    dispatchedTurns = getRuntimeContext().getMapState(descriptor);
    duplicatesDropped = getRuntimeContext().getMetricGroup().counter(DUPLICATES_METRIC);
    dispatched = getRuntimeContext().getMetricGroup().counter(DISPATCHED_METRIC);
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

    if (dispatchedTurns.contains(turnId)) {
      duplicatesDropped.inc();
      LOG.warn("Turn {} for flow {} already dispatched to agent {}, dropping duplicate match",
          turnId, startEvent.getFlowId(), agent.getAgentId());
      return;
    }
    dispatchedTurns.put(turnId, ctx.currentProcessingTime());
    dispatched.inc();

    AgentEvent request = startEvent.withEventType(startEvent.getEventType());
    request.setAgentId(agent.getAgentId());
    request.putData(REQUEST_TURN_ID, turnId);
    request.putMetadata("state", AgentState.EXECUTING.name());
    LOG.info("Dispatching agent {} for flow: {} (turn {})",
        agent.getAgentId(), startEvent.getFlowId(), turnId);
    out.collect(request);
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

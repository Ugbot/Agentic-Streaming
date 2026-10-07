package org.agentic.flink.job;

import java.time.Duration;
import java.util.Objects;
import org.agentic.flink.annotation.Internal;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.dsl.Agent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed dedup of legacy DSL execution requests: forwards the first request for a turn id per
 * conversation key and drops later redeliveries (CEP re-matches after a restart, replayed input).
 * The set of dispatched turn ids lives in keyed Flink state with a TTL, so it is part of every
 * checkpoint and survives a restart. Flink CEP's {@code PatternProcessFunction} cannot hold keyed
 * state, which is why this runs as its own keyed operator after {@link AgentTurnDispatcher}.
 */
@Deprecated(since = "1.0.0")
@Internal
public class TurnDispatchDedupFunction
    extends KeyedProcessFunction<String, AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(TurnDispatchDedupFunction.class);

  public static final Duration DEFAULT_DEDUP_TTL = Duration.ofHours(24);
  public static final String DISPATCHED_TURNS_STATE = "legacy.dispatched-turns";
  public static final String DUPLICATES_METRIC = "duplicate_turns_dropped";
  public static final String DISPATCHED_METRIC = "turns_dispatched";

  private final String agentId;
  private final long dedupTtlMillis;

  private transient MapState<String, Long> dispatchedTurns;
  private transient Counter duplicatesDropped;
  private transient Counter dispatched;

  /**
   * Uses {@link Agent#getShortTermTtl()} as the state TTL, or {@link #DEFAULT_DEDUP_TTL} when
   * unset.
   */
  public TurnDispatchDedupFunction(Agent agent) {
    this(agent, dedupTtlOf(agent));
  }

  public TurnDispatchDedupFunction(Agent agent, Duration dedupTtl) {
    this.agentId = Objects.requireNonNull(agent, "agent").getAgentId();
    if (dedupTtl == null || dedupTtl.isZero() || dedupTtl.isNegative()) {
      throw new IllegalArgumentException("dedupTtl must be positive, got " + dedupTtl);
    }
    this.dedupTtlMillis = dedupTtl.toMillis();
  }

  static Duration dedupTtlOf(Agent agent) {
    Duration ttl = Objects.requireNonNull(agent, "agent").getShortTermTtl();
    return ttl == null || ttl.isZero() || ttl.isNegative() ? DEFAULT_DEDUP_TTL : ttl;
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
  public void processElement(AgentEvent request, Context ctx, Collector<AgentEvent> out)
      throws Exception {
    String turnId = String.valueOf(request.getData(AgentTurnDispatcher.REQUEST_TURN_ID));
    if (dispatchedTurns.contains(turnId)) {
      duplicatesDropped.inc();
      LOG.warn(
          "Turn {} for flow {} already dispatched to agent {}, dropping duplicate match",
          turnId,
          request.getFlowId(),
          agentId);
      return;
    }
    dispatchedTurns.put(turnId, ctx.timerService().currentProcessingTime());
    dispatched.inc();
    LOG.info("Dispatching agent {} for flow: {} (turn {})", agentId, request.getFlowId(), turnId);
    out.collect(request);
  }
}

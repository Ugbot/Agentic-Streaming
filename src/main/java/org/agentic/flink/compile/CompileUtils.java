package org.agentic.flink.compile;

import org.agentic.flink.plan.AgentPlan;
import org.agentic.flink.plan.AgentPlanProcessFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point called from Python via PyFlink's {@code invoke_method} gateway. Given a
 * {@link KeyedStream} and a serialized {@link AgentPlan}, attaches an
 * {@link AgentPlanProcessFunction} to the stream and returns the resulting {@link DataStream}.
 *
 * <p>Mirrors the role of upstream Apache Flink Agents' {@code CompileUtils.connectToAgent} for
 * this framework's plan format.
 */
public final class CompileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CompileUtils.class);

  private CompileUtils() {}

  /** Attach an agent operator to a keyed stream from a JSON plan. */
  public static <K> SingleOutputStreamOperator<Object> attachAgent(
      KeyedStream<Object, K> stream, String planJson) {
    if (stream == null) {
      throw new IllegalArgumentException("stream must not be null");
    }
    AgentPlan plan = AgentPlan.fromJson(planJson);
    LOG.info(
        "Attaching agent operator: agent_id={}, planJson={} chars", plan.getAgentId(),
        planJson.length());
    AgentPlanProcessFunction<K> fn = new AgentPlanProcessFunction<>(plan);
    return stream.process(fn, TypeInformation.of(Object.class)).name("agent:" + plan.getAgentId());
  }

  /** Attach an agent operator from a pre-parsed plan (Java-side convenience for tests). */
  public static <K> SingleOutputStreamOperator<Object> attachAgent(
      KeyedStream<Object, K> stream, AgentPlan plan) {
    if (stream == null) {
      throw new IllegalArgumentException("stream must not be null");
    }
    if (plan == null) {
      throw new IllegalArgumentException("plan must not be null");
    }
    AgentPlanProcessFunction<K> fn = new AgentPlanProcessFunction<>(plan);
    return stream.process(fn, TypeInformation.of(Object.class)).name("agent:" + plan.getAgentId());
  }
}

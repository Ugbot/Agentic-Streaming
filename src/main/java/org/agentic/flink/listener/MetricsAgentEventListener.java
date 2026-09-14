package org.agentic.flink.listener;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Reference listener that maintains in-memory counters/sums for the most important hooks.
 *
 * <p>The counters work outside Flink (unit tests, plain JVM hosts). Inside a Flink operator call
 * {@link #open(RuntimeContext)} from the function's {@code open()}: the listener then also
 * registers Flink {@link Counter}s and gauges under {@link #METRIC_GROUP} on the operator's
 * metric group and updates them alongside the local counters. Counters are process-local; after
 * Java deserialization (Flink shipping the function to a task) they restart from zero and must be
 * registered again through {@code open()}, which every {@code RichFunction} restart does.
 */
public final class MetricsAgentEventListener implements AgentEventListener {
  private static final long serialVersionUID = 2L;

  public static final String METRIC_GROUP = "agent_events";

  private transient LongAdder chatRequests;
  private transient LongAdder chatResponses;
  private transient LongAdder toolCalls;
  private transient LongAdder toolFailures;
  private transient LongAdder compactions;
  private transient LongAdder factsWritten;
  private transient AtomicLong tokensUsed;
  private transient LongAdder inferences;
  private transient AtomicLong inferenceMillis;
  private transient LongAdder guardrailBlocks;
  private transient LongAdder guardrailRewrites;

  private transient Counter flinkChatRequests;
  private transient Counter flinkChatResponses;
  private transient Counter flinkToolCalls;
  private transient Counter flinkToolFailures;
  private transient Counter flinkCompactions;
  private transient Counter flinkFactsWritten;
  private transient Counter flinkTokensUsed;
  private transient Counter flinkInferences;
  private transient Counter flinkInferenceMillis;
  private transient Counter flinkGuardrailBlocks;
  private transient Counter flinkGuardrailRewrites;

  public MetricsAgentEventListener() {
    initCounters();
  }

  private void initCounters() {
    chatRequests = new LongAdder();
    chatResponses = new LongAdder();
    toolCalls = new LongAdder();
    toolFailures = new LongAdder();
    compactions = new LongAdder();
    factsWritten = new LongAdder();
    tokensUsed = new AtomicLong();
    inferences = new LongAdder();
    inferenceMillis = new AtomicLong();
    guardrailBlocks = new LongAdder();
    guardrailRewrites = new LongAdder();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initCounters();
  }

  /**
   * Registers Flink metrics on {@code runtimeContext.getMetricGroup()}. Call from the owning
   * function's {@code open()}; calling it again replaces the registrations.
   */
  public void open(RuntimeContext runtimeContext) {
    open(runtimeContext.getMetricGroup());
  }

  /** Registers Flink metrics under {@link #METRIC_GROUP} of the given group. */
  public void open(MetricGroup parent) {
    MetricGroup group = parent.addGroup(METRIC_GROUP);
    flinkChatRequests = group.counter("chat_requests");
    flinkChatResponses = group.counter("chat_responses");
    flinkToolCalls = group.counter("tool_calls");
    flinkToolFailures = group.counter("tool_failures");
    flinkCompactions = group.counter("compactions");
    flinkFactsWritten = group.counter("facts_written");
    flinkTokensUsed = group.counter("tokens_used");
    flinkInferences = group.counter("inferences");
    flinkInferenceMillis = group.counter("inference_millis");
    flinkGuardrailBlocks = group.counter("guardrail_blocks");
    flinkGuardrailRewrites = group.counter("guardrail_rewrites");
    group.gauge("tokens_used_total", tokensUsed::get);
    group.gauge("inference_millis_total", inferenceMillis::get);
  }

  /** Whether {@link #open} has registered Flink metrics on this instance. */
  public boolean isRegistered() {
    return flinkChatRequests != null;
  }

  private static void inc(Counter counter) {
    if (counter != null) {
      counter.inc();
    }
  }

  private static void inc(Counter counter, long n) {
    if (counter != null) {
      counter.inc(n);
    }
  }

  @Override
  public void onChatRequest(String agentId, String modelName, int messageCount) {
    chatRequests.increment();
    inc(flinkChatRequests);
  }

  @Override
  public void onChatResponse(
      String agentId, String modelName, int responseLength, Long tokens) {
    chatResponses.increment();
    inc(flinkChatResponses);
    if (tokens != null) {
      tokensUsed.addAndGet(tokens);
      inc(flinkTokensUsed, tokens);
    }
  }

  @Override
  public void onToolCallEnd(
      String agentId, String toolName, String toolCallId, boolean success, long durationMs) {
    toolCalls.increment();
    inc(flinkToolCalls);
    if (!success) {
      toolFailures.increment();
      inc(flinkToolFailures);
    }
  }

  @Override
  public void onCompaction(
      String agentId, String flowId, int itemsBefore, int itemsAfter, long durationMs) {
    compactions.increment();
    inc(flinkCompactions);
  }

  @Override
  public void onLongTermSync(String agentId, String flowId, int written) {
    factsWritten.add(written);
    inc(flinkFactsWritten, written);
  }

  @Override
  public void onInference(String agentId, String modelName, String task, long durationMs) {
    inferences.increment();
    inferenceMillis.addAndGet(durationMs);
    inc(flinkInferences);
    inc(flinkInferenceMillis, durationMs);
  }

  @Override
  public void onGuardrailBlock(String agentId, String modelName, String label) {
    guardrailBlocks.increment();
    inc(flinkGuardrailBlocks);
  }

  @Override
  public void onGuardrailRewrite(String agentId, String modelName, String reason) {
    guardrailRewrites.increment();
    inc(flinkGuardrailRewrites);
  }

  public long getInferences() {
    return inferences.sum();
  }

  public long getInferenceMillis() {
    return inferenceMillis.get();
  }

  public long getGuardrailBlocks() {
    return guardrailBlocks.sum();
  }

  public long getGuardrailRewrites() {
    return guardrailRewrites.sum();
  }

  public long getChatRequests() {
    return chatRequests.sum();
  }

  public long getChatResponses() {
    return chatResponses.sum();
  }

  public long getToolCalls() {
    return toolCalls.sum();
  }

  public long getToolFailures() {
    return toolFailures.sum();
  }

  public long getCompactions() {
    return compactions.sum();
  }

  public long getFactsWritten() {
    return factsWritten.sum();
  }

  public long getTokensUsed() {
    return tokensUsed.get();
  }
}

package org.agentic.flink.listener;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Reference listener that maintains in-memory counters/sums for the most important hooks.
 *
 * <p>Designed to be wired against Flink's {@code MetricGroup} by callers that hold a
 * {@code RuntimeContext}: register the counters / gauges in {@code open()} and update them from
 * the methods on this listener. We don't take a hard dependency on {@code flink-metrics-core}
 * here because the listener interface must work outside Flink (e.g. in unit tests).
 */
public final class MetricsAgentEventListener implements AgentEventListener {
  private static final long serialVersionUID = 1L;

  private final transient LongAdder chatRequests = new LongAdder();
  private final transient LongAdder chatResponses = new LongAdder();
  private final transient LongAdder toolCalls = new LongAdder();
  private final transient LongAdder toolFailures = new LongAdder();
  private final transient LongAdder compactions = new LongAdder();
  private final transient LongAdder factsWritten = new LongAdder();
  private final transient AtomicLong tokensUsed = new AtomicLong();
  private final transient LongAdder inferences = new LongAdder();
  private final transient AtomicLong inferenceMillis = new AtomicLong();
  private final transient LongAdder guardrailBlocks = new LongAdder();
  private final transient LongAdder guardrailRewrites = new LongAdder();

  @Override
  public void onChatRequest(String agentId, String modelName, int messageCount) {
    chatRequests.increment();
  }

  @Override
  public void onChatResponse(
      String agentId, String modelName, int responseLength, Long tokens) {
    chatResponses.increment();
    if (tokens != null) tokensUsed.addAndGet(tokens);
  }

  @Override
  public void onToolCallEnd(
      String agentId, String toolName, String toolCallId, boolean success, long durationMs) {
    toolCalls.increment();
    if (!success) toolFailures.increment();
  }

  @Override
  public void onCompaction(
      String agentId, String flowId, int itemsBefore, int itemsAfter, long durationMs) {
    compactions.increment();
  }

  @Override
  public void onLongTermSync(String agentId, String flowId, int written) {
    factsWritten.add(written);
  }

  @Override
  public void onInference(String agentId, String modelName, String task, long durationMs) {
    inferences.increment();
    inferenceMillis.addAndGet(durationMs);
  }

  @Override
  public void onGuardrailBlock(String agentId, String modelName, String label) {
    guardrailBlocks.increment();
  }

  @Override
  public void onGuardrailRewrite(String agentId, String modelName, String reason) {
    guardrailRewrites.increment();
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

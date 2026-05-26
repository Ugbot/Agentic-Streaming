package org.agentic.flink.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reference listener that emits each lifecycle event as an SLF4J log line. */
public final class LoggingAgentEventListener implements AgentEventListener {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LoggingAgentEventListener.class);

  @Override
  public void onAgentStart(String agentId) {
    LOG.info("agent.start id={}", agentId);
  }

  @Override
  public void onChatRequest(String agentId, String modelName, int messageCount) {
    LOG.debug("chat.request id={} model={} messages={}", agentId, modelName, messageCount);
  }

  @Override
  public void onChatResponse(
      String agentId, String modelName, int responseLength, Long tokensUsed) {
    LOG.debug(
        "chat.response id={} model={} chars={} tokens={}",
        agentId, modelName, responseLength, tokensUsed);
  }

  @Override
  public void onToolCallStart(String agentId, String toolName, String toolCallId) {
    LOG.debug("tool.start id={} tool={} callId={}", agentId, toolName, toolCallId);
  }

  @Override
  public void onToolCallEnd(
      String agentId, String toolName, String toolCallId, boolean success, long durationMs) {
    LOG.info(
        "tool.end id={} tool={} callId={} success={} durationMs={}",
        agentId, toolName, toolCallId, success, durationMs);
  }

  @Override
  public void onCompaction(
      String agentId, String flowId, int itemsBefore, int itemsAfter, long durationMs) {
    LOG.info(
        "compaction id={} flow={} {} -> {} items in {}ms",
        agentId, flowId, itemsBefore, itemsAfter, durationMs);
  }

  @Override
  public void onLongTermSync(String agentId, String flowId, int factsWritten) {
    LOG.debug(
        "longterm.sync id={} flow={} facts={}", agentId, flowId, factsWritten);
  }

  @Override
  public void onError(String agentId, String stage, Throwable error) {
    LOG.error("agent.error id={} stage={}: {}", agentId, stage, error.getMessage(), error);
  }

  @Override
  public void onInference(String agentId, String modelName, String task, long durationMs) {
    LOG.debug("inference id={} model={} task={} durationMs={}", agentId, modelName, task, durationMs);
  }

  @Override
  public void onGuardrailBlock(String agentId, String modelName, String label) {
    LOG.info("guardrail.block id={} model={} label={}", agentId, modelName, label);
  }

  @Override
  public void onGuardrailRewrite(String agentId, String modelName, String reason) {
    LOG.info("guardrail.rewrite id={} model={} reason={}", agentId, modelName, reason);
  }
}

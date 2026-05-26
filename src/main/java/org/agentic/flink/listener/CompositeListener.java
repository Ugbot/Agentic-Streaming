package org.agentic.flink.listener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fans out a single hook invocation to every registered listener. Exceptions in one listener
 * never block the others — they're logged and swallowed.
 */
public final class CompositeListener implements AgentEventListener {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CompositeListener.class);

  private final List<AgentEventListener> listeners;

  public CompositeListener(List<AgentEventListener> listeners) {
    this.listeners =
        listeners == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(listeners));
  }

  @Override
  public void onAgentStart(String agentId) {
    for (AgentEventListener l : listeners) safe(() -> l.onAgentStart(agentId), l, "onAgentStart");
  }

  @Override
  public void onChatRequest(String agentId, String modelName, int messageCount) {
    for (AgentEventListener l : listeners)
      safe(() -> l.onChatRequest(agentId, modelName, messageCount), l, "onChatRequest");
  }

  @Override
  public void onChatResponse(
      String agentId, String modelName, int responseLength, Long tokensUsed) {
    for (AgentEventListener l : listeners)
      safe(
          () -> l.onChatResponse(agentId, modelName, responseLength, tokensUsed),
          l,
          "onChatResponse");
  }

  @Override
  public void onToolCallStart(String agentId, String toolName, String toolCallId) {
    for (AgentEventListener l : listeners)
      safe(() -> l.onToolCallStart(agentId, toolName, toolCallId), l, "onToolCallStart");
  }

  @Override
  public void onToolCallEnd(
      String agentId, String toolName, String toolCallId, boolean success, long durationMs) {
    for (AgentEventListener l : listeners)
      safe(
          () -> l.onToolCallEnd(agentId, toolName, toolCallId, success, durationMs),
          l,
          "onToolCallEnd");
  }

  @Override
  public void onCompaction(
      String agentId, String flowId, int itemsBefore, int itemsAfter, long durationMs) {
    for (AgentEventListener l : listeners)
      safe(
          () -> l.onCompaction(agentId, flowId, itemsBefore, itemsAfter, durationMs),
          l,
          "onCompaction");
  }

  @Override
  public void onLongTermSync(String agentId, String flowId, int factsWritten) {
    for (AgentEventListener l : listeners)
      safe(() -> l.onLongTermSync(agentId, flowId, factsWritten), l, "onLongTermSync");
  }

  @Override
  public void onError(String agentId, String stage, Throwable error) {
    for (AgentEventListener l : listeners)
      safe(() -> l.onError(agentId, stage, error), l, "onError");
  }

  @Override
  public void onInference(String agentId, String modelName, String task, long durationMs) {
    for (AgentEventListener l : listeners)
      safe(() -> l.onInference(agentId, modelName, task, durationMs), l, "onInference");
  }

  @Override
  public void onGuardrailBlock(String agentId, String modelName, String label) {
    for (AgentEventListener l : listeners)
      safe(() -> l.onGuardrailBlock(agentId, modelName, label), l, "onGuardrailBlock");
  }

  @Override
  public void onGuardrailRewrite(String agentId, String modelName, String reason) {
    for (AgentEventListener l : listeners)
      safe(() -> l.onGuardrailRewrite(agentId, modelName, reason), l, "onGuardrailRewrite");
  }

  @Override
  public String name() {
    return "CompositeListener(" + listeners.size() + ")";
  }

  public List<AgentEventListener> getListeners() {
    return listeners;
  }

  private static void safe(Runnable r, AgentEventListener owner, String hook) {
    try {
      r.run();
    } catch (Throwable t) {
      LOG.warn("Listener {} threw in {}: {}", owner.name(), hook, t.toString());
    }
  }
}

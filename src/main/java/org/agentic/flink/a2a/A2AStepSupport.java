package org.agentic.flink.a2a;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.core.AgentEvent;

/**
 * Shared, stateless helpers for an {@link A2AStep}'s runtime operators — used by both the keyed
 * blocking {@link A2ADelegatingProcessFunction} and the stateless {@link A2AAsyncFunction} so the
 * input-resolution, message-building, remote-call, and result-mapping conventions stay identical
 * across the blocking and non-blocking paths.
 *
 * <p>The remote {@code contextId} (A2A conversation continuity) is carried on the event under
 * {@link #contextIdKey(A2AStep)} rather than in keyed state, so it can ride through the stateless
 * async operator: a keyed pre-step stamps the stored contextId on, the async operator forwards it to
 * the peer and writes the peer's (possibly new) contextId back, and a keyed post-step persists it.
 */
final class A2AStepSupport {

  private A2AStepSupport() {}

  /** Event data key under which the remote A2A contextId rides between operators. */
  static String contextIdKey(A2AStep step) {
    return step.outputKey() + ".contextId";
  }

  /** Resolve the prompt text for the peer from the event, honoring the step's configured inputKey. */
  static String resolveInput(A2AStep step, AgentEvent event) {
    Map<String, Object> data = event.getData();
    if (data == null) {
      return null;
    }
    if (step.inputKey() != null) {
      Object v = data.get(step.inputKey());
      return v == null ? null : v.toString();
    }
    // Default resolution order for chaining after a local agent (which writes "result").
    for (String key : List.of("input", "result", "output", "prompt")) {
      Object v = data.get(key);
      if (v instanceof String && !((String) v).isEmpty()) {
        return (String) v;
      }
      if (v != null) {
        return v.toString();
      }
    }
    return null;
  }

  /** Build a USER message with the input as a text part, continuing the given contextId if present. */
  static A2AMessage buildMessage(String input, String contextId) {
    return new A2AMessage(
        A2AMessage.Role.USER,
        UUID.randomUUID().toString(),
        List.of(A2APart.text(input == null ? "" : input)),
        contextId,
        null,
        null);
  }

  /** Invoke the peer: streaming when the spec opts in, otherwise send + poll to a final state. */
  static A2ATask call(A2AClient client, A2AStep step, String input, String contextId) {
    A2AMessage message = buildMessage(input, contextId);
    return step.spec().streaming()
        ? client.stream(message, t -> {})
        : client.sendAndAwait(message, step.spec().pollIntervalMs(), step.spec().requestTimeoutMs());
  }

  /** Flatten a task's artifact text parts. */
  static String artifactText(A2ATask task) {
    List<String> texts = new ArrayList<>();
    for (A2AArtifact artifact : task.getArtifacts()) {
      String t = artifact.textContent();
      if (!t.isEmpty()) {
        texts.add(t);
      }
    }
    return String.join("\n", texts);
  }

  /**
   * Write the peer's result onto the event: the artifact text under the step's output key, plus
   * state/taskId/contextId side fields, and advance the current stage. The returned contextId is
   * written under {@link #contextIdKey(A2AStep)} so a downstream keyed operator can persist it.
   */
  static void applyResult(A2AStep step, AgentEvent event, A2ATask task) {
    event.putData(step.outputKey(), artifactText(task));
    event.putData(step.outputKey() + ".state", task.getState().wire());
    event.putData(step.outputKey() + ".taskId", task.getId());
    if (task.getContextId() != null) {
      event.putData(contextIdKey(step), task.getContextId());
    }
    event.setCurrentStage(step.name());
  }
}

package org.agentic.flink.a2a;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link A2AClient} test double. Models a remote agent that takes {@code workingPolls}
 * {@code tasks/get} cycles to finish, then completes with a deterministic artifact echoing the
 * inbound text. Lets foundation and tool tests exercise the full send/poll/await path with no SDK
 * or network. Not for production use.
 */
public final class FakeA2AClient implements A2AClient {
  private final RemoteAgentSpec spec;
  private final int workingPolls;
  private final boolean failTerminal;
  private final Map<String, A2ATask> tasks = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> polls = new ConcurrentHashMap<>();
  private final A2AAgentCard card;

  public FakeA2AClient(RemoteAgentSpec spec, int workingPolls, boolean failTerminal) {
    this.spec = spec;
    this.workingPolls = Math.max(0, workingPolls);
    this.failTerminal = failTerminal;
    this.card =
        A2AAgentCard.builder()
            .name(spec.name())
            .description("fake peer")
            .url(spec.endpointUrl() != null ? spec.endpointUrl() : "https://fake/" + spec.name())
            .version("0.0.0-fake")
            .capabilities(true, true, false)
            .addSkill(new A2AAgentSkill("echo", "Echo", "Echoes input", null, null, null, null))
            .build();
  }

  @Override
  public RemoteAgentSpec spec() {
    return spec;
  }

  @Override
  public A2AAgentCard fetchCard() {
    return card;
  }

  @Override
  public A2ATask send(A2AMessage message) {
    String taskId = UUID.randomUUID().toString();
    String contextId =
        message.getContextId() != null ? message.getContextId() : UUID.randomUUID().toString();
    long now = System.currentTimeMillis();
    A2ATask task =
        A2ATask.submitted(taskId, contextId, message, now)
            .withState(workingPolls == 0 ? terminalState() : A2ATaskState.WORKING, null, now);
    if (workingPolls == 0) {
      task = applyTerminal(task, message, now);
    }
    tasks.put(taskId, task);
    polls.put(taskId, new AtomicInteger(0));
    return task;
  }

  @Override
  public A2ATask getTask(String taskId) {
    A2ATask task = tasks.get(taskId);
    if (task == null) {
      throw new A2AClientException("unknown task " + taskId);
    }
    if (task.getState().isFinal()) {
      return task;
    }
    int n = polls.get(taskId).incrementAndGet();
    if (n >= workingPolls) {
      long now = System.currentTimeMillis();
      A2AMessage original = task.getHistory().isEmpty() ? null : task.getHistory().get(0);
      task = applyTerminal(task.withState(terminalState(), null, now), original, now);
      tasks.put(taskId, task);
    }
    return task;
  }

  @Override
  public A2ATask cancel(String taskId) {
    A2ATask task = tasks.get(taskId);
    if (task == null) {
      throw new A2AClientException("unknown task " + taskId);
    }
    A2ATask canceled = task.withState(A2ATaskState.CANCELED, "canceled", System.currentTimeMillis());
    tasks.put(taskId, canceled);
    return canceled;
  }

  private A2ATaskState terminalState() {
    return failTerminal ? A2ATaskState.FAILED : A2ATaskState.COMPLETED;
  }

  private A2ATask applyTerminal(A2ATask task, A2AMessage original, long now) {
    if (failTerminal) {
      return task.withState(A2ATaskState.FAILED, "synthetic failure", now);
    }
    String echo = original == null ? "" : original.textContent();
    List<A2APart> parts = new ArrayList<>();
    parts.add(A2APart.text("echo: " + echo));
    parts.add(A2APart.data(Map.of("length", echo.length())));
    A2AArtifact artifact =
        new A2AArtifact(UUID.randomUUID().toString(), "result", "fake result", parts, null);
    return task.withArtifact(artifact, now);
  }
}

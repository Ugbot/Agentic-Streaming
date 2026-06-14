package org.agentic.flink.a2a;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * Service-provider interface for calling a remote A2A agent.
 *
 * <p>This is the single seam between Agentic-Flink and the A2A protocol implementation. The default
 * production binding is {@code SdkA2AClient} (wraps the official {@code a2a-java} SDK client); tests
 * use an in-memory fake. Everything else in the codebase — the outbound tool, the explicit pipeline
 * step, the DSL — depends only on this interface and the {@code A2A*} value types, so swapping SDK
 * versions (or dropping the SDK entirely) is a one-class change.
 *
 * <p>A client is bound to a single {@link RemoteAgentSpec} and built on the task side from that
 * (serializable) spec, since the underlying transport/SDK objects are not {@link
 * java.io.Serializable}. Implementations need not be thread-safe; the caller scopes one client per
 * operator subtask.
 */
public interface A2AClient extends AutoCloseable {

  /** The spec this client was built from. */
  RemoteAgentSpec spec();

  /**
   * Fetch and parse the peer's Agent Card. For pinned-endpoint specs without a card URL,
   * implementations may synthesize a minimal card from the spec.
   *
   * @throws A2AClientException if the card cannot be retrieved or parsed
   */
  A2AAgentCard fetchCard();

  /**
   * Send a message ({@code message/send}) and return the resulting task as the server first reports
   * it — which may already be terminal, or {@code working} for a long-running task.
   *
   * @throws A2AClientException on transport or protocol failure
   */
  A2ATask send(A2AMessage message);

  /**
   * Fetch the current state of a task ({@code tasks/get}).
   *
   * @throws A2AClientException if the task cannot be retrieved
   */
  A2ATask getTask(String taskId);

  /**
   * Request cancellation of a task ({@code tasks/cancel}) and return its (possibly updated) state.
   *
   * @throws A2AClientException on failure
   */
  A2ATask cancel(String taskId);

  /**
   * Stream updates for a message ({@code message/stream} over SSE), invoking {@code onUpdate} for
   * each task snapshot until a {@linkplain A2ATaskState#isFinal() final} state is observed, then
   * returning the last task seen.
   *
   * <p>The default implementation degrades gracefully to {@link #sendAndAwait} for peers/clients
   * without streaming support, delivering a single final update.
   *
   * @throws A2AClientException on failure
   */
  default A2ATask stream(A2AMessage message, Consumer<A2ATask> onUpdate) {
    A2ATask terminal = sendAndAwait(message, spec().pollIntervalMs(), spec().requestTimeoutMs());
    onUpdate.accept(terminal);
    return terminal;
  }

  /**
   * Send a message and block until the task reaches a {@linkplain A2ATaskState#isFinal() final}
   * state or the timeout elapses, polling {@code tasks/get} at the given interval. This is the
   * convenience the outbound tool uses for non-streaming peers.
   *
   * @throws A2AClientException on failure or timeout
   */
  default A2ATask sendAndAwait(A2AMessage message, long pollIntervalMs, long timeoutMs) {
    A2ATask task = send(message);
    if (task.getState().isFinal()) {
      return task;
    }
    long deadline = System.currentTimeMillis() + timeoutMs;
    String taskId = task.getId();
    while (System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(Math.max(1, pollIntervalMs));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new A2AClientException("Interrupted while awaiting A2A task " + taskId, e);
      }
      task = getTask(taskId);
      if (task.getState().isFinal()) {
        return task;
      }
    }
    throw new A2AClientException(
        "A2A task " + taskId + " did not reach a final state within "
            + Duration.ofMillis(timeoutMs));
  }

  /** Release any underlying transport resources. Default no-op. */
  @Override
  default void close() {}
}

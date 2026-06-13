package org.agentic.flink.a2a.bridge;

import java.util.function.Consumer;

/**
 * Gateway-side handle on a bridge transport: publishes {@link A2ARequest}s into the Flink job and
 * delivers the {@link A2AResponse}s that come back.
 *
 * <p>Lives in the (non-Flink) Quarkus gateway process. Responses are correlated by {@code taskId};
 * register a listener with {@link #onResponse(Consumer)} (the gateway routes each to the right SSE
 * stream / push webhook) or block for a final response with {@link #awaitFinal(String, long)}.
 */
public interface A2AGatewayConnector extends AutoCloseable {

  /** Publish a request to the Flink job. */
  void publishRequest(A2ARequest request) throws Exception;

  /** Register a listener invoked for every response (intermediate and final) the job emits. */
  void onResponse(Consumer<A2AResponse> listener);

  /**
   * Block until a {@linkplain A2AResponse#isFinal() final} response for {@code taskId} arrives or
   * {@code timeoutMs} elapses.
   *
   * @return the final response, or {@code null} on timeout
   */
  A2AResponse awaitFinal(String taskId, long timeoutMs) throws InterruptedException;

  @Override
  void close();
}

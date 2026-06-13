package org.agentic.flink.a2a.gateway;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives a {@link GatewayEmitter} from the {@link A2AResponse}s a Flink job emits for one request.
 *
 * <p>The transport-agnostic, SDK-free heart of the gateway: it registers a response listener
 * (before publishing, to avoid missing fast responses), publishes the {@link A2ARequest} over the
 * bridge, translates each response into emitter calls (working → artifacts → complete / fail /
 * input-required), and blocks until the task reaches a terminal/interrupted state or times out.
 */
public final class A2ARequestBridge {
  private static final Logger LOG = LoggerFactory.getLogger(A2ARequestBridge.class);

  private final A2AGatewayConnector connector;
  private final long timeoutMs;

  public A2ARequestBridge(A2AGatewayConnector connector, long timeoutMs) {
    this.connector = connector;
    this.timeoutMs = timeoutMs;
  }

  /**
   * Publish {@code request} and pump responses into {@code emitter} until the task finishes.
   *
   * @return true if the task reached a final/interrupted state, false on timeout
   */
  public boolean run(A2ARequest request, GatewayEmitter emitter) throws InterruptedException {
    String taskId = request.getTaskId();
    CountDownLatch done = new CountDownLatch(1);

    Consumer<A2AResponse> listener =
        response -> {
          if (!taskId.equals(response.getTaskId())) {
            return;
          }
          dispatch(response, emitter, done);
        };
    connector.onResponse(listener);

    try {
      connector.publishRequest(request);
    } catch (Exception e) {
      emitter.fail("Failed to dispatch request to Flink job: " + e.getMessage());
      return true;
    }

    boolean finished = done.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!finished) {
      LOG.warn("A2A task {} timed out after {}ms", taskId, timeoutMs);
      emitter.fail("Task timed out after " + timeoutMs + "ms");
    }
    return finished;
  }

  private void dispatch(A2AResponse response, GatewayEmitter emitter, CountDownLatch done) {
    switch (response.getState()) {
      case WORKING:
        emitter.working(response.getStatusMessage());
        break;
      case COMPLETED:
        for (A2AArtifact artifact : response.getArtifacts()) {
          emitter.artifact(
              artifact.getName() == null ? "result" : artifact.getName(), artifact.getParts());
        }
        emitter.complete();
        done.countDown();
        break;
      case FAILED:
      case REJECTED:
        emitter.fail(
            response.getErrorMessage() != null
                ? response.getErrorMessage()
                : response.getStatusMessage());
        done.countDown();
        break;
      case INPUT_REQUIRED:
      case AUTH_REQUIRED:
        emitter.inputRequired(response.getStatusMessage());
        done.countDown();
        break;
      case CANCELED:
        emitter.fail("Task canceled");
        done.countDown();
        break;
      default:
        // SUBMITTED / UNKNOWN — no emitter action.
        break;
    }
  }
}

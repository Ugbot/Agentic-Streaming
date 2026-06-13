package org.agentic.flink.a2a.bridge;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Shared {@link A2AGatewayConnector} machinery: response fan-out plus a race-free {@link
 * #awaitFinal} that also catches a final response which arrived <em>before</em> the await began.
 *
 * <p>Transport subclasses implement {@link #publishRequest} and {@link #close}, and call {@link
 * #deliver} for each inbound response. {@code awaitFinal} buffers final responses by task id so the
 * common gateway pattern (publish, then block for the result) cannot miss a fast response — a real
 * ordering hazard, not just a test artifact.
 */
abstract class AbstractA2AGatewayConnector implements A2AGatewayConnector {

  private final List<Consumer<A2AResponse>> listeners = new CopyOnWriteArrayList<>();
  private final Map<String, A2AResponse> finalBuffer = new ConcurrentHashMap<>();
  private final Object lock = new Object();

  /** Feed an inbound response to listeners + the final-response buffer. Call from the transport. */
  protected final void deliver(A2AResponse response) {
    if (response.isFinal()) {
      finalBuffer.put(response.getTaskId(), response);
      synchronized (lock) {
        lock.notifyAll();
      }
    }
    for (Consumer<A2AResponse> l : listeners) {
      l.accept(response);
    }
  }

  @Override
  public final void onResponse(Consumer<A2AResponse> listener) {
    listeners.add(listener);
  }

  @Override
  public final A2AResponse awaitFinal(String taskId, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    synchronized (lock) {
      A2AResponse buffered;
      while ((buffered = finalBuffer.remove(taskId)) == null) {
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
          return null;
        }
        lock.wait(remaining);
      }
      return buffered;
    }
  }

  /** Subclasses should call this from {@link #close()} to clear listeners/buffers. */
  protected final void clearListeners() {
    listeners.clear();
    finalBuffer.clear();
    synchronized (lock) {
      lock.notifyAll();
    }
  }
}

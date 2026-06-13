package org.agentic.flink.a2a;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link A2AClient} decorator that adds resilience around an underlying client: bounded retries
 * with exponential backoff + jitter, a per-peer {@link CircuitBreaker}, and a hard deadline derived
 * from {@link RemoteAgentSpec#requestTimeoutMs()} so retries can never run longer than the caller's
 * timeout budget.
 *
 * <p>Wraps the live client built on the task side ({@code SdkA2AClient} in production, a fake in
 * tests). Because it sits behind {@link A2AClientFactory}, every outbound path — the {@link
 * A2AToolExecutor}, the {@link A2AStep}, and the banking CS client — gains resilience automatically
 * without changing call sites.
 *
 * <p><b>Idempotency note:</b> {@code send}/{@code stream} are retried on transient failure. The A2A
 * {@code message/send} carries a stable {@code messageId}, so a peer that dedupes by message id is
 * safe; a peer that does not may observe a duplicate if a failure occurs <em>after</em> it processed
 * the request but <em>before</em> the response reached us. The deadline + bounded attempts keep this
 * tightly contained, and the alternative (no retry on a pre-processing connection blip) is worse.
 */
public final class ResilientA2AClient implements A2AClient {

  private static final Logger LOG = LoggerFactory.getLogger(ResilientA2AClient.class);

  /** Injectable for tests; defaults to {@link Thread#sleep(long)}. */
  interface Sleeper {
    void sleep(long ms) throws InterruptedException;
  }

  private final A2AClient delegate;
  private final RemoteAgentSpec spec;
  private final CircuitBreaker breaker;
  private final LongSupplier clock;
  private final Sleeper sleeper;

  public ResilientA2AClient(A2AClient delegate, RemoteAgentSpec spec) {
    this(delegate, spec, System::currentTimeMillis, Thread::sleep);
  }

  ResilientA2AClient(A2AClient delegate, RemoteAgentSpec spec, LongSupplier clock, Sleeper sleeper) {
    this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
    this.spec = java.util.Objects.requireNonNull(spec, "spec");
    this.clock = clock;
    this.sleeper = sleeper;
    this.breaker = new CircuitBreaker(spec.circuitBreakerThreshold(), spec.circuitBreakerOpenMs(), clock);
  }

  @Override
  public RemoteAgentSpec spec() {
    return spec;
  }

  @Override
  public A2AAgentCard fetchCard() {
    return guarded("fetchCard", delegate::fetchCard);
  }

  @Override
  public A2ATask send(A2AMessage message) {
    return guarded("send", () -> delegate.send(message));
  }

  @Override
  public A2ATask getTask(String taskId) {
    return guarded("getTask", () -> delegate.getTask(taskId));
  }

  @Override
  public A2ATask cancel(String taskId) {
    return guarded("cancel", () -> delegate.cancel(taskId));
  }

  @Override
  public A2ATask stream(A2AMessage message, Consumer<A2ATask> onUpdate) {
    // Retried as a unit; a mid-stream failure that already pushed updates will replay them on retry.
    return guarded("stream", () -> delegate.stream(message, onUpdate));
  }

  // sendAndAwait is intentionally NOT overridden: the A2AClient default implementation polls via
  // this.send()/this.getTask(), both of which are already guarded here. So the await loop inherits
  // retry + breaker, and its own deadline (timeoutMs) bounds the overall wait.

  @Override
  public void close() {
    delegate.close();
  }

  /** Breaker state, exposed for tests/diagnostics. */
  CircuitBreaker.State breakerState() {
    return breaker.state();
  }

  /** The wrapped client, for tests/diagnostics. */
  A2AClient delegate() {
    return delegate;
  }

  /**
   * Run {@code action} under the breaker + retry policy. Fast-fails when the breaker is open;
   * otherwise retries transient {@link A2AClientException}s with exponential backoff + jitter, never
   * exceeding the per-call deadline.
   */
  private <T> T guarded(String op, Supplier<T> action) {
    if (!breaker.allowRequest()) {
      throw new A2AClientException(
          "Circuit breaker OPEN for A2A peer '" + spec.name() + "' (" + op + "); failing fast");
    }
    try {
      T result = retrying(op, action);
      breaker.recordSuccess();
      return result;
    } catch (RuntimeException e) {
      breaker.recordFailure();
      throw e;
    }
  }

  private <T> T retrying(String op, Supplier<T> action) {
    final long deadline = clock.getAsLong() + Math.max(1, spec.requestTimeoutMs());
    int attempt = 0;
    A2AClientException last = null;
    while (true) {
      try {
        return action.get();
      } catch (A2AClientException e) {
        last = e;
        if (attempt >= spec.maxRetries()) {
          break;
        }
        long now = clock.getAsLong();
        long remaining = deadline - now;
        if (remaining <= 0) {
          break; // out of time budget
        }
        // Full-jitter backoff may legitimately be 0 ("retry immediately") — that must NOT abort the
        // retry loop; only the deadline (above) or exhausted attempts end it. Clamp to the remaining
        // budget and skip the sleep when it's zero.
        long backoff = Math.max(0, Math.min(computeBackoff(attempt), remaining));
        LOG.debug(
            "A2A {} to '{}' failed (attempt {}/{}): {} — retrying in {}ms",
            op, spec.name(), attempt + 1, spec.maxRetries() + 1, e.getMessage(), backoff);
        if (backoff > 0) {
          try {
            sleeper.sleep(backoff);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new A2AClientException(
                "Interrupted retrying A2A " + op + " to '" + spec.name() + "'", ie);
          }
        }
        attempt++;
      }
    }
    throw new A2AClientException(
        "A2A " + op + " to '" + spec.name() + "' failed after " + (attempt + 1) + " attempt(s)", last);
  }

  /** Full-jitter exponential backoff: random in [0, min(maxBackoff, base * 2^attempt)]. */
  private long computeBackoff(int attempt) {
    long base = Math.max(1, spec.retryBaseBackoffMs());
    long max = Math.max(base, spec.retryMaxBackoffMs());
    long exp = base << Math.min(attempt, 30); // cap shift to avoid overflow
    long ceiling = Math.min(max, exp <= 0 ? max : exp);
    return ThreadLocalRandom.current().nextLong(ceiling + 1);
  }
}

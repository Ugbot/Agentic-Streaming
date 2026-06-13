package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.agentic.flink.a2a.ResilientA2AClient.Sleeper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link ResilientA2AClient} on a virtual clock — the injected {@link Sleeper} advances a
 * mutable clock instead of really sleeping, so backoff/deadline behavior is deterministic and fast.
 */
class ResilientA2AClientTest {

  /** Mutable virtual time: the sleeper advances it, so deadline math runs without real waiting. */
  private static final class VirtualTime {
    final AtomicLong nowMs = new AtomicLong(0);

    long now() {
      return nowMs.get();
    }

    Sleeper sleeper() {
      return ms -> nowMs.addAndGet(ms);
    }
  }

  /** Fake peer client: fails its first {@code failuresBeforeSuccess} sends, then completes. */
  private static final class FlakyClient implements A2AClient {
    private final RemoteAgentSpec spec;
    private volatile int failuresBeforeSuccess;
    private volatile boolean alwaysFail;
    final AtomicInteger sendCalls = new AtomicInteger();

    FlakyClient(RemoteAgentSpec spec, int failuresBeforeSuccess, boolean alwaysFail) {
      this.spec = spec;
      this.failuresBeforeSuccess = failuresBeforeSuccess;
      this.alwaysFail = alwaysFail;
    }

    @Override
    public RemoteAgentSpec spec() {
      return spec;
    }

    @Override
    public A2AAgentCard fetchCard() {
      throw new A2AClientException("not used");
    }

    @Override
    public A2ATask send(A2AMessage message) {
      int n = sendCalls.incrementAndGet();
      if (alwaysFail || n <= failuresBeforeSuccess) {
        throw new A2AClientException("simulated transient failure on send #" + n);
      }
      return new A2ATask(
          UUID.randomUUID().toString(),
          "ctx",
          A2ATaskState.COMPLETED,
          "ok",
          List.of(),
          List.of(),
          null,
          0L,
          0L);
    }

    @Override
    public A2ATask getTask(String taskId) {
      throw new A2AClientException("not used");
    }

    @Override
    public A2ATask cancel(String taskId) {
      throw new A2AClientException("not used");
    }

    @Override
    public A2ATask stream(A2AMessage message, Consumer<A2ATask> onUpdate) {
      A2ATask t = send(message);
      onUpdate.accept(t);
      return t;
    }
  }

  private static RemoteAgentSpec spec(int maxRetries, long baseMs, long maxMs, long timeoutMs,
      int breakerThreshold, long breakerOpenMs) {
    return RemoteAgentSpec.builder()
        .withName("peer")
        .withEndpointUrl("http://localhost:9")
        .withTransport(A2ATransport.JSONRPC)
        .withMaxRetries(maxRetries)
        .withRetryBackoff(Duration.ofMillis(baseMs), Duration.ofMillis(maxMs))
        .withRequestTimeout(Duration.ofMillis(timeoutMs))
        .withCircuitBreakerThreshold(breakerThreshold)
        .withCircuitBreakerOpen(Duration.ofMillis(breakerOpenMs))
        .build();
  }

  private static A2AMessage msg() {
    return A2AMessage.userText(UUID.randomUUID().toString(), "hi");
  }

  @Test
  @DisplayName("retries a transient failure within the deadline, then succeeds")
  void retriesThenSucceeds() {
    VirtualTime vt = new VirtualTime();
    RemoteAgentSpec spec = spec(3, 10, 100, 10_000, 0, 1000);
    FlakyClient flaky = new FlakyClient(spec, 2, false); // fail twice, then succeed
    ResilientA2AClient client = new ResilientA2AClient(flaky, spec, vt::now, vt.sleeper());

    A2ATask task = client.send(msg());

    assertNotNull(task);
    assertEquals(A2ATaskState.COMPLETED, task.getState());
    assertEquals(3, flaky.sendCalls.get(), "should take 2 failures + 1 success");
  }

  @Test
  @DisplayName("gives up after exhausting maxRetries and surfaces the last failure")
  void givesUpAfterMaxRetries() {
    VirtualTime vt = new VirtualTime();
    RemoteAgentSpec spec = spec(2, 10, 100, 10_000, 0, 1000);
    FlakyClient flaky = new FlakyClient(spec, 0, true); // always fails
    ResilientA2AClient client = new ResilientA2AClient(flaky, spec, vt::now, vt.sleeper());

    A2AClientException ex = assertThrows(A2AClientException.class, () -> client.send(msg()));

    assertEquals(3, flaky.sendCalls.get(), "1 initial + 2 retries");
    assertTrue(ex.getMessage().contains("after 3 attempt"), ex.getMessage());
    assertNotNull(ex.getCause());
  }

  @Test
  @DisplayName("stops retrying once the per-call deadline is exceeded, even below maxRetries")
  void respectsDeadline() {
    VirtualTime vt = new VirtualTime();
    // Big retry budget but a short timeout: backoff sleeps advance virtual time past the deadline.
    RemoteAgentSpec spec = spec(100, 1000, 1000, 1500, 0, 1000);
    FlakyClient flaky = new FlakyClient(spec, 0, true);
    ResilientA2AClient client = new ResilientA2AClient(flaky, spec, vt::now, vt.sleeper());

    assertThrows(A2AClientException.class, () -> client.send(msg()));

    // The 1500ms deadline must halt retries far below maxRetries=100 (each backoff advances virtual
    // time toward the deadline; full-jitter makes the exact count vary, but it can never approach 100).
    int attempts = flaky.sendCalls.get();
    assertTrue(attempts >= 2 && attempts < 100,
        "deadline must bound attempts well below maxRetries=100, was " + attempts);
    assertTrue(vt.now() <= 1500 + 1000,
        "virtual clock must not advance far past the deadline, was " + vt.now());
  }

  @Test
  @DisplayName("breaker opens after threshold failures, fast-fails, then half-opens and closes on success")
  void breakerOpensThenRecovers() {
    VirtualTime vt = new VirtualTime();
    // No retries so each call is exactly one delegate attempt; breaker trips after 2 failures.
    RemoteAgentSpec spec = spec(0, 10, 100, 10_000, 2, 1000);
    FlakyClient flaky = new FlakyClient(spec, 0, true); // failing
    ResilientA2AClient client = new ResilientA2AClient(flaky, spec, vt::now, vt.sleeper());

    assertThrows(A2AClientException.class, () -> client.send(msg())); // failure 1
    assertEquals(CircuitBreaker.State.CLOSED, client.breakerState());
    assertThrows(A2AClientException.class, () -> client.send(msg())); // failure 2 → OPEN
    assertEquals(CircuitBreaker.State.OPEN, client.breakerState());

    // While open, calls fast-fail WITHOUT touching the delegate.
    int callsBefore = flaky.sendCalls.get();
    A2AClientException open = assertThrows(A2AClientException.class, () -> client.send(msg()));
    assertTrue(open.getMessage().contains("Circuit breaker OPEN"), open.getMessage());
    assertEquals(callsBefore, flaky.sendCalls.get(), "open breaker must not call the peer");

    // Advance past the open window and let the peer recover; the half-open probe should close it.
    vt.nowMs.addAndGet(1001);
    flaky.alwaysFail = false;
    flaky.failuresBeforeSuccess = 0;
    A2ATask task = client.send(msg());
    assertEquals(A2ATaskState.COMPLETED, task.getState());
    assertEquals(CircuitBreaker.State.CLOSED, client.breakerState());
  }

  @Test
  @DisplayName("A2AClientFactory.resilient wraps clients idempotently")
  void resilientFactoryIsIdempotent() {
    RemoteAgentSpec spec = spec(1, 10, 100, 1000, 3, 1000);
    A2AClientFactory base = s -> new FlakyClient(s, 0, false);
    A2AClient once = A2AClientFactory.resilient(base).create(spec);
    assertTrue(once instanceof ResilientA2AClient);
    // Wrapping an already-resilient factory output must not double-wrap.
    A2AClientFactory wrapTwice = A2AClientFactory.resilient(A2AClientFactory.resilient(base));
    A2AClient twice = wrapTwice.create(spec);
    assertTrue(twice instanceof ResilientA2AClient);
    assertSame(
        ResilientA2AClient.class, twice.getClass(), "double resilient() must stay single-wrapped");
  }
}

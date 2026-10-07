package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Wire-level round-trip tests for {@link ZeroMqChannel} (now a native FLIP-27 {@code PollingSource}
 * via {@link ZeroMqChannel.ZmqPollFn}) + {@link ZeroMqSink}. The source side is driven directly
 * through the {@code ZmqPollFn} open/poll/close lifecycle on a worker thread (no MiniCluster); the
 * sink via its open/invoke lifecycle.
 *
 * <p>Uses an ephemeral free port to avoid collisions and the JDK's {@link ServerSocket} trick to
 * grab one without race-prone {@code tcp://*:0} parsing on the jeromq side.
 *
 * <p>No fixed sleeps: the source driver signals when its socket is open, and PUB/SUB tests close
 * the slow-joiner window by publishing warm-up frames until the subscriber has seen one. Every test
 * is bounded by a separate-thread timeout so a stuck socket fails the test instead of hanging the
 * build.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class ZeroMqChannelTest {

  private static final String WARMUP = "warmup";
  private static final long WAIT_SECONDS = 10;

  /** Allocate a free TCP port at call time. Standard test idiom. */
  private static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }

  private static ZeroMqChannel.ZmqPollFn<TestMsg> pollFn(
      ZeroMqChannel.Pattern pattern, String endpoint, boolean bind, String sub, int recvTimeoutMs) {
    ZeroMqChannel<TestMsg> ch = ZeroMqChannel.builder(pattern, endpoint, TestMsg.class).build();
    return new ZeroMqChannel.ZmqPollFn<>(
        pattern,
        endpoint,
        bind,
        sub,
        1000,
        0,
        recvTimeoutMs,
        new KafkaChannel.JsonSchema<>(TestMsg.class, ch.elementType()));
  }

  private static int randomCount() {
    return ThreadLocalRandom.current().nextInt(5, 40);
  }

  private static String randomPrefix() {
    return "m" + Long.toHexString(ThreadLocalRandom.current().nextLong()) + "-";
  }

  private static List<TestMsg> sendBatch(ZeroMqSink.ZmqWriteFn<TestMsg> sink, String prefix, int n)
      throws Exception {
    List<TestMsg> sent = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      TestMsg m = new TestMsg(prefix + i, ThreadLocalRandom.current().nextInt());
      sink.write(m);
      sent.add(m);
    }
    return sent;
  }

  private static Predicate<TestMsg> withPrefix(String prefix) {
    return m -> m.id != null && m.id.startsWith(prefix);
  }

  /**
   * Closes the PUB/SUB slow-joiner window deterministically: keep publishing warm-up frames until
   * the subscriber has received one (so the subscription has reached the publisher), then stop.
   */
  private static void primeSubscriber(
      ZeroMqSink.ZmqWriteFn<TestMsg> sink, ZmqSourceDriver<TestMsg> src) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    Predicate<TestMsg> warm = m -> WARMUP.equals(m.id);
    while (System.nanoTime() < deadline) {
      sink.write(new TestMsg(WARMUP, -1));
      if (src.awaitMatching(warm, 1, 20, TimeUnit.MILLISECONDS)) {
        return;
      }
    }
    src.rethrowFailure();
    throw new AssertionError("subscriber never received a warm-up frame");
  }

  private static void assertReceivedInOrder(
      List<TestMsg> sent, ZmqSourceDriver<TestMsg> src, String prefix) {
    List<TestMsg> got = new ArrayList<>();
    for (TestMsg m : src.collected) {
      if (withPrefix(prefix).test(m)) {
        got.add(m);
      }
    }
    assertEquals(sent.size(), got.size(), "message count for prefix " + prefix);
    for (int i = 0; i < sent.size(); i++) {
      assertEquals(sent.get(i).id, got.get(i).id, "out-of-order at " + i);
      assertEquals(sent.get(i).value, got.get(i).value, "value mismatch at " + i);
    }
  }

  @Test
  @DisplayName("PUSH/PULL: sink invokes N strings, source receives N")
  void pushPullRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;
    String prefix = randomPrefix();
    int n = randomCount();

    // PULL source binds; drive its PollFn on a worker thread.
    try (ZmqSourceDriver<TestMsg> src =
        new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.PULL, endpoint, true, "", 250))) {
      src.awaitOpen();

      // PUSH sink connects.
      ZeroMqSink.ZmqWriteFn<TestMsg> sink =
          ZeroMqSink.<TestMsg>builder(ZeroMqSink.Pattern.PUSH, endpoint).writeFn();
      sink.open(0);
      List<TestMsg> sent = sendBatch(sink, prefix, n);

      boolean ok = src.awaitMatching(withPrefix(prefix), n, WAIT_SECONDS, TimeUnit.SECONDS);
      sink.close();
      assertTrue(ok, "expected " + n + " messages, got " + src.collected.size());
      assertReceivedInOrder(sent, src, prefix);
      src.rethrowFailure();
    }
  }

  @Test
  @DisplayName("PUB/SUB: SUB receives every broadcast published after it is subscribed")
  void pubSubRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;
    String prefix = randomPrefix();
    int n = randomCount();

    ZeroMqSink.ZmqWriteFn<TestMsg> sink =
        ZeroMqSink.<TestMsg>builder(ZeroMqSink.Pattern.PUB, endpoint).topic("").writeFn();
    sink.open(0);
    try (ZmqSourceDriver<TestMsg> src =
        new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.SUB, endpoint, false, "", 250))) {
      src.awaitOpen();
      primeSubscriber(sink, src);

      List<TestMsg> sent = sendBatch(sink, prefix, n);

      boolean ok = src.awaitMatching(withPrefix(prefix), n, WAIT_SECONDS, TimeUnit.SECONDS);
      assertTrue(ok, "expected " + n + " messages, got " + src.collected.size());
      assertReceivedInOrder(sent, src, prefix);
      src.rethrowFailure();
    } finally {
      sink.close();
    }
  }

  @Test
  @DisplayName("ROUTER/DEALER: DEALER sends, ROUTER reads the payload frame")
  void routerDealerRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;
    String prefix = randomPrefix();
    int n = randomCount();

    try (ZmqSourceDriver<TestMsg> src =
        new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.ROUTER, endpoint, true, "", 250))) {
      src.awaitOpen();

      ZeroMqSink.ZmqWriteFn<TestMsg> sink =
          ZeroMqSink.<TestMsg>builder(ZeroMqSink.Pattern.DEALER, endpoint).writeFn();
      sink.open(0);
      List<TestMsg> sent = sendBatch(sink, prefix, n);

      boolean ok = src.awaitMatching(withPrefix(prefix), n, WAIT_SECONDS, TimeUnit.SECONDS);
      sink.close();
      assertTrue(ok, "expected " + n + " messages, got " + src.collected.size());
      assertReceivedInOrder(sent, src, prefix);
      src.rethrowFailure();
    }
  }

  @Test
  @DisplayName("XSUB↔XPUB proxy forwards from a PUB-side to a SUB-side and stops cleanly")
  void xsubXpubProxyForwards() throws Exception {
    int pubPort = freePort();
    int subPort = freePort();
    String pubFront = "tcp://127.0.0.1:" + pubPort; // publishers connect here (XSUB)
    String subBack = "tcp://127.0.0.1:" + subPort; // subscribers connect here (XPUB)
    String prefix = randomPrefix();
    int n = randomCount();

    ZeroMqProxy proxy = ZeroMqProxy.pubSubProxy(pubFront, subBack);
    try {
      assertTrue(proxy.isRunning(), "proxy thread running after construction");

      try (ZmqSourceDriver<TestMsg> src =
          new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.SUB, subBack, false, "", 250))) {
        src.awaitOpen();

        // Publisher connects to the front end of the proxy (so it must NOT bind).
        ZeroMqSink.ZmqWriteFn<TestMsg> sink =
            ZeroMqSink.<TestMsg>builder(ZeroMqSink.Pattern.PUB, pubFront).bind(false).writeFn();
        sink.open(0);
        try {
          // The subscription travels SUB -> XPUB -> XSUB -> PUB; warm up until it has arrived.
          primeSubscriber(sink, src);

          List<TestMsg> sent = sendBatch(sink, prefix, n);

          boolean ok = src.awaitMatching(withPrefix(prefix), n, WAIT_SECONDS, TimeUnit.SECONDS);
          assertTrue(ok, "expected " + n + " proxied messages, got " + src.collected.size());
          assertReceivedInOrder(sent, src, prefix);
          src.rethrowFailure();
        } finally {
          sink.close();
        }
      }
    } finally {
      proxy.close();
    }
    assertFalse(proxy.isRunning(), "proxy thread exited after close()");
  }

  @Test
  @DisplayName("proxy: stop() returns promptly and is idempotent, with or without traffic")
  void proxyStopIsBounded() throws Exception {
    String front = "tcp://127.0.0.1:" + freePort();
    String back = "tcp://127.0.0.1:" + freePort();
    ZeroMqProxy proxy = ZeroMqProxy.routerDealerProxy(front, back);
    assertTrue(proxy.isRunning());
    long start = System.nanoTime();
    proxy.stop();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(proxy.awaitTermination(1, TimeUnit.SECONDS), "proxy thread terminated");
    assertFalse(proxy.isRunning());
    assertTrue(elapsedMs < 5_000, "stop() took " + elapsedMs + " ms");
    proxy.stop();
    proxy.close();
    assertFalse(proxy.isRunning());
  }

  // ---- helpers ----

  /** Public POJO so Jackson can deserialize without special access. */
  public static final class TestMsg {
    public String id;
    public int value;

    public TestMsg() {}

    public TestMsg(String id, int value) {
      this.id = id;
      this.value = value;
    }
  }

  /**
   * Drives a {@link ZeroMqChannel.ZmqPollFn} on a daemon thread: opens on that thread, loops {@code
   * poll()} into a thread-safe queue, and closes on the same thread (ZMQ thread-affinity). Any
   * throwable from the poll loop is captured and surfaced through {@link #rethrowFailure()}.
   */
  static final class ZmqSourceDriver<T> implements AutoCloseable {
    final ConcurrentLinkedQueue<T> collected = new ConcurrentLinkedQueue<>();
    private final ZeroMqChannel.ZmqPollFn<T> fn;
    private final Thread thread;
    private final CountDownLatch opened = new CountDownLatch(1);
    private volatile boolean running = true;
    private volatile Throwable failure;

    ZmqSourceDriver(ZeroMqChannel.ZmqPollFn<T> fn) {
      this.fn = fn;
      this.thread = new Thread(this::run, "zmq-test-src");
      this.thread.setDaemon(true);
      this.thread.start();
    }

    private void run() {
      try {
        fn.open(0);
        opened.countDown();
        while (running) {
          T m = fn.poll(250);
          if (m != null) {
            collected.add(m);
          }
        }
      } catch (Throwable t) {
        failure = t;
      } finally {
        opened.countDown();
        try {
          fn.close();
        } catch (Throwable t) {
          if (failure == null) {
            failure = t;
          }
        }
      }
    }

    /** Blocks until the socket is open (bound or connected) on the driver thread. */
    void awaitOpen() throws InterruptedException {
      assertTrue(opened.await(WAIT_SECONDS, TimeUnit.SECONDS), "source did not open in time");
      rethrowFailure();
    }

    /** True once at least {@code n} collected messages satisfy {@code p}, false on timeout. */
    boolean awaitMatching(Predicate<T> p, int n, long timeout, TimeUnit unit)
        throws InterruptedException {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      while (true) {
        if (countMatching(p) >= n) {
          return true;
        }
        if (failure != null || System.nanoTime() >= deadline) {
          return countMatching(p) >= n;
        }
        Thread.sleep(10);
      }
    }

    private int countMatching(Predicate<T> p) {
      int c = 0;
      for (T m : collected) {
        if (p.test(m)) {
          c++;
        }
      }
      return c;
    }

    /** Fails the test with the driver thread's throwable, if any. */
    void rethrowFailure() {
      Throwable t = failure;
      if (t != null) {
        throw new AssertionError("zmq source driver failed: " + t, t);
      }
    }

    @Override
    public void close() {
      running = false;
      try {
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      assertFalse(thread.isAlive(), "zmq source driver thread did not stop");
      assertNull(failure, "zmq source driver failed: " + failure);
    }
  }

  static {
    assertNotNull(ZeroMqChannel.class, "guard");
  }
}

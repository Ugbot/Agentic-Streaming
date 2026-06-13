package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Wire-level round-trip tests for {@link ZeroMqChannel} (now a native FLIP-27 {@code PollingSource}
 * via {@link ZeroMqChannel.ZmqPollFn}) + {@link ZeroMqSink}. The source side is driven directly
 * through the {@code ZmqPollFn} open/poll/close lifecycle on a worker thread (no MiniCluster); the
 * sink via its open/invoke lifecycle.
 *
 * <p>Uses an ephemeral free port to avoid collisions and the JDK's {@link ServerSocket} trick to
 * grab one without race-prone {@code tcp://*:0} parsing on the jeromq side.
 */
final class ZeroMqChannelTest {

  /** Allocate a free TCP port at call time. Standard test idiom. */
  private static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }

  private static ZeroMqChannel.ZmqPollFn<TestMsg> pollFn(
      ZeroMqChannel.Pattern pattern, String endpoint, boolean bind, String sub, int recvTimeoutMs) {
    ZeroMqChannel<TestMsg> ch =
        ZeroMqChannel.builder(pattern, endpoint, TestMsg.class).build();
    return new ZeroMqChannel.ZmqPollFn<>(
        pattern, endpoint, bind, sub, 1000, 0, recvTimeoutMs,
        new KafkaChannel.JsonSchema<>(TestMsg.class, ch.elementType()));
  }

  @Test
  @DisplayName("PUSH/PULL: sink invokes N strings, source receives N")
  void pushPullRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;

    // PULL source binds; drive its PollFn on a worker thread.
    try (ZmqSourceDriver<TestMsg> src =
        new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.PULL, endpoint, true, "", 250))) {

      // PUSH sink connects.
      ZeroMqSink<TestMsg> sink = ZeroMqSink.push(endpoint);
      sink.open(null);
      int n = 25;
      for (int i = 0; i < n; i++) {
        sink.invoke(new TestMsg("m-" + i, i), null);
      }

      boolean ok = src.awaitCount(n, 5, TimeUnit.SECONDS);
      sink.close();
      assertTrue(ok, "expected " + n + " messages, got " + src.collected.size());
      List<TestMsg> got = new ArrayList<>(src.collected);
      for (int i = 0; i < n; i++) {
        assertEquals("m-" + i, got.get(i).id, "out-of-order at " + i);
        assertEquals(i, got.get(i).value, "value mismatch at " + i);
      }
    }
  }

  @Test
  @DisplayName("PUB/SUB: SUB receives broadcast after PUB binds + small settle")
  void pubSubRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;

    ZeroMqSink<TestMsg> sink = ZeroMqSink.pub(endpoint, ""); // empty topic = no prefix frame
    sink.open(null);
    Thread.sleep(100); // let the bind settle

    try (ZmqSourceDriver<TestMsg> src =
        new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.SUB, endpoint, false, "", 250))) {
      Thread.sleep(300); // settle the subscription (PUB/SUB slow-joiner window)

      int n = 20;
      for (int i = 0; i < n; i++) {
        sink.invoke(new TestMsg("pub-" + i, i), null);
      }

      boolean ok = src.awaitCount(n, 5, TimeUnit.SECONDS);
      sink.close();
      assertTrue(ok, "expected " + n + " messages, got " + src.collected.size());
    }
  }

  @Test
  @DisplayName("ROUTER/DEALER: DEALER sends, ROUTER reads the payload frame")
  void routerDealerRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;

    try (ZmqSourceDriver<TestMsg> src =
        new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.ROUTER, endpoint, true, "", 250))) {
      Thread.sleep(100);

      ZeroMqSink<TestMsg> sink = ZeroMqSink.dealer(endpoint);
      sink.open(null);
      int n = 10;
      for (int i = 0; i < n; i++) {
        sink.invoke(new TestMsg("rd-" + i, i), null);
      }

      boolean ok = src.awaitCount(n, 5, TimeUnit.SECONDS);
      sink.close();
      assertTrue(ok, "expected " + n + " messages, got " + src.collected.size());
    }
  }

  @Test
  @DisplayName("XSUB↔XPUB proxy forwards from a PUB-side to a SUB-side")
  void xsubXpubProxyForwards() throws Exception {
    int pubPort = freePort();
    int subPort = freePort();
    String pubFront = "tcp://127.0.0.1:" + pubPort; // publishers connect here (XSUB)
    String subBack = "tcp://127.0.0.1:" + subPort; // subscribers connect here (XPUB)

    try (ZeroMqProxy proxy = ZeroMqProxy.pubSubProxy(pubFront, subBack)) {
      Thread.sleep(150);

      try (ZmqSourceDriver<TestMsg> src =
          new ZmqSourceDriver<>(pollFn(ZeroMqChannel.Pattern.SUB, subBack, false, "", 250))) {
        Thread.sleep(250);

        // Publisher connects to the front end of the proxy (so it must NOT bind).
        ZeroMqSink<TestMsg> sink =
            ZeroMqSink.<TestMsg>builder(ZeroMqSink.Pattern.PUB, pubFront).bind(false).build();
        sink.open(null);
        Thread.sleep(150);

        int n = 12;
        for (int i = 0; i < n; i++) {
          sink.invoke(new TestMsg("proxied-" + i, i), null);
        }

        boolean ok = src.awaitCount(n, 5, TimeUnit.SECONDS);
        sink.close();
        assertTrue(ok, "expected " + n + " proxied messages, got " + src.collected.size());
      }
    }
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
   * Drives a {@link ZeroMqChannel.ZmqPollFn} on a daemon thread: opens on that thread, loops
   * {@code poll()} into a thread-safe queue, and closes on the same thread (ZMQ thread-affinity).
   */
  static final class ZmqSourceDriver<T> implements AutoCloseable {
    final ConcurrentLinkedQueue<T> collected = new ConcurrentLinkedQueue<>();
    private final ZeroMqChannel.ZmqPollFn<T> fn;
    private final Thread thread;
    private volatile boolean running = true;

    ZmqSourceDriver(ZeroMqChannel.ZmqPollFn<T> fn) {
      this.fn = fn;
      this.thread = new Thread(this::run, "zmq-test-src");
      this.thread.setDaemon(true);
      this.thread.start();
    }

    private void run() {
      try {
        fn.open(0);
        while (running) {
          T m = fn.poll(250);
          if (m != null) {
            collected.add(m);
          }
        }
      } catch (Exception ignored) {
        // test thread; surfaced via awaitCount failing
      } finally {
        try {
          fn.close();
        } catch (Exception ignored) {
          // best-effort
        }
      }
    }

    boolean awaitCount(int n, long timeout, TimeUnit unit) throws InterruptedException {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      while (System.nanoTime() < deadline) {
        if (collected.size() >= n) {
          return true;
        }
        Thread.sleep(20);
      }
      return collected.size() >= n;
    }

    @Override
    public void close() {
      running = false;
      thread.interrupt();
      try {
        thread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  static {
    assertNotNull(ZeroMqChannel.class, "guard");
  }
}

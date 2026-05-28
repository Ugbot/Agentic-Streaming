package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Wire-level round-trip tests for {@link ZeroMqChannel} + {@link ZeroMqSink}. The Source is
 * driven directly (without a Flink MiniCluster) against a capturing context; the Sink is driven
 * via its {@link ZeroMqSink#open}/{@link ZeroMqSink#invoke} lifecycle.
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

  @Test
  @DisplayName("PUSH/PULL: sink invokes N strings, source receives N")
  void pushPullRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;

    // PULL source binds. Drive it on a worker thread; collect into a queue via a capturing ctx.
    ZeroMqChannel<TestMsg> ch = ZeroMqChannel.pull(endpoint, TestMsg.class);
    // Build the inner Source by re-constructing with the same parameters via reflection? No —
    // we expose package-private fields, so just open it in a controlled way:
    final ZeroMqChannel.Source<TestMsg> src =
        new ZeroMqChannel.Source<>(
            ZeroMqChannel.Pattern.PULL,
            endpoint,
            true,
            "",
            1000,
            0,
            250,
            new KafkaChannel.JsonSchema<>(TestMsg.class, ch.elementType()));

    CapturingCtx<TestMsg> ctx = new CapturingCtx<>();
    Thread runner =
        new Thread(
            () -> {
              try {
                src.run(ctx);
              } catch (Exception e) {
                ctx.fail(e);
              }
            },
            "zmq-test-pull");
    runner.setDaemon(true);
    runner.start();

    // PUSH sink connects.
    ZeroMqSink<TestMsg> sink = ZeroMqSink.push(endpoint);
    sink.open(new Configuration());

    int n = 25;
    for (int i = 0; i < n; i++) {
      sink.invoke(new TestMsg("m-" + i, i), null);
    }

    boolean ok = ctx.awaitCount(n, 5, TimeUnit.SECONDS);
    src.cancel();
    runner.join(2000);
    sink.close();

    assertTrue(ok, "expected " + n + " messages, got " + ctx.collected.size());
    List<TestMsg> got = new ArrayList<>(ctx.collected);
    for (int i = 0; i < n; i++) {
      assertEquals("m-" + i, got.get(i).id, "out-of-order at " + i);
      assertEquals(i, got.get(i).value, "value mismatch at " + i);
    }
  }

  @Test
  @DisplayName("PUB/SUB: SUB receives broadcast after PUB binds + small settle")
  void pubSubRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;

    ZeroMqSink<TestMsg> sink = ZeroMqSink.pub(endpoint, ""); // empty topic = no prefix frame
    sink.open(new Configuration());

    // Give the bind a moment, then connect a SUB and let it settle (PUB/SUB has a slow-joiner
    // window where early messages are dropped).
    Thread.sleep(100);

    ZeroMqChannel<TestMsg> ch = ZeroMqChannel.sub(endpoint, TestMsg.class, "");
    final ZeroMqChannel.Source<TestMsg> src =
        new ZeroMqChannel.Source<>(
            ZeroMqChannel.Pattern.SUB,
            endpoint,
            false,
            "",
            1000,
            0,
            250,
            new KafkaChannel.JsonSchema<>(TestMsg.class, ch.elementType()));

    CapturingCtx<TestMsg> ctx = new CapturingCtx<>();
    Thread runner =
        new Thread(
            () -> {
              try {
                src.run(ctx);
              } catch (Exception e) {
                ctx.fail(e);
              }
            },
            "zmq-test-sub");
    runner.setDaemon(true);
    runner.start();

    // Settle the subscription.
    Thread.sleep(300);

    int n = 20;
    for (int i = 0; i < n; i++) {
      sink.invoke(new TestMsg("pub-" + i, i), null);
    }

    boolean ok = ctx.awaitCount(n, 5, TimeUnit.SECONDS);
    src.cancel();
    runner.join(2000);
    sink.close();

    assertTrue(ok, "expected " + n + " messages, got " + ctx.collected.size());
  }

  @Test
  @DisplayName("ROUTER/DEALER: DEALER sends, ROUTER reads the payload frame")
  void routerDealerRoundTrip() throws Exception {
    int port = freePort();
    String endpoint = "tcp://127.0.0.1:" + port;

    // ROUTER source binds.
    ZeroMqChannel<TestMsg> ch = ZeroMqChannel.router(endpoint, TestMsg.class);
    final ZeroMqChannel.Source<TestMsg> src =
        new ZeroMqChannel.Source<>(
            ZeroMqChannel.Pattern.ROUTER,
            endpoint,
            true,
            "",
            1000,
            0,
            250,
            new KafkaChannel.JsonSchema<>(TestMsg.class, ch.elementType()));
    CapturingCtx<TestMsg> ctx = new CapturingCtx<>();
    Thread runner =
        new Thread(
            () -> {
              try {
                src.run(ctx);
              } catch (Exception e) {
                ctx.fail(e);
              }
            },
            "zmq-test-router");
    runner.setDaemon(true);
    runner.start();

    Thread.sleep(100);

    // DEALER sink connects.
    ZeroMqSink<TestMsg> sink = ZeroMqSink.dealer(endpoint);
    sink.open(new Configuration());

    int n = 10;
    for (int i = 0; i < n; i++) {
      sink.invoke(new TestMsg("rd-" + i, i), null);
    }

    boolean ok = ctx.awaitCount(n, 5, TimeUnit.SECONDS);
    src.cancel();
    runner.join(2000);
    sink.close();

    assertTrue(ok, "expected " + n + " messages, got " + ctx.collected.size());
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

      // Subscriber connects to the back end of the proxy.
      ZeroMqChannel<TestMsg> ch = ZeroMqChannel.sub(subBack, TestMsg.class, "");
      final ZeroMqChannel.Source<TestMsg> src =
          new ZeroMqChannel.Source<>(
              ZeroMqChannel.Pattern.SUB,
              subBack,
              false,
              "",
              1000,
              0,
              250,
              new KafkaChannel.JsonSchema<>(TestMsg.class, ch.elementType()));

      CapturingCtx<TestMsg> ctx = new CapturingCtx<>();
      Thread runner =
          new Thread(
              () -> {
                try {
                  src.run(ctx);
                } catch (Exception e) {
                  ctx.fail(e);
                }
              },
              "zmq-test-proxy-sub");
      runner.setDaemon(true);
      runner.start();
      Thread.sleep(250);

      // Publisher connects to the front end of the proxy (so it must NOT bind).
      ZeroMqSink<TestMsg> sink =
          ZeroMqSink.<TestMsg>builder(ZeroMqSink.Pattern.PUB, pubFront).bind(false).build();
      sink.open(new Configuration());
      Thread.sleep(150);

      int n = 12;
      for (int i = 0; i < n; i++) {
        sink.invoke(new TestMsg("proxied-" + i, i), null);
      }

      boolean ok = ctx.awaitCount(n, 5, TimeUnit.SECONDS);
      src.cancel();
      runner.join(2000);
      sink.close();

      assertTrue(ok, "expected " + n + " proxied messages, got " + ctx.collected.size());
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

  /** Minimal capturing SourceContext for unit tests. Thread-safe collection. */
  static final class CapturingCtx<T> implements SourceFunction.SourceContext<T> {
    final ConcurrentLinkedQueue<T> collected = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private volatile Throwable failure;
    private final Object lock = new Object();
    private CountDownLatch latch;
    private int target;

    boolean awaitCount(int n, long timeout, TimeUnit unit) throws InterruptedException {
      synchronized (lock) {
        target = n;
        latch = new CountDownLatch(Math.max(0, n - collected.size()));
      }
      return latch.await(timeout, unit);
    }

    void fail(Throwable t) {
      failure = t;
      failed.set(true);
    }

    @Override
    public void collect(T element) {
      collected.add(element);
      synchronized (lock) {
        if (latch != null && collected.size() <= target) {
          latch.countDown();
        }
      }
    }

    @Override
    public void collectWithTimestamp(T element, long timestamp) {
      collect(element);
    }

    @Override
    public void emitWatermark(Watermark mark) {}

    @Override
    public void markAsTemporarilyIdle() {}

    @Override
    public Object getCheckpointLock() {
      return lock;
    }

    @Override
    public void close() {}
  }

  // Avoid an "unused import" complaint if StreamRecord ever gets pulled in.
  @SuppressWarnings("unused")
  private static StreamRecord<Object> never() {
    return null;
  }

  static {
    assertNotNull(ZeroMqChannel.class, "guard");
  }
}

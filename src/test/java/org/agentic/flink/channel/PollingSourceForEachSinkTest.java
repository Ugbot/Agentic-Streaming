package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.channel.sink.ForEachSink;
import org.agentic.flink.channel.source.PollingSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Proves the generic native-2.2 connector bases — {@link PollingSource} (FLIP-27) and
 * {@link ForEachSink} (FLIP-143) — move records end-to-end on a real MiniCluster, independent of any
 * specific transport (ZMQ, Redis, …). A {@link PollingSource.PollFn} emits a bounded run of integers
 * then idles; a {@link ForEachSink.WriteFn} collects them.
 */
final class PollingSourceForEachSinkTest {

  /** Sink-side collector (static so it survives the per-subtask WriteFn lifecycle). */
  static final ConcurrentLinkedQueue<Integer> SINK = new ConcurrentLinkedQueue<>();

  private JobClient job;

  @AfterEach
  void tearDown() throws Exception {
    if (job != null) {
      job.cancel().get();
      job = null;
    }
    SINK.clear();
  }

  @Test
  @DisplayName("PollingSource(PollFn) -> sinkTo(ForEachSink(WriteFn)) delivers every emitted record")
  void endToEnd() throws Exception {
    int n = 50;
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());

    PollingSource<Integer> source = new PollingSource<>(new CountingPollFn(n));
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "counting-source", TypeInformation.of(Integer.class))
        .sinkTo(new ForEachSink<>(new CollectingWriteFn()))
        .name("collect-sink");

    job = env.executeAsync("polling-source-foreach-sink");

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (System.nanoTime() < deadline && SINK.size() < n) {
      Thread.sleep(50);
    }
    assertTrue(SINK.size() >= n, "expected >= " + n + " records, got " + SINK.size());
    // No duplicates lost or doubled for the first n distinct values.
    for (int i = 0; i < n; i++) {
      assertTrue(SINK.contains(i), "missing record " + i);
    }
  }

  /** Emits 0..count-1 once, then returns null forever (idle but unbounded). */
  static final class CountingPollFn implements PollingSource.PollFn<Integer> {
    private static final long serialVersionUID = 1L;
    private final int count;
    private transient AtomicInteger next;

    CountingPollFn(int count) {
      this.count = count;
    }

    @Override
    public void open(int subtaskIndex) {
      next = new AtomicInteger(0);
    }

    @Override
    public Integer poll(long timeoutMs) throws InterruptedException {
      int v = next.getAndIncrement();
      if (v < count) {
        return v;
      }
      // Idle: respect the timeout so the reader can shut down promptly.
      Thread.sleep(Math.min(timeoutMs, 50));
      return null;
    }
  }

  static final class CollectingWriteFn implements ForEachSink.WriteFn<Integer> {
    private static final long serialVersionUID = 1L;

    @Override
    public void write(Integer element) {
      SINK.add(element);
    }
  }
}

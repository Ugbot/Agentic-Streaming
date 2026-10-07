package org.agentic.flink.channel.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.agentic.flink.channel.sink.ForEachSink;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link PollingSource} recovery: the split position survives a savepoint and restore, and the
 * resumed reader neither re-emits nor skips records. The poll target is an in-JVM counter with a
 * monotonic cursor; the sink is an in-JVM list. Both are static maps keyed by a per-test id because
 * MiniCluster tasks run in the test JVM.
 */
final class PollingSourceRecoveryTest {

  /** Every record the sink received, in order, per test id. */
  static final Map<String, List<Long>> SINK = new ConcurrentHashMap<>();

  /** Every position the poll fn was asked to seek to, per test id. */
  static final Map<String, List<String>> SEEKS = new ConcurrentHashMap<>();

  private static MiniCluster cluster;

  private JobClient job;

  @BeforeAll
  static void startCluster() throws Exception {
    cluster = TestClusters.start(2);
  }

  @AfterAll
  static void stopCluster() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @AfterEach
  void cancelJob() throws Exception {
    if (job != null) {
      try {
        job.cancel().get(30, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // already finished
      }
      job = null;
    }
  }

  @Test
  @DisplayName(
      "stop-with-savepoint then restore resumes after the last emitted record: no gaps, no repeats")
  void savepointRestoreResumesWithoutRedeliveryOrSkips(@TempDir Path dir) throws Exception {
    String id = UUID.randomUUID().toString();
    SINK.put(id, new CopyOnWriteArrayList<>());
    SEEKS.put(id, new CopyOnWriteArrayList<>());
    int firstBatch = 40 + ThreadLocalRandom.current().nextInt(60);
    int secondBatch = 40 + ThreadLocalRandom.current().nextInt(60);

    job = start(id, dir, null);
    awaitSinkSize(id, firstBatch);
    String savepoint =
        job.stopWithSavepoint(
                false, dir.resolve("sp").toUri().toString(), SavepointFormatType.CANONICAL)
            .get(60, TimeUnit.SECONDS);
    job = null;

    List<Long> beforeRestore = new ArrayList<>(SINK.get(id));
    assertTrue(beforeRestore.size() >= firstBatch);
    assertContiguousFromZero(beforeRestore);
    long lastEmitted = beforeRestore.get(beforeRestore.size() - 1);
    // The target ran ahead of the sink (records were queued but never emitted); the split must not
    // have recorded those positions.
    assertEquals(List.of("<start>"), SEEKS.get(id));

    job = start(id, dir, savepoint);
    awaitSinkSize(id, beforeRestore.size() + secondBatch);

    List<Long> all = new ArrayList<>(SINK.get(id));
    assertContiguousFromZero(all);
    assertEquals(
        List.of("<start>", Long.toString(lastEmitted)),
        SEEKS.get(id),
        "restored reader must seek to the last emitted position");
  }

  @Test
  @DisplayName("split serializer round-trips positions and reads version 1 (positionless) splits")
  void splitSerializerVersions() throws Exception {
    String position = "cursor-" + ThreadLocalRandom.current().nextLong();
    PollingSource.PollingSplit split = new PollingSource.PollingSplit(position);
    byte[] bytes = PollingSource.PollingSplit.SERIALIZER.serialize(split);
    assertEquals(2, PollingSource.PollingSplit.SERIALIZER.getVersion());
    assertEquals(split, PollingSource.PollingSplit.SERIALIZER.deserialize(2, bytes));
    assertEquals(position, PollingSource.PollingSplit.SERIALIZER.deserialize(2, bytes).position());

    PollingSource.PollingSplit none = new PollingSource.PollingSplit(null);
    assertNull(
        PollingSource.PollingSplit.SERIALIZER
            .deserialize(2, PollingSource.PollingSplit.SERIALIZER.serialize(none))
            .position());
    assertNull(PollingSource.PollingSplit.SERIALIZER.deserialize(1, new byte[0]).position());
  }

  @Test
  @DisplayName("enumerator checkpoint keeps an unassigned split's position and reads version 1")
  void enumeratorStateSerializerVersions() throws Exception {
    String position = "cursor-" + ThreadLocalRandom.current().nextLong();
    PollingSource.EnumeratorState pending =
        new PollingSource.EnumeratorState(new PollingSource.PollingSplit(position));
    byte[] bytes = PollingSource.EnumeratorState.SERIALIZER.serialize(pending);
    PollingSource.EnumeratorState back =
        PollingSource.EnumeratorState.SERIALIZER.deserialize(2, bytes);
    assertEquals(pending, back);
    assertEquals(position, back.pending().position());

    PollingSource.EnumeratorState assigned =
        PollingSource.EnumeratorState.SERIALIZER.deserialize(
            2,
            PollingSource.EnumeratorState.SERIALIZER.serialize(
                PollingSource.EnumeratorState.ASSIGNED));
    assertTrue(assigned.assigned());

    assertTrue(PollingSource.EnumeratorState.SERIALIZER.deserialize(1, new byte[] {0}).assigned());
    PollingSource.EnumeratorState v1Unassigned =
        PollingSource.EnumeratorState.SERIALIZER.deserialize(1, new byte[] {1});
    assertTrue(!v1Unassigned.assigned());
    assertNull(v1Unassigned.pending().position());
  }

  private JobClient start(String id, Path dir, String savepoint) throws Exception {
    Configuration conf = new Configuration();
    conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, dir.resolve("cp").toUri().toString());
    if (savepoint != null) {
      conf.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
    }
    StreamExecutionEnvironment env =
        new TestStreamEnvironment(cluster, conf, 1, List.of(), List.of());
    env.setParallelism(1);
    env.fromSource(
            new PollingSource<>(new CounterPollFn(id), 8),
            WatermarkStrategy.noWatermarks(),
            "counter",
            TypeInformation.of(Long.class))
        .sinkTo(new ForEachSink<>(new SlowCollectingWriteFn(id)))
        .name("collect");
    JobClient client = env.executeAsync("polling-recovery-" + id);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (client.getJobStatus().get() != JobStatus.RUNNING) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("job did not reach RUNNING");
      }
      Thread.sleep(20);
    }
    return client;
  }

  private static void awaitSinkSize(String id, int size) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (SINK.get(id).size() < size) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("sink has " + SINK.get(id).size() + " records, wanted " + size);
      }
      Thread.sleep(10);
    }
  }

  private static void assertContiguousFromZero(List<Long> records) {
    Map<Long, Integer> counts = new HashMap<>();
    for (Long r : records) {
      counts.merge(r, 1, Integer::sum);
    }
    for (long i = 0; i < records.size(); i++) {
      assertEquals(
          1,
          counts.getOrDefault(i, 0).intValue(),
          "record " + i + " delivered " + counts.getOrDefault(i, 0) + " times in " + records);
    }
  }

  /**
   * Fake poll target with a monotonic cursor: an ever-growing counter. {@code seek} restarts after
   * the given value; {@code position} is the last value returned. Produces faster than the sink
   * consumes so the reader queue is never empty at snapshot time.
   */
  static final class CounterPollFn implements PollingSource.PositionedPollFn<Long> {
    private static final long serialVersionUID = 1L;
    private final String id;
    private transient AtomicLong next;
    private transient String position;

    CounterPollFn(String id) {
      this.id = id;
    }

    @Override
    public void open(int subtaskIndex) {
      next = new AtomicLong();
      position = null;
    }

    @Override
    public void seek(String restored) {
      SEEKS.get(id).add(restored == null ? "<start>" : restored);
      next.set(restored == null ? 0L : Long.parseLong(restored) + 1);
      position = null;
    }

    @Override
    public String position() {
      return position;
    }

    @Override
    public Long poll(long timeoutMs) {
      long v = next.getAndIncrement();
      position = Long.toString(v);
      return v;
    }
  }

  /** Sink that takes a few milliseconds per record so the source queue stays populated. */
  static final class SlowCollectingWriteFn implements ForEachSink.WriteFn<Long> {
    private static final long serialVersionUID = 1L;
    private final String id;

    SlowCollectingWriteFn(String id) {
      this.id = id;
    }

    @Override
    public void write(Long element) throws InterruptedException {
      Thread.sleep(2);
      SINK.get(id).add(element);
    }
  }
}

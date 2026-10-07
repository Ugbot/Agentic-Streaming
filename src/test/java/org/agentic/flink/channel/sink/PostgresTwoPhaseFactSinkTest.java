package org.agentic.flink.channel.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.agentic.flink.channel.source.PollingSource;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.agentic.flink.storage.postgres.PostgresTestDatabase;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link PostgresTwoPhaseFactSink} on a MiniCluster against a real PostgreSQL (Testcontainers).
 * Checkpoints are only taken when the test triggers them (the interval is one hour), so the test
 * controls exactly when the committer may write.
 */
@Tag("integration")
final class PostgresTwoPhaseFactSinkTest {

  /** Facts the writer received, per test id (counted in the fact mapper). */
  static final Map<String, AtomicInteger> MAPPED = new ConcurrentHashMap<>();

  /** When set, {@link FailingSnapshotMap} throws in snapshotState, failing the checkpoint. */
  static final Map<String, AtomicBoolean> FAIL_SNAPSHOT = new ConcurrentHashMap<>();

  /** Checkpoint ids the map operator saw complete, per test id. */
  static final Map<String, List<Long>> COMPLETED = new ConcurrentHashMap<>();

  private static PostgresTestDatabase database;
  private static MiniCluster cluster;
  private JobClient job;

  @BeforeAll
  static void start() throws Exception {
    database = PostgresTestDatabase.start();
    cluster = TestClusters.start(4);
  }

  @AfterAll
  static void stop() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    if (database != null) {
      database.close();
    }
  }

  @AfterEach
  void cancelJob() {
    if (job != null) {
      try {
        job.cancel().get(30, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // already gone
      }
      job = null;
    }
  }

  @Test
  @DisplayName(
      "a failed checkpoint writes nothing; the next completed checkpoint commits every fact exactly once")
  void failedCheckpointLeavesNoPartialWrite(@TempDir Path dir) throws Exception {
    String id = UUID.randomUUID().toString();
    String flow = "flow-" + id;
    MAPPED.put(id, new AtomicInteger());
    FAIL_SNAPSHOT.put(id, new AtomicBoolean(false));
    COMPLETED.put(id, new ArrayList<>());

    Configuration conf = new Configuration();
    conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofHours(1));
    conf.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, 10);
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, dir.resolve("cp").toUri().toString());
    conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
    StreamExecutionEnvironment env =
        new TestStreamEnvironment(cluster, conf, 1, List.of(), List.of());
    env.setParallelism(1);
    env.fromSource(
            new PollingSource<>(new CounterPollFn(), 16),
            WatermarkStrategy.noWatermarks(),
            "counter",
            TypeInformation.of(Long.class))
        .map(new FailingSnapshotMap(id))
        .sinkTo(
            new PostgresTwoPhaseFactSink<>(
                database.jdbcUrl(), user(), password(), new CountingFactMapper(id, flow)));
    job = env.executeAsync("two-phase-" + id);
    awaitRunning(job);

    int minBeforeFailure = 20 + ThreadLocalRandom.current().nextInt(30);
    await(() -> MAPPED.get(id).get() >= minBeforeFailure, "writer received " + minBeforeFailure);

    // Checkpoint 1: the map operator's snapshot throws. The checkpoint fails and the job restarts
    // from scratch (there is no completed checkpoint yet). The writer had buffered dozens of facts
    // but none of them may have reached the database.
    FAIL_SNAPSHOT.get(id).set(true);
    CompletableFuture<String> failed = cluster.triggerCheckpoint(job.getJobID());
    assertThrows(ExecutionException.class, () -> failed.get(60, TimeUnit.SECONDS));
    FAIL_SNAPSHOT.get(id).set(false);
    int mappedAtFailedCheckpoint = MAPPED.get(id).get();
    assertTrue(mappedAtFailedCheckpoint >= minBeforeFailure);
    assertEquals(0, countFacts(flow), "no fact of the failed checkpoint may be visible");
    assertEquals(0, countCommits(), "no commit marker may be written for a failed checkpoint");
    awaitRunning(job);
    assertEquals(0, countFacts(flow), "still nothing visible after the restart");

    // Checkpoint 2 completes: whatever the restarted writer buffered up to the barrier becomes
    // visible atomically and exactly once, starting again from value 0 because the source replayed.
    await(
        () -> MAPPED.get(id).get() >= mappedAtFailedCheckpoint + minBeforeFailure,
        "restarted writer received facts");
    cluster.triggerCheckpoint(job.getJobID()).get(60, TimeUnit.SECONDS);
    await(() -> countFacts(flow) >= 1, "facts committed after checkpoint 2");
    await(() -> COMPLETED.get(id).size() >= 1, "map operator notified of completion");

    List<Long> visible = factValues(flow);
    assertEquals(1, countCommits(), "exactly one batch committed");
    assertTrue(visible.size() >= 1);
    assertEquals(visible.size(), sumCommittedFactCount(), "commit markers account for every row");
    assertContiguousFromZero(visible);
  }

  @Test
  @DisplayName("re-committing the same batch after a restore is idempotent")
  void recommitIsIdempotent() throws Exception {
    String flow = "flow-" + UUID.randomUUID();
    int before = countCommits();
    PostgresTwoPhaseFactSink.PostgresCommitter committer =
        new PostgresTwoPhaseFactSink.PostgresCommitter(database.jdbcUrl(), user(), password());
    committer.ensureTables();
    List<FactBatch.Fact> facts = new ArrayList<>();
    int n = 2 + ThreadLocalRandom.current().nextInt(10);
    for (int i = 0; i < n; i++) {
      facts.add(
          new FactBatch.Fact(
              flow, "fact-" + i, "{\"content\":\"" + UUID.randomUUID() + "\"}", 1000L * i));
    }
    FactBatch batch = new FactBatch(UUID.randomUUID().toString(), facts);

    RecordingRequest first = new RecordingRequest(batch);
    committer.commit(List.of(first));
    assertFalse(first.alreadyCommitted.get());
    assertEquals(n, countFacts(flow));
    assertEquals(before + 1, countCommits());

    RecordingRequest second = new RecordingRequest(batch);
    committer.commit(List.of(second));
    assertTrue(second.alreadyCommitted.get(), "second commit of the same batch is skipped");
    assertFalse(second.failed.get());
    assertEquals(n, countFacts(flow));
    assertEquals(before + 1, countCommits());
    committer.close();
  }

  // --- helpers --------------------------------------------------------------------------------

  private static void awaitRunning(JobClient client) throws Exception {
    await(
        () -> {
          try {
            return client.getJobStatus().get() == JobStatus.RUNNING;
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        "job RUNNING");
  }

  private interface Condition {
    boolean test() throws Exception;
  }

  private static void await(Condition condition, String what) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.test()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(20);
    }
  }

  private static void assertContiguousFromZero(List<Long> values) {
    List<Long> sorted = new ArrayList<>(values);
    sorted.sort(Long::compareTo);
    for (int i = 0; i < sorted.size(); i++) {
      assertEquals(
          (long) i, sorted.get(i), "visible values must be 0..n-1 without gaps: " + sorted);
    }
  }

  private static int countFacts(String flow) throws Exception {
    try (Connection c = connect();
        PreparedStatement ps =
            c.prepareStatement("SELECT COUNT(*) FROM agent_facts WHERE flow_id = ?")) {
      ps.setString(1, flow);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static List<Long> factValues(String flow) throws Exception {
    List<Long> values = new ArrayList<>();
    try (Connection c = connect();
        PreparedStatement ps =
            c.prepareStatement("SELECT fact_json FROM agent_facts WHERE flow_id = ?")) {
      ps.setString(1, flow);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          values.add(CountingFactMapper.valueOf(rs.getString(1)));
        }
      }
    }
    return values;
  }

  private static int countCommits() throws Exception {
    try (Connection c = connect();
        PreparedStatement ps =
            c.prepareStatement("SELECT COUNT(*) FROM " + PostgresTwoPhaseFactSink.COMMITS_TABLE);
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getInt(1);
    }
  }

  private static int sumCommittedFactCount() throws Exception {
    try (Connection c = connect();
        PreparedStatement ps =
            c.prepareStatement(
                "SELECT COALESCE(SUM(fact_count), 0) FROM "
                    + PostgresTwoPhaseFactSink.COMMITS_TABLE);
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getInt(1);
    }
  }

  private static Connection connect() throws Exception {
    return DriverManager.getConnection(database.jdbcUrl(), user(), password());
  }

  private static String user() {
    return database.storeConfig().get("postgres.user");
  }

  private static String password() {
    return database.storeConfig().get("postgres.password");
  }

  /** Unbounded monotonic counter that replays from the restored position. */
  static final class CounterPollFn implements PollingSource.PositionedPollFn<Long> {
    private static final long serialVersionUID = 1L;
    private transient AtomicLong next;
    private transient String position;

    @Override
    public void open(int subtaskIndex) {
      next = new AtomicLong();
    }

    @Override
    public void seek(String restored) {
      next.set(restored == null ? 0L : Long.parseLong(restored) + 1);
      position = null;
    }

    @Override
    public String position() {
      return position;
    }

    @Override
    public Long poll(long timeoutMs) throws InterruptedException {
      Thread.sleep(2);
      long v = next.getAndIncrement();
      position = Long.toString(v);
      return v;
    }
  }

  /** Identity map whose snapshot fails while the test's flag is set. */
  static final class FailingSnapshotMap
      implements MapFunction<Long, Long>, CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 1L;
    private final String id;

    FailingSnapshotMap(String id) {
      this.id = id;
    }

    @Override
    public Long map(Long value) {
      return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
      if (FAIL_SNAPSHOT.get(id).get()) {
        throw new IllegalStateException(
            "injected snapshot failure for checkpoint " + context.getCheckpointId());
      }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      COMPLETED.get(id).add(checkpointId);
    }
  }

  static final class CountingFactMapper implements PostgresTwoPhaseFactSink.FactMapper<Long> {
    private static final long serialVersionUID = 1L;
    private static final String PREFIX = "{\"value\":";
    private final String id;
    private final String flow;

    CountingFactMapper(String id, String flow) {
      this.id = id;
      this.flow = flow;
    }

    @Override
    public FactBatch.Fact map(Long element) {
      MAPPED.get(id).incrementAndGet();
      return new FactBatch.Fact(flow, "fact-" + element, PREFIX + element + "}", element);
    }

    static long valueOf(String json) {
      return Long.parseLong(json.substring(PREFIX.length(), json.length() - 1));
    }
  }

  static final class RecordingRequest implements Committer.CommitRequest<FactBatch> {
    final FactBatch batch;
    final AtomicBoolean alreadyCommitted = new AtomicBoolean();
    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicBoolean retry = new AtomicBoolean();

    RecordingRequest(FactBatch batch) {
      this.batch = batch;
    }

    @Override
    public FactBatch getCommittable() {
      return batch;
    }

    @Override
    public int getNumberOfRetries() {
      return 0;
    }

    @Override
    public void signalFailedWithKnownReason(Throwable t) {
      failed.set(true);
    }

    @Override
    public void signalFailedWithUnknownReason(Throwable t) {
      failed.set(true);
    }

    @Override
    public void retryLater() {
      retry.set(true);
    }

    @Override
    public void updateAndRetryLater(FactBatch committable) {
      retry.set(true);
    }

    @Override
    public void signalAlreadyCommitted() {
      alreadyCommitted.set(true);
    }
  }
}

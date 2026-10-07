package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.agentic.flink.testkit.FlussTestCluster;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for "Fluss logs between stages": a Flink stage writes records to a Fluss table
 * via {@link FlussSink} (native FLIP-143), and a second stage reads them back by tailing the table
 * log via {@link FlussChannel}'s native {@link FlussChannel.FlussLogPollFn} (FLIP-27). Proves the
 * durable stage→Fluss→stage boundary round-trips on Flink 2.2.
 *
 * <p>Runs under {@code ./mvnw verify -P integration-tests}. The cluster comes from {@link
 * FlussTestCluster}: {@code FLUSS_BOOTSTRAP_SERVERS} if set (for example the compose stack in
 * {@code docker-compose-fluss.yml}), otherwise Testcontainers on Podman. An unreachable cluster
 * fails the test.
 */
@Tag("integration")
class FlussChannelIT {

  private static FlussTestCluster cluster;
  private static String bootstrap;

  @BeforeAll
  static void startCluster() {
    cluster = FlussTestCluster.start();
    bootstrap = cluster.bootstrapServers();
  }

  @AfterAll
  static void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  /** Public POJO so Jackson round-trips it through the Fluss payload column. */
  public static final class Rec {
    public String id;
    public int value;

    public Rec() {}

    public Rec(String id, int value) {
      this.id = id;
      this.value = value;
    }
  }

  @Test
  @DisplayName("stage -> FlussSink -> FlussChannel(log tail) -> stage round-trips every record")
  void stageToFlussToStage() throws Exception {
    String db = "agentic_it";
    String table = "log_" + UUID.randomUUID().toString().replace("-", "");
    int n = 30;

    // --- write stage: a bounded job emits N records into the Fluss table ---
    StreamExecutionEnvironment writeEnv =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
    Rec[] recs = new Rec[n];
    for (int i = 0; i < n; i++) {
      recs[i] = new Rec("r-" + i, i);
    }
    writeEnv
        .fromElements(recs)
        .sinkTo(
            FlussSink.of(bootstrap, db, table, (FlussSink.SerializableKeySelector<Rec>) r -> r.id))
        .setParallelism(1);
    writeEnv.execute("fluss-write-stage");

    // --- read stage: tail the table log via the native poll fn, collect until N (or timeout) ---
    FlussChannel.FlussLogPollFn<Rec> poll =
        new FlussChannel.FlussLogPollFn<>(bootstrap, db, table, 1, Rec.class, true);
    Set<String> seen = new HashSet<>();
    int maxValue = -1;
    try {
      poll.open(0);
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
      while (seen.size() < n && System.nanoTime() < deadline) {
        Rec r = poll.poll(500);
        if (r != null) {
          seen.add(r.id);
          maxValue = Math.max(maxValue, r.value);
        }
      }
    } finally {
      poll.close();
    }

    assertTrue(
        seen.size() >= n,
        "expected " + n + " records tailed from the Fluss log, got " + seen.size());
    for (int i = 0; i < n; i++) {
      assertTrue(seen.contains("r-" + i), "missing record r-" + i + " in the Fluss log");
    }
    assertTrue(
        maxValue == n - 1, "payload values must survive the round trip; maxValue=" + maxValue);
  }
}

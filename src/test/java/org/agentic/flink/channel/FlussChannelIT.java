package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * <p>Runs only under {@code -P integration-tests}; bring the cluster up first:
 *
 * <pre>
 *   podman network create agentic-flink-network   # once
 *   podman compose -f docker-compose-fluss.yml up -d
 *   mvn test -P integration-tests -Dtest=FlussChannelIT
 * </pre>
 *
 * Bootstrap defaults to {@code localhost:9123}; self-skips if the cluster is unreachable.
 */
@Tag("integration")
class FlussChannelIT {

  private static String bootstrap;

  @BeforeAll
  static void requireCluster() {
    bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123");
    String[] hp = bootstrap.split(",")[0].split(":");
    boolean up;
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 2000);
      up = true;
    } catch (Exception e) {
      up = false;
    }
    assumeTrue(up, "Fluss not reachable at " + bootstrap + " — skipping");
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
        .sinkTo(FlussSink.of(bootstrap, db, table, (FlussSink.SerializableKeySelector<Rec>) r -> r.id))
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

    assertTrue(seen.size() >= n, "expected " + n + " records tailed from the Fluss log, got " + seen.size());
    for (int i = 0; i < n; i++) {
      assertTrue(seen.contains("r-" + i), "missing record r-" + i + " in the Fluss log");
    }
    assertTrue(maxValue == n - 1, "payload values must survive the round trip; maxValue=" + maxValue);
  }

  /** Quiet "unused" guard for the imports used only when the cluster is present. */
  @SuppressWarnings("unused")
  private static List<TypeInformation<?>> never() {
    return null;
  }
}

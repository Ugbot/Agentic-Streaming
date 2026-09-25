package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class FlinkJobDefaultsTest {

  private static AgenticFlinkConfig config(Map<String, String> props) {
    return AgenticFlinkConfig.fromMap(props);
  }

  @Test
  void defaultsAreExactlyOnceWithRetainedCheckpointsOnHashmap() {
    FlinkJobDefaults d = FlinkJobDefaults.fromConfig(AgenticFlinkConfig.forTesting());

    assertTrue(d.isEnabled());
    assertEquals(Duration.ofMillis(10_000), d.getInterval());
    assertEquals(Duration.ofMillis(1_000), d.getMinPause());
    assertEquals(Duration.ofMillis(600_000), d.getTimeout());
    assertEquals(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION, d.getRetention());
    assertEquals(FlinkJobDefaults.BACKEND_HASHMAP, d.getStateBackend());
    assertNull(d.getStorageDir());
  }

  @Test
  void applyConfiguresEnvironmentCheckpointing() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    long interval = rnd.nextLong(500, 60_000);
    long minPause = rnd.nextLong(0, interval);
    long timeout = rnd.nextLong(interval, 900_000);
    String dir = "file:///tmp/agentic-test-" + rnd.nextInt(1_000_000);
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_INTERVAL_MS, Long.toString(interval));
    props.put(ConfigKeys.CHECKPOINT_MIN_PAUSE_MS, Long.toString(minPause));
    props.put(ConfigKeys.CHECKPOINT_TIMEOUT_MS, Long.toString(timeout));
    props.put(ConfigKeys.CHECKPOINT_RETENTION, "delete");
    props.put(ConfigKeys.CHECKPOINT_STORAGE_DIR, dir);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkJobDefaults.fromConfig(config(props)).apply(env);

    CheckpointConfig cp = env.getCheckpointConfig();
    assertTrue(cp.isCheckpointingEnabled());
    assertEquals(CheckpointingMode.EXACTLY_ONCE, cp.getCheckpointingConsistencyMode());
    assertEquals(interval, cp.getCheckpointInterval());
    assertEquals(minPause, cp.getMinPauseBetweenCheckpoints());
    assertEquals(timeout, cp.getCheckpointTimeout());
    assertEquals(
        ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION,
        cp.getExternalizedCheckpointRetention());
    assertEquals(dir, env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
    assertEquals(
        FlinkJobDefaults.BACKEND_HASHMAP,
        env.getConfiguration().get(StateBackendOptions.STATE_BACKEND));
  }

  @Test
  void applyWithoutStorageDirFallsBackToLocalTmp() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkJobDefaults.fromConfig(AgenticFlinkConfig.forTesting()).apply(env);

    String dir = env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
    assertTrue(dir.startsWith("file:"), dir);
    assertTrue(dir.contains("agentic-flink"), dir);
  }

  @Test
  void applyKeepsCheckpointDirectoryAlreadyOnEnvironment() {
    String preset = "file:///tmp/preset-" + ThreadLocalRandom.current().nextInt(1_000_000);
    Configuration conf = new Configuration();
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, preset);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

    FlinkJobDefaults.fromConfig(AgenticFlinkConfig.forTesting()).apply(env);

    assertEquals(preset, env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
  }

  @Test
  void explicitOptOutLeavesCheckpointingOff() {
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_ENABLED, "false");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    FlinkJobDefaults d = FlinkJobDefaults.fromConfig(config(props));
    d.apply(env);

    assertFalse(d.isEnabled());
    assertFalse(env.getCheckpointConfig().isCheckpointingEnabled());
    assertFalse(d.toConfiguration().contains(CheckpointingOptions.CHECKPOINTING_INTERVAL));
  }

  @Test
  void malformedEnabledFlagIsRejectedInsteadOfSilentlyDisabling() {
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_ENABLED, "off");
    assertThrows(IllegalArgumentException.class, () -> FlinkJobDefaults.fromConfig(config(props)));
  }

  @Test
  void rejectsNonPositiveIntervalAndTimeoutAndNegativePause() {
    for (String key :
        new String[] {ConfigKeys.CHECKPOINT_INTERVAL_MS, ConfigKeys.CHECKPOINT_TIMEOUT_MS}) {
      Map<String, String> props = new HashMap<>();
      props.put(key, "0");
      assertThrows(
          IllegalArgumentException.class, () -> FlinkJobDefaults.fromConfig(config(props)), key);
    }
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_MIN_PAUSE_MS, "-1");
    assertThrows(IllegalArgumentException.class, () -> FlinkJobDefaults.fromConfig(config(props)));
    Map<String, String> notANumber = new HashMap<>();
    notANumber.put(ConfigKeys.CHECKPOINT_INTERVAL_MS, "ten");
    assertThrows(
        IllegalArgumentException.class, () -> FlinkJobDefaults.fromConfig(config(notANumber)));
  }

  @Test
  void retentionNamesMapToFlinkEnum() {
    assertEquals(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION,
        FlinkJobDefaults.parseRetention("RETAIN"));
    assertEquals(
        ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION,
        FlinkJobDefaults.parseRetention(" delete "));
    assertEquals(
        ExternalizedCheckpointRetention.NO_EXTERNALIZED_CHECKPOINTS,
        FlinkJobDefaults.parseRetention("none"));
    assertThrows(IllegalArgumentException.class, () -> FlinkJobDefaults.parseRetention("keep"));
  }

  @Test
  void unknownStateBackendIsRejected() {
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_STATE_BACKEND, "leveldb");
    assertThrows(IllegalArgumentException.class, () -> FlinkJobDefaults.fromConfig(config(props)));
  }

  @Test
  void rocksDbBackendResolvesWhenArtifactPresentOtherwiseNamesIt() {
    boolean present;
    try {
      Class.forName(FlinkJobDefaults.ROCKSDB_FACTORY);
      present = true;
    } catch (ClassNotFoundException e) {
      present = false;
    }
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_STATE_BACKEND, "RocksDB");
    if (present) {
      FlinkJobDefaults d = FlinkJobDefaults.fromConfig(config(props));
      assertEquals(FlinkJobDefaults.BACKEND_ROCKSDB, d.getStateBackend());
      assertEquals(
          FlinkJobDefaults.BACKEND_ROCKSDB,
          d.toConfiguration().get(StateBackendOptions.STATE_BACKEND));
    } else {
      IllegalStateException e =
          assertThrows(
              IllegalStateException.class, () -> FlinkJobDefaults.fromConfig(config(props)));
      assertTrue(e.getMessage().contains(FlinkJobDefaults.ROCKSDB_ARTIFACT), e.getMessage());
    }
  }

  @Test
  void forstBackendWithoutArtifactNamesTheArtifact() {
    boolean present;
    try {
      Class.forName(FlinkJobDefaults.FORST_FACTORY);
      present = true;
    } catch (ClassNotFoundException e) {
      present = false;
    }
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_STATE_BACKEND, "forst");
    if (present) {
      assertEquals(
          FlinkJobDefaults.BACKEND_FORST, FlinkJobDefaults.fromConfig(config(props)).getStateBackend());
    } else {
      IllegalStateException e =
          assertThrows(
              IllegalStateException.class, () -> FlinkJobDefaults.fromConfig(config(props)));
      assertTrue(e.getMessage().contains(FlinkJobDefaults.FORST_ARTIFACT), e.getMessage());
    }
  }

  @Test
  void withIntervalAndStorageDirProduceCopies() {
    FlinkJobDefaults base = FlinkJobDefaults.fromConfig(AgenticFlinkConfig.forTesting());
    Duration interval = Duration.ofMillis(ThreadLocalRandom.current().nextLong(1, 100_000));
    String dir = "file:///tmp/copy-" + ThreadLocalRandom.current().nextInt(1_000_000);

    FlinkJobDefaults copy = base.withInterval(interval).withStorageDir(dir);

    assertEquals(interval, copy.getInterval());
    assertEquals(dir, copy.getStorageDir());
    assertEquals(Duration.ofMillis(10_000), base.getInterval());
    assertNull(base.getStorageDir());
    assertEquals(dir, copy.toConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
    assertThrows(IllegalArgumentException.class, () -> base.withInterval(Duration.ZERO));
  }

  @Test
  void agentJobCarriesDefaultsIntoGenerator() {
    Map<String, String> props = new HashMap<>();
    long interval = ThreadLocalRandom.current().nextLong(1_000, 50_000);
    props.put(ConfigKeys.CHECKPOINT_INTERVAL_MS, Long.toString(interval));
    AgentJob job =
        AgentJob.builder()
            .withId("job-" + ThreadLocalRandom.current().nextInt(1_000_000))
            .withAgent(
                org.agentic.flink.dsl.Agent.builder()
                    .withId("a")
                    .withSystemPrompt("s")
                    .withStateMachine(
                        org.agentic.flink.execution.AgentExecutorTest.stateMachine())
                    .build())
            .withAgenticFlinkConfig(config(props))
            .build();
    assertEquals(Duration.ofMillis(interval), job.getJobDefaults().getInterval());

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    new AgentJobGenerator(env, job);

    assertTrue(env.getCheckpointConfig().isCheckpointingEnabled());
    assertEquals(interval, env.getCheckpointConfig().getCheckpointInterval());
    assertEquals(
        CheckpointingMode.EXACTLY_ONCE,
        env.getCheckpointConfig().getCheckpointingConsistencyMode());
  }
}

package org.agentic.flink.config;

import org.agentic.flink.annotation.Public;

/**
 * Constants class defining all configuration keys and their default values for the Agentic Flink
 * framework.
 *
 * <p>Configuration keys follow a dot-separated naming convention (e.g., {@code
 * checkpoint.interval.ms}). When resolved via environment variables, keys are transformed by
 * uppercasing, replacing dots with underscores, and prepending {@code AGENTIC_FLINK_} (e.g., {@code
 * AGENTIC_FLINK_CHECKPOINT_INTERVAL_MS}).
 *
 * @author Agentic Flink Team
 * @see AgenticFlinkConfig
 */
@Public
public final class ConfigKeys {

  private ConfigKeys() {}

  // Checkpointing and state backend, applied by FlinkJobDefaults to every job the framework
  // builds. Durations are milliseconds. Checkpointing is on unless checkpoint.enabled=false.
  public static final String CHECKPOINT_ENABLED = "checkpoint.enabled";
  public static final String CHECKPOINT_INTERVAL_MS = "checkpoint.interval.ms";
  public static final String CHECKPOINT_MIN_PAUSE_MS = "checkpoint.min.pause.ms";
  public static final String CHECKPOINT_TIMEOUT_MS = "checkpoint.timeout.ms";
  public static final String CHECKPOINT_RETENTION = "checkpoint.retention";
  public static final String CHECKPOINT_STORAGE_DIR = "checkpoint.storage.dir";
  public static final String CHECKPOINT_STATE_BACKEND = "checkpoint.state.backend";
  public static final String CHECKPOINT_STATE_BACKEND_INCREMENTAL =
      "checkpoint.state.backend.incremental";
  public static final String DEFAULT_CHECKPOINT_ENABLED = "true";
  public static final String DEFAULT_CHECKPOINT_INTERVAL_MS = "10000";
  public static final String DEFAULT_CHECKPOINT_MIN_PAUSE_MS = "1000";
  public static final String DEFAULT_CHECKPOINT_TIMEOUT_MS = "600000";

  /** {@code retain} keeps externalized checkpoints on cancellation, {@code delete} removes them. */
  public static final String DEFAULT_CHECKPOINT_RETENTION = "retain";

  /**
   * {@code hashmap} (heap) or {@code rocksdb} (needs flink-statebackend-rocksdb on the classpath).
   */
  public static final String DEFAULT_CHECKPOINT_STATE_BACKEND = "hashmap";

  public static final String DEFAULT_CHECKPOINT_STATE_BACKEND_INCREMENTAL = "true";
}

package org.agentic.flink.job;

import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fault tolerance defaults applied to every {@link StreamExecutionEnvironment} the framework
 * builds a job on: exactly-once checkpointing with a configurable interval, minimum pause,
 * timeout and externalized checkpoint retention, plus the state backend and checkpoint storage
 * location.
 *
 * <p>Values come from {@link AgenticFlinkConfig} under the {@code checkpoint.*} keys
 * ({@code AGENTIC_FLINK_CHECKPOINT_*} in the environment). Checkpointing is enabled by default;
 * the only way to run without it is the explicit opt-out {@code checkpoint.enabled=false}, which
 * is logged at WARN level so a job never runs unprotected by accident.
 *
 * <p>State backends: {@code hashmap} (default, heap) works out of the box. {@code rocksdb}
 * requires {@code org.apache.flink:flink-statebackend-rocksdb:2.2.1} on the job classpath and
 * {@code forst} requires {@code org.apache.flink:flink-statebackend-forst:2.2.1}; when the
 * artifact is missing {@link #fromConfig} fails with a message naming it rather than silently
 * falling back to the heap backend.
 *
 * <p>Checkpoint storage: {@code checkpoint.storage.dir} sets the externalized checkpoint
 * directory. When it is not configured and the environment does not already carry
 * {@code execution.checkpointing.dir}, {@link #apply} uses {@code
 * ${java.io.tmpdir}/agentic-flink/checkpoints} and logs the location, which is enough for
 * local runs and recovery on the same host. Production deployments should point it at durable
 * shared storage.
 */
public final class FlinkJobDefaults implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobDefaults.class);

  public static final String BACKEND_HASHMAP = "hashmap";
  public static final String BACKEND_ROCKSDB = "rocksdb";
  public static final String BACKEND_FORST = "forst";
  public static final String RETENTION_RETAIN = "retain";
  public static final String RETENTION_DELETE = "delete";
  public static final String RETENTION_NONE = "none";

  static final String ROCKSDB_FACTORY = "org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackendFactory";
  static final String ROCKSDB_ARTIFACT = "org.apache.flink:flink-statebackend-rocksdb:2.2.1";
  static final String FORST_FACTORY = "org.apache.flink.state.forst.ForStStateBackendFactory";
  static final String FORST_ARTIFACT = "org.apache.flink:flink-statebackend-forst:2.2.1";

  private final boolean enabled;
  private final Duration interval;
  private final Duration minPause;
  private final Duration timeout;
  private final ExternalizedCheckpointRetention retention;
  private final String stateBackend;
  private final boolean incremental;
  private final String storageDir;

  private FlinkJobDefaults(
      boolean enabled,
      Duration interval,
      Duration minPause,
      Duration timeout,
      ExternalizedCheckpointRetention retention,
      String stateBackend,
      boolean incremental,
      String storageDir) {
    this.enabled = enabled;
    this.interval = interval;
    this.minPause = minPause;
    this.timeout = timeout;
    this.retention = retention;
    this.stateBackend = stateBackend;
    this.incremental = incremental;
    this.storageDir = storageDir;
  }

  /** Defaults resolved from the process environment ({@link AgenticFlinkConfig#fromEnvironment}). */
  public static FlinkJobDefaults fromEnvironment() {
    return fromConfig(AgenticFlinkConfig.fromEnvironment());
  }

  /**
   * Resolves and validates the checkpoint settings in {@code config}.
   *
   * @throws IllegalArgumentException when a value is malformed or out of range
   * @throws IllegalStateException when the selected state backend artifact is not on the
   *     classpath
   */
  public static FlinkJobDefaults fromConfig(AgenticFlinkConfig config) {
    Objects.requireNonNull(config, "config");
    boolean enabled = config.getBoolean(ConfigKeys.CHECKPOINT_ENABLED, true);
    Duration interval =
        positive(
            ConfigKeys.CHECKPOINT_INTERVAL_MS,
            config.getLong(
                ConfigKeys.CHECKPOINT_INTERVAL_MS,
                Long.parseLong(ConfigKeys.DEFAULT_CHECKPOINT_INTERVAL_MS)));
    Duration minPause =
        nonNegative(
            ConfigKeys.CHECKPOINT_MIN_PAUSE_MS,
            config.getLong(
                ConfigKeys.CHECKPOINT_MIN_PAUSE_MS,
                Long.parseLong(ConfigKeys.DEFAULT_CHECKPOINT_MIN_PAUSE_MS)));
    Duration timeout =
        positive(
            ConfigKeys.CHECKPOINT_TIMEOUT_MS,
            config.getLong(
                ConfigKeys.CHECKPOINT_TIMEOUT_MS,
                Long.parseLong(ConfigKeys.DEFAULT_CHECKPOINT_TIMEOUT_MS)));
    ExternalizedCheckpointRetention retention =
        parseRetention(
            config.get(ConfigKeys.CHECKPOINT_RETENTION, ConfigKeys.DEFAULT_CHECKPOINT_RETENTION));
    String backend =
        resolveStateBackend(
            config.get(
                ConfigKeys.CHECKPOINT_STATE_BACKEND, ConfigKeys.DEFAULT_CHECKPOINT_STATE_BACKEND));
    boolean incremental = config.getBoolean(ConfigKeys.CHECKPOINT_STATE_BACKEND_INCREMENTAL, true);
    String storageDir = blankToNull(config.get(ConfigKeys.CHECKPOINT_STORAGE_DIR));
    return new FlinkJobDefaults(
        enabled, interval, minPause, timeout, retention, backend, incremental, storageDir);
  }

  /** A copy with a different checkpoint interval (positive). */
  public FlinkJobDefaults withInterval(Duration newInterval) {
    Objects.requireNonNull(newInterval, "interval");
    positive(ConfigKeys.CHECKPOINT_INTERVAL_MS, newInterval.toMillis());
    return new FlinkJobDefaults(
        enabled, newInterval, minPause, timeout, retention, stateBackend, incremental, storageDir);
  }

  /** A copy with a different checkpoint directory ({@code null} restores the default location). */
  public FlinkJobDefaults withStorageDir(String newStorageDir) {
    return new FlinkJobDefaults(
        enabled,
        interval,
        minPause,
        timeout,
        retention,
        stateBackend,
        incremental,
        blankToNull(newStorageDir));
  }

  /**
   * Applies these defaults to {@code env}. Explicit settings already present on the environment
   * for the checkpoint directory are kept; everything else is set from this instance.
   *
   * @return {@code env} for chaining
   */
  public StreamExecutionEnvironment apply(StreamExecutionEnvironment env) {
    Objects.requireNonNull(env, "env");
    Configuration conf = new Configuration();
    conf.set(StateBackendOptions.STATE_BACKEND, stateBackend);
    conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);
    if (!enabled) {
      LOG.warn(
          "Checkpointing is DISABLED by {}=false; state is lost on failure and sources restart "
              + "from their initial position",
          ConfigKeys.CHECKPOINT_ENABLED);
      env.configure(conf);
      return env;
    }
    String dir = storageDir;
    if (dir == null) {
      dir = env.getConfiguration().getOptional(CheckpointingOptions.CHECKPOINTS_DIRECTORY).orElse(null);
    }
    if (dir == null) {
      dir = defaultStorageDir();
      LOG.info(
          "{} not set; externalized checkpoints go to {} (set the key to durable shared storage "
              + "for production)",
          ConfigKeys.CHECKPOINT_STORAGE_DIR,
          dir);
    }
    conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, dir);
    conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, interval);
    conf.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, minPause);
    conf.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, timeout);
    conf.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
    conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION, retention);
    env.configure(conf);

    CheckpointConfig cp = env.getCheckpointConfig();
    cp.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
    cp.setCheckpointInterval(interval.toMillis());
    cp.setMinPauseBetweenCheckpoints(minPause.toMillis());
    cp.setCheckpointTimeout(timeout.toMillis());
    cp.setExternalizedCheckpointRetention(retention);
    LOG.info(
        "Checkpointing enabled: exactly-once every {} ms (min pause {} ms, timeout {} ms), "
            + "retention {}, state backend {}, storage {}",
        interval.toMillis(),
        minPause.toMillis(),
        timeout.toMillis(),
        retention,
        stateBackend,
        dir);
    return env;
  }

  /**
   * The same settings as a {@link Configuration}, for callers that create the environment with
   * {@link StreamExecutionEnvironment#createLocalEnvironment(int, Configuration)} and want the
   * cluster side configured too. {@link #apply} is still required for the pipeline side.
   */
  public Configuration toConfiguration() {
    Configuration conf = new Configuration();
    conf.set(StateBackendOptions.STATE_BACKEND, stateBackend);
    conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);
    if (enabled) {
      conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
      conf.set(
          CheckpointingOptions.CHECKPOINTS_DIRECTORY,
          storageDir != null ? storageDir : defaultStorageDir());
      conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, interval);
      conf.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, minPause);
      conf.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, timeout);
      conf.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
      conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION, retention);
    }
    return conf;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Duration getInterval() {
    return interval;
  }

  public Duration getMinPause() {
    return minPause;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public ExternalizedCheckpointRetention getRetention() {
    return retention;
  }

  public String getStateBackend() {
    return stateBackend;
  }

  public boolean isIncremental() {
    return incremental;
  }

  /** Configured checkpoint directory, or {@code null} when the default location is used. */
  public String getStorageDir() {
    return storageDir;
  }

  /**
   * Validates a state backend name and checks that its factory is loadable.
   *
   * @return the canonical backend name understood by Flink
   */
  public static String resolveStateBackend(String name) {
    String normalized = name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case BACKEND_HASHMAP:
        return BACKEND_HASHMAP;
      case BACKEND_ROCKSDB:
        requireOnClasspath(BACKEND_ROCKSDB, ROCKSDB_FACTORY, ROCKSDB_ARTIFACT);
        return BACKEND_ROCKSDB;
      case BACKEND_FORST:
        requireOnClasspath(BACKEND_FORST, FORST_FACTORY, FORST_ARTIFACT);
        return BACKEND_FORST;
      default:
        throw new IllegalArgumentException(
            ConfigKeys.CHECKPOINT_STATE_BACKEND
                + " must be one of hashmap, rocksdb, forst; got '"
                + name
                + "'");
    }
  }

  static ExternalizedCheckpointRetention parseRetention(String value) {
    String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case RETENTION_RETAIN:
        return ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION;
      case RETENTION_DELETE:
        return ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION;
      case RETENTION_NONE:
        return ExternalizedCheckpointRetention.NO_EXTERNALIZED_CHECKPOINTS;
      default:
        throw new IllegalArgumentException(
            ConfigKeys.CHECKPOINT_RETENTION
                + " must be one of retain, delete, none; got '"
                + value
                + "'");
    }
  }

  static String defaultStorageDir() {
    return Paths.get(System.getProperty("java.io.tmpdir"), "agentic-flink", "checkpoints")
        .toUri()
        .toString();
  }

  private static void requireOnClasspath(String backend, String factoryClass, String artifact) {
    try {
      Class.forName(factoryClass, false, FlinkJobDefaults.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          ConfigKeys.CHECKPOINT_STATE_BACKEND
              + "="
              + backend
              + " requires "
              + artifact
              + " on the job classpath ("
              + factoryClass
              + " not found)",
          e);
    }
  }

  private static Duration positive(String key, long millis) {
    if (millis <= 0) {
      throw new IllegalArgumentException(key + " must be positive, got " + millis);
    }
    return Duration.ofMillis(millis);
  }

  private static Duration nonNegative(String key, long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException(key + " must not be negative, got " + millis);
    }
    return Duration.ofMillis(millis);
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }

  @Override
  public String toString() {
    return "FlinkJobDefaults{enabled="
        + enabled
        + ", interval="
        + interval
        + ", minPause="
        + minPause
        + ", timeout="
        + timeout
        + ", retention="
        + retention
        + ", stateBackend="
        + stateBackend
        + ", incremental="
        + incremental
        + ", storageDir="
        + storageDir
        + '}';
  }
}

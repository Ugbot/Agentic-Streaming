package org.agentic.flink.memory;

import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Serializable factory for {@link ShortTermMemory} instances.
 *
 * <p>A spec is what travels in the job graph; the actual memory implementation is constructed
 * per-task inside a {@code RichFunction.open()} by calling {@link #bind(RuntimeContext)}. This
 * mirrors how Flink itself splits the job-graph-time and runtime concerns — descriptors and TTL
 * configs are serializable, but the underlying state handles aren't.
 *
 * <p>Implementations must be {@link Serializable}. The default
 * implementation, {@link FlinkStateShortTermMemory#spec()}, returns a Flink-keyed-state-backed
 * memory. Third parties may provide their own (Caffeine cache, Redis-as-HOT, etc.) by
 * implementing this interface and registering it via {@code ServiceLoader}.
 */
public interface ShortTermMemorySpec extends Serializable {

  /** Construct the operator-scoped memory instance for the running task. */
  ShortTermMemory bind(RuntimeContext runtimeContext) throws Exception;

  /**
   * The TTL applied to state entries managed by this memory. {@link Duration#ZERO} means "no
   * TTL." Used by {@code FlinkStateShortTermMemory} to configure {@code StateTtlConfig}; ignored
   * by implementations that manage expiry themselves.
   */
  default Duration ttl() {
    return Duration.ZERO;
  }

  /** Human-readable spec name, used for logging. */
  default String providerName() {
    return getClass().getSimpleName();
  }
}

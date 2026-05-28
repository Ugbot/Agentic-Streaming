package org.agentic.flink.control;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Flips an operator's debug side-output on or off. The receiver applies passive TTL expiry — no
 * Flink timers required.
 *
 * <p>Static factories cover the common cases. Construct directly only if you need a non-default
 * TTL with the {@code enabled=true} state.
 *
 * <p>Declared as a Flink-compatible POJO (public no-arg constructor, JavaBean getters/setters)
 * so Flink's {@code PojoSerializer} can carry it across the network without falling back to
 * Kryo. {@link #toString}, {@link #equals}, {@link #hashCode} mirror the record semantics this
 * type replaced.
 */
public final class DebugControl implements ControlMessage {
  private static final long serialVersionUID = 1L;

  private String operatorId;
  private boolean enabled;
  private long ttlMillis;

  /** Required by Flink's POJO serializer. */
  public DebugControl() {}

  @JsonCreator
  public DebugControl(
      @JsonProperty("operatorId") String operatorId,
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("ttlMillis") long ttlMillis) {
    this.operatorId = operatorId;
    this.enabled = enabled;
    this.ttlMillis = ttlMillis;
  }

  // ---- ControlMessage ----

  @Override
  public String operatorId() {
    return operatorId;
  }

  @Override
  public long ttlMillis() {
    return ttlMillis;
  }

  // ---- POJO-shaped getters/setters (also required by Flink POJO detection) ----

  public String getOperatorId() {
    return operatorId;
  }

  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public long getTtlMillis() {
    return ttlMillis;
  }

  public void setTtlMillis(long ttlMillis) {
    this.ttlMillis = ttlMillis;
  }

  /** Convenience accessor for the {@code enabled} bit; mirrors the old record component name. */
  public boolean enabled() {
    return enabled;
  }

  // ---- factories ----

  /** Enable debug for {@code operatorId} for the framework default of 2 minutes. */
  public static DebugControl on(String operatorId) {
    return new DebugControl(operatorId, true, DEFAULT_TTL_MILLIS);
  }

  /** Enable debug for {@code operatorId} for a custom TTL in milliseconds. */
  public static DebugControl on(String operatorId, long ttlMillis) {
    return new DebugControl(operatorId, true, ttlMillis);
  }

  /** Enable debug for {@code operatorId} permanently until cancelled. */
  public static DebugControl pinned(String operatorId) {
    return new DebugControl(operatorId, true, PERMANENT);
  }

  /** Disable debug for {@code operatorId}; cancels any outstanding TTL. */
  public static DebugControl off(String operatorId) {
    return new DebugControl(operatorId, false, 0L);
  }

  /** Enable debug everywhere for the framework default TTL. */
  public static DebugControl everywhere() {
    return new DebugControl(ALL_OPERATORS, true, DEFAULT_TTL_MILLIS);
  }

  /** Disable debug everywhere. */
  public static DebugControl silenceAll() {
    return new DebugControl(ALL_OPERATORS, false, 0L);
  }

  // ---- canonical equality/hash/string ----

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DebugControl that)) {
      return false;
    }
    return enabled == that.enabled
        && ttlMillis == that.ttlMillis
        && Objects.equals(operatorId, that.operatorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operatorId, enabled, ttlMillis);
  }

  @Override
  public String toString() {
    return "DebugControl["
        + "operatorId="
        + operatorId
        + ", enabled="
        + enabled
        + ", ttlMillis="
        + ttlMillis
        + ']';
  }
}

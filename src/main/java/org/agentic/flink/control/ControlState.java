package org.agentic.flink.control;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Shared descriptor for the broadcast state that every framework operator uses to track
 * currently-active {@link ControlMessage}s by {@code operatorId}.
 *
 * <p>The state stores compact directives rather than the raw {@link ControlMessage} so the
 * passive-expiry check on the hot path is one map lookup + two comparisons.
 */
public final class ControlState {
  private ControlState() {}

  /**
   * Per-operatorId directive cache. Key is the operator id (or {@code "*"} for broadcast-all);
   * value is the resolved {@link Directive}.
   */
  public static final MapStateDescriptor<String, Directive> DIRECTIVES =
      new MapStateDescriptor<>(
          "agentic-control-directives",
          TypeInformation.of(String.class),
          TypeInformation.of(Directive.class));

  /**
   * Resolved directive snapshot stored in broadcast state. Flink POJO so the serializer doesn't
   * fall back to Kryo; {@code expiresAtMs == Long.MAX_VALUE} means pinned (permanent).
   */
  public static final class Directive implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean debugEnabled;
    private long expiresAtMs;

    /** Required by Flink POJO serializer. */
    public Directive() {}

    public Directive(boolean debugEnabled, long expiresAtMs) {
      this.debugEnabled = debugEnabled;
      this.expiresAtMs = expiresAtMs;
    }

    /** True if this directive's debug flag is still in effect at {@code nowMs}. */
    public boolean active(long nowMs) {
      return debugEnabled && expiresAtMs > nowMs;
    }

    public boolean isDebugEnabled() {
      return debugEnabled;
    }

    public void setDebugEnabled(boolean debugEnabled) {
      this.debugEnabled = debugEnabled;
    }

    public long getExpiresAtMs() {
      return expiresAtMs;
    }

    public void setExpiresAtMs(long expiresAtMs) {
      this.expiresAtMs = expiresAtMs;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Directive that)) {
        return false;
      }
      return debugEnabled == that.debugEnabled && expiresAtMs == that.expiresAtMs;
    }

    @Override
    public int hashCode() {
      return Objects.hash(debugEnabled, expiresAtMs);
    }

    @Override
    public String toString() {
      return "Directive[debugEnabled=" + debugEnabled + ", expiresAtMs=" + expiresAtMs + ']';
    }
  }
}

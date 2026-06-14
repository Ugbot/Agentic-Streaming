package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Lifecycle states of an A2A {@link A2ATask}, matching the A2A protocol v1.0 {@code TaskState}
 * enumeration.
 *
 * <p>The JSON wire form uses kebab-case strings ({@code input-required}, {@code auth-required});
 * {@link #wire()} / {@link #fromWire(String)} translate between those and the Java enum so our
 * bridge envelopes and the SDK adapter agree on a single representation.
 */
public enum A2ATaskState {
  SUBMITTED("submitted", false, false),
  WORKING("working", false, false),
  /** Agent paused awaiting more input from the caller — resumable, non-terminal. */
  INPUT_REQUIRED("input-required", false, true),
  /** Agent paused awaiting credentials — resumable, non-terminal. */
  AUTH_REQUIRED("auth-required", false, true),
  COMPLETED("completed", true, false),
  CANCELED("canceled", true, false),
  FAILED("failed", true, false),
  REJECTED("rejected", true, false),
  UNKNOWN("unknown", false, false);

  private final String wire;
  private final boolean terminal;
  private final boolean interrupted;

  A2ATaskState(String wire, boolean terminal, boolean interrupted) {
    this.wire = wire;
    this.terminal = terminal;
    this.interrupted = interrupted;
  }

  /** The kebab-case protocol string for this state (e.g. {@code "input-required"}). */
  @JsonValue
  public String wire() {
    return wire;
  }

  /** True for states no further work will leave: completed, canceled, failed, rejected. */
  public boolean isTerminal() {
    return terminal;
  }

  /**
   * True for resumable pause states ({@code input-required}, {@code auth-required}) — the task is
   * not finished but is waiting on the caller.
   */
  public boolean isInterrupted() {
    return interrupted;
  }

  /** True once the task will produce no more updates without caller action. */
  public boolean isFinal() {
    return terminal || interrupted;
  }

  /** Parse a protocol wire string back into the enum; unknown / null map to {@link #UNKNOWN}. */
  @JsonCreator
  public static A2ATaskState fromWire(String wire) {
    if (wire == null) {
      return UNKNOWN;
    }
    String normalized = wire.trim().toLowerCase();
    for (A2ATaskState state : values()) {
      if (state.wire.equals(normalized)) {
        return state;
      }
    }
    // Tolerate the proto enum form TASK_STATE_INPUT_REQUIRED as well.
    String stripped = normalized.replace("task_state_", "").replace('_', '-');
    for (A2ATaskState state : values()) {
      if (state.wire.equals(stripped)) {
        return state;
      }
    }
    return UNKNOWN;
  }
}

package org.jagentic.core;

import java.util.Optional;

/** The closed v1 event type set (spec/v1/primitives.md §1, spec/v1/result.schema.json). */
public enum EventType {
  TURN_RECEIVED,
  GUARDRAIL_REJECTED,
  ROUTED,
  BRAIN_STARTED,
  TOOL_CALLED,
  TOOL_FAILED,
  RETRIEVED,
  REPLY_DRAFTED,
  VERIFICATION_FAILED,
  MEMORY_WRITTEN,
  TURN_COMPLETED,
  TURN_FAILED,
  TURN_SUSPENDED,
  TURN_RESUMED,
  TIMER_SCHEDULED,
  TIMER_FIRED,
  COMPENSATION_STARTED,
  COMPENSATION_STEP,
  COMPENSATION_COMPLETED,
  DELEGATED;

  /** The snake_case wire name used in logs and normalized results. */
  public String wire() {
    return name().toLowerCase();
  }

  /** Empty for names outside the v1 set; reducers must ignore those rather than fail. */
  public static Optional<EventType> parse(String wire) {
    if (wire == null) {
      return Optional.empty();
    }
    for (EventType t : values()) {
      if (t.wire().equals(wire)) {
        return Optional.of(t);
      }
    }
    return Optional.empty();
  }
}

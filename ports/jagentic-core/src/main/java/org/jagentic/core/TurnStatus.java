package org.jagentic.core;

/** The six terminal turn statuses (spec/v1/primitives.md §1 "Turn result"). */
public enum TurnStatus {
  COMPLETED,
  REJECTED,
  UNVERIFIED,
  FAILED,
  SUSPENDED,
  DUPLICATE;

  public String wire() {
    return name().toLowerCase();
  }

  public static TurnStatus parse(String wire) {
    return valueOf(wire.toUpperCase());
  }
}

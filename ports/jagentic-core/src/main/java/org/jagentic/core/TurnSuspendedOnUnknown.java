package org.jagentic.core;

/** A call's outcome cannot be known, so the turn stops here and waits for a person. */
final class TurnSuspendedOnUnknown extends RuntimeException {
  private static final long serialVersionUID = 1L;
  final String tool;
  final int index;

  TurnSuspendedOnUnknown(String tool, int index) {
    super(tool + " outcome unknown");
    this.tool = tool;
    this.index = index;
  }
}

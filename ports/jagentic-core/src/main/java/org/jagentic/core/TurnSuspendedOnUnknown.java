package org.jagentic.core;

/** A call's outcome cannot be known, or a call needs a person first: the turn stops here and waits. */
final class TurnSuspendedOnUnknown extends RuntimeException {
  private static final long serialVersionUID = 1L;
  final String tool;
  final int index;
  final String until;

  TurnSuspendedOnUnknown(String tool, int index) {
    this(tool, index, ConversationState.UNKNOWN_OUTCOME);
  }

  TurnSuspendedOnUnknown(String tool, int index, String until) {
    super(tool + " waits for a person");
    this.tool = tool;
    this.index = index;
    this.until = until;
  }
}

package org.jagentic.core;

/** A call's outcome cannot be known, or a call needs a person first: the turn stops here and waits. */
final class TurnSuspendedOnUnknown extends RuntimeException {
  private static final long serialVersionUID = 1L;
  final String tool;
  final int index;
  final String until;
  /** For an approval, what the call is: the tool and a digest of its arguments. Null otherwise. */
  final String intent;

  TurnSuspendedOnUnknown(String tool, int index) {
    this(tool, index, ConversationState.UNKNOWN_OUTCOME);
  }

  TurnSuspendedOnUnknown(String tool, int index, String until) {
    this(tool, index, until, null);
  }

  TurnSuspendedOnUnknown(String tool, int index, String until, String intent) {
    super(tool + " waits for a person");
    this.tool = tool;
    this.index = index;
    this.until = until;
    this.intent = intent;
  }
}

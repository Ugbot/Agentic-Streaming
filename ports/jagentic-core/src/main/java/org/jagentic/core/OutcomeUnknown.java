package org.jagentic.core;

/**
 * An attempt ended without a definite answer: a timeout, a lost connection, a server error. The
 * effect may have landed. A {@code non_idempotent} tool that throws this suspends the turn for a
 * person; for any other tool it is an ordinary failed attempt, retried under the same key.
 */
public class OutcomeUnknown extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public OutcomeUnknown(String message) {
    super(message);
  }

  public OutcomeUnknown(String message, Throwable cause) {
    super(message, cause);
  }
}

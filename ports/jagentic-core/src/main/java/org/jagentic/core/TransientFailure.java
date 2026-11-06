package org.jagentic.core;

/**
 * A failure that may pass if the same request is made again: a model or network outage. It is
 * retried under {@code policies.retry}; a turn that outlasts the budget ends with class {@code
 * transient}, which does not spend its {@code turn_id}.
 */
public class TransientFailure extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public TransientFailure(String message) {
    super(message);
  }

  public TransientFailure(String message, Throwable cause) {
    super(message, cause);
  }
}

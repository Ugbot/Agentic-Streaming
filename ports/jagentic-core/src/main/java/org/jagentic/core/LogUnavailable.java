package org.jagentic.core;

/**
 * The conversation log itself could not be read or written: a storage failure, not something a
 * turn did. It is the one failure the turn boundary lets through. A turn cannot record its own end
 * in a log it cannot reach, so the runtime has to treat this as infrastructure and restart.
 */
public final class LogUnavailable extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public LogUnavailable(String message, Throwable cause) {
    super(message, cause);
  }
}

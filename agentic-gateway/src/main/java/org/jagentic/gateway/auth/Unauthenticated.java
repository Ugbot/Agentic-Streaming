package org.jagentic.gateway.auth;

/** A request that proved nothing about its caller. The message is for logs, never for the caller. */
public final class Unauthenticated extends Exception {
  private static final long serialVersionUID = 1L;

  public Unauthenticated(String message) {
    super(message);
  }

  public Unauthenticated(String message, Throwable cause) {
    super(message, cause);
  }
}

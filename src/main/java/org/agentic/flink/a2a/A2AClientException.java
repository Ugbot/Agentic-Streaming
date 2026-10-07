package org.agentic.flink.a2a;

import org.agentic.flink.annotation.Experimental;

/** Unchecked failure raised by an {@link A2AClient} when a remote A2A call cannot complete. */
@Experimental
public class A2AClientException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public A2AClientException(String message) {
    super(message);
  }

  public A2AClientException(String message, Throwable cause) {
    super(message, cause);
  }
}

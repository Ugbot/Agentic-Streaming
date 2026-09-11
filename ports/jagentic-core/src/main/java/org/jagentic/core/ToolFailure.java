package org.jagentic.core;

/** A tool call that failed after the retry budget was exhausted (error class {@code tool}). */
public final class ToolFailure extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final String toolId;
  private final int index;
  private final int attempts;

  public ToolFailure(String toolId, int index, int attempts, String message, Throwable cause) {
    super(message, cause);
    this.toolId = toolId;
    this.index = index;
    this.attempts = attempts;
  }

  public String toolId() {
    return toolId;
  }

  public int index() {
    return index;
  }

  public int attempts() {
    return attempts;
  }
}

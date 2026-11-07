package org.jagentic.core;

/**
 * The tool policy said no (error class {@code policy}). Nothing ran, the refusal is on the log as
 * {@code tool_refused}, and the turn ends: nothing offers the tool back to the model.
 */
public final class ToolRefused extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final String toolId;

  public ToolRefused(String toolId, String reason) {
    super(reason);
    this.toolId = toolId;
  }

  public String toolId() {
    return toolId;
  }
}

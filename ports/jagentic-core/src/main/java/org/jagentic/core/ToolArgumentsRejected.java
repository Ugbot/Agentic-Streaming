package org.jagentic.core;

/** Arguments failed the tool's {@code parameters} schema. Error class {@code validation}. */
public final class ToolArgumentsRejected extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;

  public ToolArgumentsRejected(String toolId, java.util.List<String> errors) {
    super("tool " + toolId + " arguments rejected: " + String.join("; ", errors));
  }
}

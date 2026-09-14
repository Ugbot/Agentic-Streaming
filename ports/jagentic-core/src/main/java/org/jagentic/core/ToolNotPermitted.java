package org.jagentic.core;

import java.util.Collection;
import java.util.List;

/**
 * A brain asked for a tool the path does not declare (error class {@code validation}). Raised by
 * {@link org.jagentic.core.llm.LlmBrain#withStrictTools()} instead of feeding the model an error
 * observation, so a scripted step naming an undeclared tool fails the turn like pyagentic does.
 */
public final class ToolNotPermitted extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;
  private final String toolId;
  private final List<String> permitted;

  public ToolNotPermitted(String toolId, String path, Collection<String> permitted) {
    super("tool " + toolId + " is not in path " + path + " tools " + permitted.stream().sorted().toList());
    this.toolId = toolId;
    this.permitted = permitted.stream().sorted().toList();
  }

  public String toolId() {
    return toolId;
  }

  public List<String> permitted() {
    return permitted;
  }
}

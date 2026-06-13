package org.agentic.flink.example.banking;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One step of a ReAct loop, the structured-output schema the model fills each iteration. A
 * standalone POJO (no Flink dependency) so {@link ReActTurnBrain} can run in a plain JVM — e.g. the
 * standalone {@link BankingA2AServer} — without pulling in the Flink-coupled {@code
 * ReActProcessFunction}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReActStep {
  /** "thought" | "action" | "final". */
  private String type;

  private String thought;

  /** Tool name when {@code type=action}; null otherwise. */
  private String tool;

  private Map<String, Object> arguments;

  /** Final answer when {@code type=final}. */
  private String answer;
}

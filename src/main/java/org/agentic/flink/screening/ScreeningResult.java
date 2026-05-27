package org.agentic.flink.screening;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;

/** Outcome of running one {@link ScreenItem} through a {@link ScreeningPipeline}. */
public final class ScreeningResult implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Which tier produced the final decision. */
  public enum Tier {
    RULES,
    ML,
    LLM
  }

  public final ScreenItem item;
  public final List<Signal> fired;
  public final double combinedRisk;
  public final Tier decidedBy;
  public final String verdict; // ALLOW / REVIEW / BLOCK
  public final String mlLabel; // null if ML tier not reached
  public final double mlScore;
  public final String llmRationale; // null if LLM tier not reached
  public final String reason;

  public ScreeningResult(
      ScreenItem item, List<Signal> fired, double combinedRisk, Tier decidedBy, String verdict,
      String mlLabel, double mlScore, String llmRationale, String reason) {
    this.item = item;
    this.fired = fired;
    this.combinedRisk = combinedRisk;
    this.decidedBy = decidedBy;
    this.verdict = verdict;
    this.mlLabel = mlLabel;
    this.mlScore = mlScore;
    this.llmRationale = llmRationale;
    this.reason = reason;
  }

  @Override
  public String toString() {
    StringBuilder phases = new StringBuilder();
    for (Signal s : fired) {
      if (phases.length() > 0) phases.append(',');
      phases.append(s.phase());
    }
    return String.format(Locale.ROOT, "[%s] %s risk=%.2f signals=[%s] — %s",
        decidedBy, verdict, combinedRisk, phases, reason);
  }
}

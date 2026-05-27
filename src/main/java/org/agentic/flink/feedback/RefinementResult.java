package org.agentic.flink.feedback;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;

/** Outcome of a {@link RefinementLoop}: the chosen output, whether it was accepted, and the trace. */
public final class RefinementResult implements Serializable {
  private static final long serialVersionUID = 1L;

  /** One generate-and-check cycle. */
  public static final class Attempt implements Serializable {
    private static final long serialVersionUID = 1L;
    public final int index;
    public final String output;
    public final double score;
    public final String critique; // null when that attempt passed

    public Attempt(int index, String output, double score, String critique) {
      this.index = index;
      this.output = output;
      this.score = score;
      this.critique = critique;
    }
  }

  public final String finalText;
  public final boolean accepted;
  public final int attemptsUsed;
  public final List<Attempt> trace;

  public RefinementResult(String finalText, boolean accepted, int attemptsUsed, List<Attempt> trace) {
    this.finalText = finalText;
    this.accepted = accepted;
    this.attemptsUsed = attemptsUsed;
    this.trace = trace;
  }

  public double finalScore() {
    return trace.isEmpty() ? 0.0 : trace.stream().mapToDouble(a -> a.score).max().orElse(0.0);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "accepted=%s attempts=%d score=%.2f",
        accepted, attemptsUsed, finalScore());
  }
}

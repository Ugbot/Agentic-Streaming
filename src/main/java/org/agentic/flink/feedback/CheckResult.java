package org.agentic.flink.feedback;

import java.io.Serializable;

/** Outcome of a {@link QualityCheck}: a 0–1 quality score, whether it passed, and a critique. */
public final class CheckResult implements Serializable {
  private static final long serialVersionUID = 1L;

  public final double score;
  public final boolean passed;
  public final String critique; // null when passed

  public CheckResult(double score, boolean passed, String critique) {
    this.score = score;
    this.passed = passed;
    this.critique = passed ? null : critique;
  }

  public static CheckResult pass(double score) {
    return new CheckResult(score, true, null);
  }

  public static CheckResult fail(double score, String critique) {
    return new CheckResult(score, false, critique);
  }
}

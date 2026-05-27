package org.agentic.flink.feedback;

import java.io.Serializable;

/**
 * The "check" in a {@link RefinementLoop}: judges an output's quality and, when it falls short,
 * returns a critique that is fed back to the generator for the next attempt.
 */
public interface QualityCheck extends Serializable {

  /**
   * @param task the original task/prompt
   * @param output the candidate output to judge
   */
  CheckResult check(String task, String output);
}

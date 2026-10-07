package org.agentic.flink.inference;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.agentic.flink.annotation.Experimental;

/**
 * Parsed outcome of a validator/judge model response of the form {@code VALID|INVALID [score]
 * [reason]}.
 *
 * <p>The verdict is decided by whole-word matching, so {@code INVALID} never reads as {@code
 * VALID}, and a negated {@code NOT VALID} (any spacing, case or hyphenation between the two words)
 * reads as {@code INVALID}. The first verdict in the response wins, wherever it sits in the
 * surrounding prose; a response with no verdict word is {@link Outcome#UNDETERMINED} and must be
 * treated as a failure by callers rather than as a pass.
 *
 * <p>This is the single verdict parser for the framework: {@code ValidationFunction}, {@code
 * ValidationExecutor}, {@code ValidationGuardrail} and the examples all go through {@link #parse}.
 */
@Experimental
public final class ValidationVerdict implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum Outcome {
    VALID,
    INVALID,
    UNDETERMINED
  }

  private static final Pattern VERDICT_WORD =
      Pattern.compile("\\b(?:(NOT[\\s_-]+VALID)|(INVALID)|(VALID))\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern SCORE = Pattern.compile("\\b(?:0(?:\\.\\d+)?|1(?:\\.0+)?)\\b");

  private final Outcome outcome;
  private final double score;
  private final String rawResponse;

  private ValidationVerdict(Outcome outcome, double score, String rawResponse) {
    this.outcome = outcome;
    this.score = score;
    this.rawResponse = rawResponse;
  }

  public static ValidationVerdict parse(String response) {
    if (response == null || response.isBlank()) {
      return new ValidationVerdict(Outcome.UNDETERMINED, 0.0, response == null ? "" : response);
    }
    Matcher m = VERDICT_WORD.matcher(response);
    if (!m.find()) {
      return new ValidationVerdict(Outcome.UNDETERMINED, 0.0, response);
    }
    Outcome outcome = m.group(3) != null ? Outcome.VALID : Outcome.INVALID;
    double score = outcome == Outcome.VALID ? 1.0 : 0.0;
    Matcher s = SCORE.matcher(response);
    if (s.find(m.end())) {
      score = Double.parseDouble(s.group());
    }
    return new ValidationVerdict(outcome, score, response);
  }

  public Outcome getOutcome() {
    return outcome;
  }

  public boolean isValid() {
    return outcome == Outcome.VALID;
  }

  public double getScore() {
    return score;
  }

  public String getRawResponse() {
    return rawResponse;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ValidationVerdict)) return false;
    ValidationVerdict that = (ValidationVerdict) o;
    return outcome == that.outcome
        && Double.compare(score, that.score) == 0
        && rawResponse.equals(that.rawResponse);
  }

  @Override
  public int hashCode() {
    return Objects.hash(outcome, score, rawResponse);
  }

  @Override
  public String toString() {
    return "ValidationVerdict{" + outcome + ", score=" + score + "}";
  }
}

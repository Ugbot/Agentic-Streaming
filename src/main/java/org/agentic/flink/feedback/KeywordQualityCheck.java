package org.agentic.flink.feedback;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Deterministic, zero-infra {@link QualityCheck}: the output passes when it contains all required
 * terms (case-insensitive) and meets a minimum length. Score is the fraction of checks satisfied.
 * The workhorse for tests and offline demos.
 */
public final class KeywordQualityCheck implements QualityCheck {
  private static final long serialVersionUID = 1L;

  private final List<String> requiredTerms;
  private final int minLength;
  private final double threshold;

  public KeywordQualityCheck(List<String> requiredTerms, int minLength, double threshold) {
    this.requiredTerms = requiredTerms == null ? List.of() : List.copyOf(requiredTerms);
    this.minLength = minLength;
    this.threshold = threshold;
  }

  /** No requirements — always passes (a no-op default gate). */
  public KeywordQualityCheck() {
    this(List.of(), 0, 1.0);
  }

  public static KeywordQualityCheck requiring(String... terms) {
    return new KeywordQualityCheck(List.of(terms), 0, 1.0);
  }

  @Override
  public CheckResult check(String task, String output) {
    String text = output == null ? "" : output.toLowerCase(Locale.ROOT);
    int total = requiredTerms.size() + (minLength > 0 ? 1 : 0);
    if (total == 0) return CheckResult.pass(1.0);

    List<String> missing = new ArrayList<>();
    int satisfied = 0;
    for (String term : requiredTerms) {
      if (text.contains(term.toLowerCase(Locale.ROOT))) satisfied++;
      else missing.add(term);
    }
    boolean lengthOk = minLength <= 0 || text.length() >= minLength;
    if (minLength > 0 && lengthOk) satisfied++;

    double score = (double) satisfied / total;
    if (score >= threshold) return CheckResult.pass(score);

    StringBuilder critique = new StringBuilder();
    if (!missing.isEmpty()) critique.append("missing required terms: ").append(missing);
    if (!lengthOk) {
      if (critique.length() > 0) critique.append("; ");
      critique.append("too short (need >= ").append(minLength).append(" chars)");
    }
    return CheckResult.fail(score, critique.toString());
  }
}

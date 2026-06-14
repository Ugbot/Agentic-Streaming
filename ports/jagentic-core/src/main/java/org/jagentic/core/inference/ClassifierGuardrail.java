package org.jagentic.core.inference;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jagentic.core.Guardrail;

/** A {@link Guardrail} that blocks when a classifier assigns a *blocked* label with
 * confidence at or above {@code threshold}. A learned/lexicon alternative to RegexGuardrail. */
public final class ClassifierGuardrail implements Guardrail {

  private final Classifier classifier;
  private final Set<String> blocked;
  private final double threshold;
  private final String reason;
  private final boolean checkOutputs;

  public ClassifierGuardrail(Classifier classifier, List<String> blockedLabels, double threshold,
      String reason, boolean checkOutputs) {
    this.classifier = classifier;
    this.blocked = blockedLabels.stream().map(String::toLowerCase).collect(Collectors.toSet());
    this.threshold = threshold;
    this.reason = reason == null || reason.isBlank() ? "blocked by classifier policy" : reason;
    this.checkOutputs = checkOutputs;
  }

  private String hit(String text) {
    if (text == null || text.isBlank()) {
      return null;
    }
    Classification c = classifier.classify(text);
    if (blocked.contains(c.label().toLowerCase()) && c.score() >= threshold) {
      return String.format("%s (%s=%.2f)", reason, c.label(), c.score());
    }
    return null;
  }

  @Override
  public String checkInput(String text) {
    return hit(text);
  }

  @Override
  public String checkOutput(String reply) {
    return checkOutputs ? hit(reply) : null;
  }
}

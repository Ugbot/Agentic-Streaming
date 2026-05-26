package org.agentic.flink.inference;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Output of {@link Classifier#classify}.
 *
 * <p>Carries the top label as a string (not a generic type parameter — Java erasure makes that
 * ugly through Jackson), its score, and the full probability distribution for callers that need
 * to apply their own thresholds.
 */
public final class ClassificationResult implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String label;
  private final double score;
  private final Map<String, Double> probabilities;

  public ClassificationResult(String label, double score, Map<String, Double> probabilities) {
    this.label = Objects.requireNonNull(label, "label");
    this.score = score;
    this.probabilities =
        probabilities == null ? Collections.emptyMap() : Map.copyOf(probabilities);
  }

  public String getLabel() {
    return label;
  }

  public double getScore() {
    return score;
  }

  public Map<String, Double> getProbabilities() {
    return probabilities;
  }

  @Override
  public String toString() {
    return "ClassificationResult[label=" + label + ", score=" + score + "]";
  }
}

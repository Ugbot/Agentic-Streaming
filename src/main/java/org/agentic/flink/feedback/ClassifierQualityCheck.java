package org.agentic.flink.feedback;

import org.agentic.flink.inference.ClassificationResult;
import org.agentic.flink.inference.Classifier;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import java.util.Locale;

/**
 * {@link QualityCheck} backed by an ML {@link Classifier}. Classifies the output and passes/fails
 * against a threshold. Direction matters: for a "toxicity"/"risk" classifier you want the score
 * <b>below</b> a ceiling (set {@code passIfBelow=true}); for a "quality" classifier you want it
 * <b>at/above</b> a floor ({@code passIfBelow=false}). The reported quality score is normalized so
 * higher is always better.
 *
 * <p>Zero-infra with the default {@link org.agentic.flink.inference.LexiconInferenceConnection}.
 * The classifier binds lazily and is held {@code transient} for Flink serializability.
 */
public final class ClassifierQualityCheck implements QualityCheck {
  private static final long serialVersionUID = 1L;

  private final InferenceConnection connection;
  private final InferenceSetup setup;
  private final double threshold;
  private final boolean passIfBelow;
  private transient Classifier classifier;

  public ClassifierQualityCheck(
      InferenceConnection connection, InferenceSetup setup, double threshold, boolean passIfBelow) {
    this.connection = connection;
    this.setup = setup;
    this.threshold = threshold;
    this.passIfBelow = passIfBelow;
  }

  private Classifier classifier() {
    if (classifier == null) {
      try {
        classifier = connection.bind(null).asClassifier();
      } catch (Exception e) {
        throw new RuntimeException("Failed to bind classifier: " + e.getMessage(), e);
      }
    }
    return classifier;
  }

  @Override
  public CheckResult check(String task, String output) {
    ClassificationResult cr = classifier().classify(output == null ? "" : output, setup);
    double raw = cr.getScore();
    boolean passed = passIfBelow ? raw <= threshold : raw >= threshold;
    double quality = passIfBelow ? 1.0 - raw : raw; // higher is better
    if (passed) return CheckResult.pass(quality);
    String critique =
        String.format(Locale.ROOT,
            "classifier label '%s' score %.2f fails %s %.2f",
            cr.getLabel(), raw, passIfBelow ? "ceiling" : "floor", threshold);
    return CheckResult.fail(quality, critique);
  }
}

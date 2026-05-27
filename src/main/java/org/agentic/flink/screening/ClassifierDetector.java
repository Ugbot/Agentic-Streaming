package org.agentic.flink.screening;

import org.agentic.flink.inference.ClassificationResult;
import org.agentic.flink.inference.Classifier;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceSetup;

/**
 * Classifier-backed detector: runs the ML {@link Classifier} over a text view of the item and emits
 * a {@link Phase#CLASSIFIER} signal weighted by the classification score. Used by
 * {@link ScreeningPipeline}'s ML tier, but also usable as a rule-tier {@link Detector}.
 *
 * <p>The {@link Classifier} is bound lazily and held {@code transient} so the detector stays
 * Serializable for Flink distribution; it rebinds per task.
 */
public final class ClassifierDetector implements Detector {
  private static final long serialVersionUID = 1L;

  private final InferenceConnection connection;
  private final InferenceSetup setup;
  private transient Classifier classifier;

  public ClassifierDetector(InferenceConnection connection, InferenceSetup setup) {
    this.connection = connection;
    this.setup = setup;
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

  /** Full classification result for a screen item (used by the pipeline's ML tier). */
  public ClassificationResult classify(ScreenItem item) {
    return classifier().classify(textOf(item), setup);
  }

  @Override
  public Signal inspect(ScreenItem item, ScreenContext ctx) {
    ClassificationResult r = classify(item);
    return new Signal(
        name(), Phase.CLASSIFIER, r.getScore(),
        String.format("classified '%s' (%.2f)", r.getLabel(), r.getScore()));
  }

  /** Text view of an item for a text-in classifier: label plus attribute values. */
  static String textOf(ScreenItem item) {
    StringBuilder sb = new StringBuilder(item.label() == null ? "" : item.label());
    if (item.attrs() != null) {
      for (String v : item.attrs().values()) {
        sb.append(' ').append(v);
      }
    }
    return sb.toString();
  }

  @Override
  public String name() {
    return "classifier";
  }
}

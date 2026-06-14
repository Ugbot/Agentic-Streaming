package org.jagentic.core.inference;

/** Adapts a {@link Classifier} into a {@link Scorer} for one target label (its probability). */
public final class ClassifierScorer implements Scorer {
  private final Classifier classifier;
  private final String label;

  public ClassifierScorer(Classifier classifier, String label) {
    this.classifier = classifier;
    this.label = label;
  }

  @Override
  public double score(String text) {
    Classification c = classifier.classify(text);
    Double s = c.scores().get(label);
    if (s != null) {
      return s;
    }
    return c.label().equals(label) ? c.score() : 0.0;
  }
}

package org.agentic.flink.inference;

import java.util.List;

/**
 * Typed task view for regression / scoring models: input in, single numeric out.
 *
 * <p>Returned by {@link InferenceClient#asScorer()}. Used to plug a trained ranker, quality
 * estimator, or relevancy model into the framework — see
 * {@link org.agentic.flink.context.relevancy.RelevancyScorer}.
 */
public interface Scorer {

  /** Score a single input. Higher = better unless documented otherwise by the model. */
  double score(String input, InferenceSetup setup);

  /**
   * Score one item against a reference (intent, query, hypothesis) — common for cross-encoder
   * ranking models. Default falls back to {@link #score(String, InferenceSetup)} ignoring
   * {@code reference}; backends with native pair-scoring support should override.
   */
  default double scorePair(String input, String reference, InferenceSetup setup) {
    return score(input, setup);
  }

  /** Batch variant; default loops. */
  default List<Double> scoreBatch(List<String> inputs, InferenceSetup setup) {
    java.util.ArrayList<Double> out = new java.util.ArrayList<>(inputs.size());
    for (String s : inputs) {
      out.add(score(s, setup));
    }
    return out;
  }
}

package org.agentic.flink.inference;

import java.util.List;

/**
 * Typed task view for classification models: text in, label + score out.
 *
 * <p>Returned by {@link InferenceClient#asClassifier()}. Implementations that don't support
 * classification throw {@link UnsupportedOperationException} from that accessor; callers can
 * check {@link InferenceClient#supports} before asking.
 */
public interface Classifier {

  /** Classify a single input under the given setup. */
  ClassificationResult classify(String input, InferenceSetup setup);

  /**
   * Classify a batch. Default implementation calls {@link #classify} per item; backends with
   * native batch support (the typical case for DJL) should override.
   */
  default List<ClassificationResult> classifyBatch(List<String> inputs, InferenceSetup setup) {
    java.util.ArrayList<ClassificationResult> out = new java.util.ArrayList<>(inputs.size());
    for (String s : inputs) {
      out.add(classify(s, setup));
    }
    return out;
  }
}

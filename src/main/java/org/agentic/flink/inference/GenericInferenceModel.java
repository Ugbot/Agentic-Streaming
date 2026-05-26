package org.agentic.flink.inference;

import java.util.Map;

/**
 * Untyped escape hatch for inference models whose I/O doesn't fit {@link Classifier} or
 * {@link Scorer}: object detection (returns bounding boxes), sequence labelling (returns spans),
 * anomaly detection (returns a flag + score), feature extractors, etc.
 *
 * <p>Inputs and outputs are loosely-typed maps so the framework can hand off without forcing a
 * shared schema. The backend documents what keys it expects and produces.
 */
public interface GenericInferenceModel {

  /** Run inference. Backend-specific input/output shape. */
  Map<String, Object> infer(Map<String, Object> input, InferenceSetup setup);
}

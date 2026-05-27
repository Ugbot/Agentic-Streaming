package org.agentic.flink.screening;

/** Which screening phase a {@link Signal} came from. */
public enum Phase {
  BAND_PASS,
  REPEAT,
  VELOCITY,
  CLASSIFIER
}

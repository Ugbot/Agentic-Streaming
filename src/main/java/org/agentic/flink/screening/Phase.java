package org.agentic.flink.screening;

/** Which screening phase a {@link Signal} came from. */
public enum Phase {
  BAND_PASS,
  REPEAT,
  VELOCITY,
  /** Text keyword/lexicon rule (e.g. prompt-injection / social-engineering screening). */
  LEXICON,
  CLASSIFIER
}

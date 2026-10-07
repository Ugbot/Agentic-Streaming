package org.agentic.flink.screening;

import org.agentic.flink.annotation.Public;

/** Which screening phase a {@link Signal} came from. */
@Public
public enum Phase {
  BAND_PASS,
  REPEAT,
  VELOCITY,
  /** Text keyword/lexicon rule (e.g. prompt-injection / social-engineering screening). */
  LEXICON,
  CLASSIFIER
}

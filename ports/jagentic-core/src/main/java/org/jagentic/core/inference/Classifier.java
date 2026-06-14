package org.jagentic.core.inference;

/** Assigns a label (with confidence) to a piece of text. Portable analogue of the Flink
 * {@code inference/Classifier} SPI. */
public interface Classifier {
  Classification classify(String text);
}

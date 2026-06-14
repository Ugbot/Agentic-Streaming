package org.jagentic.core.inference;

/** Maps text to a single score in [0,1]. */
public interface Scorer {
  double score(String text);
}

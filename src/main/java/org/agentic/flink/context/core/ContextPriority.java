package org.agentic.flink.context.core;

/**
 * MoSCoW priority levels for context items
 *
 * <p>MUST: Hard facts, immutable, always retained SHOULD: Important context, compressed if needed
 * COULD: Nice to have, easily discarded WONT: Not needed, discarded immediately
 */
public enum ContextPriority {
  MUST(1.0, "Hard facts, never discard"),
  SHOULD(0.7, "Important, compress if needed"),
  COULD(0.5, "Nice to have, discard easily"),
  WONT(0.0, "Not needed, discard immediately");

  private final double retentionScore;
  private final String description;

  ContextPriority(double retentionScore, String description) {
    this.retentionScore = retentionScore;
    this.description = description;
  }

  public double getRetentionScore() {
    return retentionScore;
  }

  public String getDescription() {
    return description;
  }

  public boolean shouldAlwaysKeep() {
    return this == MUST;
  }

  public boolean canDiscard() {
    return this == COULD || this == WONT;
  }

  public boolean canCompress() {
    return this == SHOULD || this == COULD;
  }
}

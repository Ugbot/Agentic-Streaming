package org.agentic.flink.retrieve;

import java.io.Serializable;

/** A passage retrieved from a corpus, with its score. */
public final class RetrievedPassage implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final String text;
  private final double score;
  private final String sourceUrl;

  public RetrievedPassage(String id, String text, double score, String sourceUrl) {
    this.id = id;
    this.text = text == null ? "" : text;
    this.score = score;
    this.sourceUrl = sourceUrl;
  }

  public String getId() {
    return id;
  }

  public String getText() {
    return text;
  }

  public double getScore() {
    return score;
  }

  public String getSourceUrl() {
    return sourceUrl;
  }
}

package org.agentic.flink.retrieve;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** Final answer emitted by a {@link RetrievalPipeline}. */
public final class Answer implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String question;
  private final String text;
  private final List<RetrievedPassage> citations;
  private final long answeredAt;

  public Answer(String question, String text, List<RetrievedPassage> citations, long answeredAt) {
    this.question = question;
    this.text = text == null ? "" : text;
    this.citations = citations == null ? Collections.emptyList() : List.copyOf(citations);
    this.answeredAt = answeredAt;
  }

  public String getQuestion() {
    return question;
  }

  public String getText() {
    return text;
  }

  public List<RetrievedPassage> getCitations() {
    return citations;
  }

  public long getAnsweredAt() {
    return answeredAt;
  }

  @Override
  public String toString() {
    return "Answer[" + question + " → "
        + (text.length() > 100 ? text.substring(0, 97) + "..." : text)
        + " (" + citations.size() + " citations)]";
  }
}

package org.jagentic.core;

import java.util.ArrayList;
import java.util.List;

/** The outcome of one turn. */
public final class TurnResult {
  public final String conversationId;
  public String reply;
  public String path;
  public boolean ok = true;
  public final List<String> toolCalls;

  public TurnResult(String conversationId, String reply, List<String> toolCalls) {
    this.conversationId = conversationId;
    this.reply = reply;
    this.toolCalls = toolCalls == null ? new ArrayList<>() : new ArrayList<>(toolCalls);
  }
}

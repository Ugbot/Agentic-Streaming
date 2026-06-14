package org.jagentic.core;

/** A prioritized piece of context with a cheap ~4-chars/token estimate. */
public record ContextItem(String text, Priority priority) {
  public int tokens() {
    return Math.max(1, (text.length() + 3) / 4);
  }
}

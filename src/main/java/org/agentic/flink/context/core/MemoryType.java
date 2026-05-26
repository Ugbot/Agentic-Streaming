package org.agentic.flink.context.core;

/**
 * Types of memory in the context hierarchy
 *
 * <p>SHORT_TERM: Working memory, ephemeral LONG_TERM: Persistent facts, survives agent restarts
 * STEERING: Rules and constraints (MoSCoW)
 */
public enum MemoryType {
  SHORT_TERM("Ephemeral working memory"),
  LONG_TERM("Persistent facts and knowledge"),
  STEERING("Rules, constraints, and priorities");

  private final String description;

  MemoryType(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }
}

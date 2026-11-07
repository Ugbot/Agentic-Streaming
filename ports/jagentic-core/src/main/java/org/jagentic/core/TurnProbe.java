package org.jagentic.core;

import java.io.Serializable;

/**
 * A seam a test uses to stop a turn at a named point (the fixtures' {@code crash_at}). It observes
 * and must not alter anything; only a test has a reason to throw from it.
 */
public interface TurnProbe extends Serializable {
  /** Does nothing. An enum constant, so it stays the same object through serialization. */
  TurnProbe NONE = Nothing.INSTANCE;

  /** The probe that observes nothing. */
  enum Nothing implements TurnProbe {
    INSTANCE
  }

  default void beforeAppend(String eventType) {}

  default void afterAppend(String eventType) {}

  /** About to run a tool. */
  default void beforeCall(String toolId) {}

  /** The tool has run and its outcome is not yet journaled. */
  default void duringCall(String toolId) {}

  /** The outcome is journaled and not yet logged. */
  default void afterCall(String toolId) {}
}

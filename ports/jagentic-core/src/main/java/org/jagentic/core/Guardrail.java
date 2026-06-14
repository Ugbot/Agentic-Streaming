package org.jagentic.core;

/**
 * Screens the inbound user text and/or the outbound reply. Returning a non-empty reason
 * blocks the turn (the RoutedGraph short-circuits with an {@code ok=false}
 * {@code [blocked]} reply). Portable analogue of the Flink {@code Guardrail} /
 * {@code BankingScreening}.
 */
public interface Guardrail {
  /** @return a block reason for the inbound text, or {@code null} to allow. */
  default String checkInput(String text) {
    return null;
  }

  /** @return a block reason for the outbound reply, or {@code null} to allow. */
  default String checkOutput(String reply) {
    return null;
  }
}

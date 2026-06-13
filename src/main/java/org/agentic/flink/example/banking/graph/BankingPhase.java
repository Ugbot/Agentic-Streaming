package org.agentic.flink.example.banking.graph;

/**
 * The cross-turn workflow phase for a banking conversation, held in per-{@code contextId} keyed
 * state and advanced by {@link BankingVerifierFunction}. The router reads it to pick the next path;
 * the conversation chains forward through these phases until the task's condition is met. This is
 * how "operators chain to meet the conditions" without a Flink cycle — the phase carries the
 * progress across A2A turns.
 */
public enum BankingPhase {
  /** Fresh session — nothing gathered yet. */
  NEW,
  /** Product/policy facts obtained; still missing details from the user. */
  NEED_INFO,
  /** All required details present — ready to perform the action. */
  READY_TO_ACT,
  /** The environment action has been performed. */
  ACTED,
  /** The task is complete (terminal). */
  DONE,
  /** Escalated to a human / refused (terminal). */
  ESCALATED
}

package org.agentic.flink.example.banking.graph;

/**
 * The specialized operator path a banking turn is routed to by {@link BankingRouterFunction}.
 * Each non-terminal path is a keyed operator running a focused brain; the router emits each turn to
 * exactly one path's side output.
 */
public enum BankingPath {
  /** Answer a knowledge/policy question from the KB (CS agent). */
  KNOWLEDGE,
  /** Ask the user for missing required details (personal agent). */
  GATHER,
  /** Consult the bank's customer service for product facts (personal agent). */
  DELEGATE,
  /** Perform a guarded environment action (apply/submit/transfer/...). */
  ACTION,
  /** Handle a dispute/chargeback via policy (CS agent). */
  DISPUTE,
  /** Escalate to a human / safe refusal. */
  ESCALATE,
  /** Screening blocked the turn — emit a safe refusal without running a path. */
  REFUSE
}

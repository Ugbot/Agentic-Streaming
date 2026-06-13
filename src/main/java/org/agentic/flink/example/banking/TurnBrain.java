package org.agentic.flink.example.banking;

import java.io.Serializable;
import org.agentic.flink.example.banking.safety.RoutingBudget;

/**
 * The "agent brain" for one banking turn — the part that actually reasons and acts, separated from
 * the safety/routing shell ({@link BankingTurnFunction}) so it can be swapped: a Gemini-backed
 * {@code AgentExecutor} in production, a deterministic stub in tests.
 *
 * <p>The brain must consult the supplied {@link RoutingBudget} before each internal step / customer-
 * service round-trip and stop gracefully when a gate denies — that is what keeps a turn from running
 * into the harness timeout. The framework also enforces the budget at the {@link
 * BankingTurnContext} boundary as a backstop.
 */
@FunctionalInterface
public interface TurnBrain extends Serializable {

  /** Produce the reply text for one user turn. Implementations may call {@code ctx} to act. */
  String respond(String userText, BankingTurnContext ctx);
}

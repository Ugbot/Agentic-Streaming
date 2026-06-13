package org.agentic.flink.example.banking;

import java.io.Serializable;
import java.util.List;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;

/**
 * The "agent brain" for one banking turn — the part that actually reasons and acts, separated from
 * the safety/routing shell ({@link BankingTurnFunction}, {@link
 * org.agentic.flink.example.banking.graph.BankingPathFunction}) so it can be swapped: a Gemini-backed
 * {@link ReActTurnBrain} in production, a deterministic stub in tests.
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

  /**
   * Produce the reply given the prior multi-turn dialogue (ending with the current user turn) — used
   * by the routed graph so a path brain sees the conversation from {@link
   * org.agentic.flink.example.banking.graph.ConversationMemory}. The default delegates to {@link
   * #respond} on the latest user message (enough for deterministic stubs); {@link ReActTurnBrain}
   * overrides it to replay the whole transcript into the model.
   */
  default String converse(List<ChatMessage> conversation, BankingTurnContext ctx) {
    String last = "";
    if (conversation != null) {
      for (int i = conversation.size() - 1; i >= 0; i--) {
        ChatMessage m = conversation.get(i);
        if (m.getRole() == ChatRole.USER) {
          last = m.getContent();
          break;
        }
      }
    }
    return respond(last, ctx);
  }
}

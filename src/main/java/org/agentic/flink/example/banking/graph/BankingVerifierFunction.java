package org.agentic.flink.example.banking.graph;

import java.util.List;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.llm.ChatMessage;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The terminal operator of a banking agent graph: the rule-based (no-LLM) verifier. Keyed by A2A
 * {@code contextId}, it takes the path's {@link BankingTurn}, advances the shared {@link
 * BankingPhase} in the {@link PhaseStore} based on which path ran and whether an env action fired,
 * records the agent's reply into the shared {@link ConversationMemory} (so the next turn's path
 * brain sees it), and builds the {@link A2AResponse} the bridge returns to the caller — preserving
 * the {@code taskId}/{@code contextId}.
 *
 * <p>Keeping this LLM-free keeps the per-turn model-call count flat (one path brain per turn),
 * which matters under the hackathon's single-key rate limit. The phase it advances is what makes a
 * multi-turn task <em>chain</em> forward: e.g. {@code NEW →(DELEGATE)→ NEED_INFO →(GATHER)→
 * READY_TO_ACT →(ACTION+actionPerformed)→ ACTED → DONE}.
 */
public final class BankingVerifierFunction
    extends KeyedProcessFunction<String, BankingTurn, A2AResponse> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(BankingVerifierFunction.class);

  @Override
  public void processElement(BankingTurn turn, Context ctx, Collector<A2AResponse> out) {
    String contextId = turn.getContextId();
    BankingPhase current = PhaseStore.get(contextId);
    BankingPhase next = advance(current, turn);
    if (next != current) {
      PhaseStore.set(contextId, next);
      LOG.debug(
          "Verifier ctx {} path {} : phase {} -> {} (action={})",
          contextId,
          turn.getPath(),
          current,
          next,
          turn.isActionPerformed());
    }

    String reply =
        turn.getReplyText() == null || turn.getReplyText().isBlank()
            ? "I'm sorry — I wasn't able to produce a response. Please try again."
            : turn.getReplyText();

    // Record the agent's reply so the next turn's path brain sees the full dialogue. The router
    // already appended the user's message for this turn.
    ConversationMemory.append(contextId, ChatMessage.assistant(reply));

    A2AArtifact artifact = A2AArtifact.text("reply", "reply", reply);
    out.collect(A2AResponse.completed(turn.getTaskId(), contextId, List.of(artifact)));
  }

  /**
   * Pure phase-transition rule (extracted for unit testing). Advances the workflow phase from the
   * path that just ran and whether it performed an environment action.
   */
  public static BankingPhase advance(BankingPhase current, BankingTurn turn) {
    BankingPath path = turn.getPath();
    if (path == null) {
      return current;
    }
    switch (path) {
      case ESCALATE:
      case REFUSE:
        return BankingPhase.ESCALATED;
      case DELEGATE:
        // Pulled product/policy facts from CS; now we need details from the user.
        return current == BankingPhase.NEW ? BankingPhase.NEED_INFO : current;
      case KNOWLEDGE:
      case DISPUTE:
        // Informational turn — no workflow progress, but a fresh session is now under way.
        return current == BankingPhase.NEW ? BankingPhase.NEED_INFO : current;
      case GATHER:
        // Asked for / received the missing details — ready to act next.
        return BankingPhase.READY_TO_ACT;
      case ACTION:
        // Only advance to ACTED if the env action actually fired; otherwise we still need info.
        if (turn.isActionPerformed()) {
          return BankingPhase.DONE;
        }
        return current == BankingPhase.READY_TO_ACT ? BankingPhase.READY_TO_ACT : BankingPhase.NEED_INFO;
      default:
        return current;
    }
  }
}

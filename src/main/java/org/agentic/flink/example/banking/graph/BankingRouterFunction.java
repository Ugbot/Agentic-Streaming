package org.agentic.flink.example.banking.graph;

import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.example.banking.safety.BankingScreening;
import org.agentic.flink.screening.ScreeningResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The triage router: the first operator of a banking agent graph. Keyed by A2A {@code contextId},
 * it screens the inbound message, reads the conversation {@link BankingPhase} from the shared {@link
 * PhaseStore}, and uses the LLM-free {@link BankingClassifier} to label the turn with a {@link
 * BankingPath}. The labelled {@link BankingTurn} is fanned out to the matching path operator by
 * {@link org.agentic.flink.graph.RoutedAgentGraph}.
 *
 * <p>Screening {@code BLOCK} short-circuits to the {@code REFUSE} path with a safe message — the
 * threat never reaches a path brain. The router does not advance the phase (that's the verifier's
 * job); it only reads it from the {@link PhaseStore} the verifier writes.
 */
public final class BankingRouterFunction
    extends KeyedProcessFunction<String, A2ARequest, BankingTurn> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(BankingRouterFunction.class);

  private final String agentId;
  private final boolean personal;

  private transient BankingScreening screening;

  public BankingRouterFunction(String agentId, boolean personal) {
    this.agentId = agentId;
    this.personal = personal;
  }

  @Override
  public void open(Configuration parameters) {
    screening = BankingScreening.defaults();
  }

  @Override
  public void processElement(A2ARequest req, Context ctx, Collector<BankingTurn> out)
      throws Exception {
    String contextId = req.getContextId() != null ? req.getContextId() : req.getTaskId();
    String userText = req.getMessage() == null ? "" : req.getMessage().textContent();
    long now = ctx.timerService().currentProcessingTime();

    BankingTurn turn = BankingTurn.of(req.getTaskId(), contextId, agentId, userText);

    // Record the user's turn in the shared transcript before routing, so whichever path runs sees
    // the full multi-turn dialogue.
    ConversationMemory.append(contextId, org.agentic.flink.llm.ChatMessage.user(userText));

    ScreeningResult screen = screening.screen(contextId, userText, now);
    if ("BLOCK".equals(screen.verdict)) {
      LOG.info("Screening BLOCK ctx {}: {}", contextId, screen.reason);
      turn.setBlocked(true);
      turn.setPath(BankingPath.REFUSE);
      turn.setReplyText("I'm sorry, I can't help with that request.");
      turn.setRouteReason("screening:BLOCK");
      out.collect(turn);
      return;
    }

    BankingPhase phase = PhaseStore.get(contextId);
    BankingPath path = BankingClassifier.classify(personal, phase, userText);
    turn.setPath(path);
    turn.setRouteReason("phase=" + phase + " -> " + path);
    LOG.debug("Router ctx {} phase {} -> path {}", contextId, phase, path);
    out.collect(turn);
  }
}

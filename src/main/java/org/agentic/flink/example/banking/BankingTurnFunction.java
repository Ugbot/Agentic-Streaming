package org.agentic.flink.example.banking;
import org.apache.flink.api.common.functions.OpenContext;

import java.util.List;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.example.banking.env.EnvSession;
import org.agentic.flink.example.banking.safety.BankingScreening;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.agentic.flink.screening.ScreeningResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The safety/routing shell for one banking agent, as a Flink operator keyed by A2A {@code
 * contextId}. Turns an inbound {@link A2ARequest} into an {@link A2AResponse} by:
 *
 * <ol>
 *   <li>binding the per-session {@link RoutingBudget} from keyed state (fresh sessions get defaults);
 *   <li>screening the inbound message — {@code BLOCK} → safe refusal, never reaching the brain;
 *   <li>binding the {@code contextId} on the thread ({@link EnvSession}) and running the {@link
 *       TurnBrain} with a budget-gated {@link BankingTurnContext};
 *   <li>persisting the updated budget.
 * </ol>
 *
 * <p>Keying by {@code contextId} gives free per-session isolation (the harness's statelessness rule)
 * and lets the {@link RoutingBudget} bound the personal↔CS loop across turns — the anti-explosion
 * guarantee. The brain is pluggable (Gemini-backed in production, a stub in tests).
 */
public final class BankingTurnFunction
    extends KeyedProcessFunction<String, A2ARequest, A2AResponse> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(BankingTurnFunction.class);

  private final String agentId;
  private final TurnBrain brain;
  private final BankingTurnContext.CustomerServiceClient cs; // nullable for the CS agent
  private final int maxRoundTrips;
  private final int maxIterations;
  private final long turnDeadlineMs;
  private final int dedupeWindow;

  // RoutingBudget is @TypeInfo-annotated (JSON via FlinkJson), so it's a first-class keyed-state
  // type — no manual byte[] / Kryo workaround needed.
  private transient ValueState<RoutingBudget> budgetState;
  private transient BankingScreening screening;

  public BankingTurnFunction(
      String agentId,
      TurnBrain brain,
      BankingTurnContext.CustomerServiceClient cs,
      int maxRoundTrips,
      int maxIterations,
      long turnDeadlineMs,
      int dedupeWindow) {
    this.agentId = agentId;
    this.brain = java.util.Objects.requireNonNull(brain, "brain");
    this.cs = cs;
    this.maxRoundTrips = maxRoundTrips;
    this.maxIterations = maxIterations;
    this.turnDeadlineMs = turnDeadlineMs;
    this.dedupeWindow = dedupeWindow;
  }

  @Override
  public void open(OpenContext openContext) {
    budgetState =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("routing-budget-" + agentId, RoutingBudget.class));
    screening = BankingScreening.defaults();
  }

  @Override
  public void processElement(A2ARequest req, Context ctx, Collector<A2AResponse> out)
      throws Exception {
    String contextId = req.getContextId() != null ? req.getContextId() : req.getTaskId();
    long now = ctx.timerService().currentProcessingTime();

    RoutingBudget budget = budgetState.value();
    if (budget == null) {
      budget = new RoutingBudget(maxRoundTrips, maxIterations, turnDeadlineMs, dedupeWindow);
    }
    budget.startTurn(now);

    String userText = req.getMessage() == null ? "" : req.getMessage().textContent();

    // 1) Threat screening — BLOCK never reaches the brain.
    ScreeningResult screen = screening.screen(contextId, userText, now);
    if ("BLOCK".equals(screen.verdict)) {
      LOG.info("Screening BLOCK for ctx {} ({})", contextId, screen.reason);
      out.collect(reply(req, "I can't help with that request."));
      budgetState.update(budget);
      return;
    }

    // 2) Run the brain with a budget-gated action surface, contextId bound for env tools.
    final RoutingBudget b = budget;
    final long t = now;
    String replyText =
        EnvSession.withContext(
            contextId,
            () -> brain.respond(userText, new BankingTurnContext(contextId, b, t, cs)));

    out.collect(reply(req, replyText));
    budgetState.update(budget);
  }

  private A2AResponse reply(A2ARequest req, String text) {
    A2AArtifact artifact =
        A2AArtifact.text(java.util.UUID.randomUUID().toString(), "reply", text == null ? "" : text);
    return A2AResponse.completed(req.getTaskId(), req.getContextId(), List.of(artifact));
  }
}

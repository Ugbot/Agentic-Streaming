package org.agentic.flink.example.banking.graph;

import org.agentic.flink.example.banking.BankingTurnContext;
import org.agentic.flink.example.banking.TurnBrain;
import org.agentic.flink.example.banking.env.EnvSession;
import org.agentic.flink.example.banking.env.TurnSignals;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * One path operator of a banking agent graph: runs the path's focused {@link ReActTurnBrain}
 * (path-specific prompt + tool subset) for the turn, bounded by the per-{@code contextId} {@link
 * RoutingBudget}, within an {@link EnvSession} (contextId for env calls) and a {@link TurnSignals}
 * scope (so a performed env action is detected for the verifier). The reply + action signal are
 * written onto the {@link BankingTurn} and passed to the verifier.
 *
 * <p>A null brain (the {@code REFUSE} path, where the router already set a safe reply) passes the
 * turn through untouched.
 */
public final class BankingPathFunction
    extends KeyedProcessFunction<String, BankingTurn, BankingTurn> {
  private static final long serialVersionUID = 1L;

  private final BankingPath path;
  private final TurnBrain brain; // nullable (REFUSE pass-through)
  private final BankingTurnContext.CustomerServiceClient cs; // nullable
  private final int maxRoundTrips;
  private final int maxIterations;
  private final long turnDeadlineMs;
  private final int dedupeWindow;

  private transient ValueState<byte[]> budgetState;

  public BankingPathFunction(
      BankingPath path,
      TurnBrain brain,
      BankingTurnContext.CustomerServiceClient cs,
      int maxRoundTrips,
      int maxIterations,
      long turnDeadlineMs,
      int dedupeWindow) {
    this.path = path;
    this.brain = brain;
    this.cs = cs;
    this.maxRoundTrips = maxRoundTrips;
    this.maxIterations = maxIterations;
    this.turnDeadlineMs = turnDeadlineMs;
    this.dedupeWindow = dedupeWindow;
  }

  @Override
  public void open(Configuration parameters) {
    budgetState =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>(
                    "banking-budget-" + path, TypeInformation.of(byte[].class)));
  }

  @Override
  public void processElement(BankingTurn turn, Context ctx, Collector<BankingTurn> out)
      throws Exception {
    if (brain == null || turn.isBlocked()) {
      out.collect(turn); // REFUSE / already-answered — straight to the verifier
      return;
    }

    long now = ctx.timerService().currentProcessingTime();
    RoutingBudget budget = deserialize(budgetState.value());
    if (budget == null) {
      budget = new RoutingBudget(maxRoundTrips, maxIterations, turnDeadlineMs, dedupeWindow);
    }
    budget.startTurn(now);

    final String contextId = turn.getContextId();
    final RoutingBudget b = budget;
    // The path brain sees the full multi-turn dialogue (the router appended this turn's user
    // message), so a split-operator agent still progresses across turns.
    final java.util.List<org.agentic.flink.llm.ChatMessage> history =
        ConversationMemory.history(contextId);
    TurnSignals.Result<String> result =
        TurnSignals.capture(
            () ->
                EnvSession.withContext(
                    contextId,
                    () -> brain.converse(history, new BankingTurnContext(contextId, b, now, cs))));

    turn.setReplyText(result.value);
    turn.setActionPerformed(result.actionPerformed);
    budgetState.update(serialize(budget));
    out.collect(turn);
  }

  private static byte[] serialize(RoutingBudget budget) {
    try (java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
      oos.writeObject(budget);
      oos.flush();
      return bos.toByteArray();
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to persist RoutingBudget", e);
    }
  }

  private static RoutingBudget deserialize(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try (java.io.ObjectInputStream ois =
        new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))) {
      return (RoutingBudget) ois.readObject();
    } catch (java.io.IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to read RoutingBudget", e);
    }
  }
}

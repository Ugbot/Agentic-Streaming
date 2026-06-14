package org.agentic.flink.example.banking.graph;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.agentic.flink.a2a.bridge.A2ABridge;
import org.agentic.flink.a2a.bridge.A2AJsonTypeInfo;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.example.banking.BankingAgentSetup;
import org.agentic.flink.example.banking.BankingTurnContext;
import org.agentic.flink.example.banking.TurnBrain;
import org.agentic.flink.graph.RoutedAgentGraph;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * Assembles one banking agent as a <b>routed multi-operator Flink graph</b> over an {@link
 * A2ABridge}: the bridge's request channel feeds a rule-based {@link BankingRouterFunction} (screen
 * + classify, no LLM), which tags each turn with a {@link BankingPath}; {@link RoutedAgentGraph}
 * fans each turn to that path's {@link BankingPathFunction} (a focused {@link
 * org.agentic.flink.example.banking.ReActTurnBrain} from {@link BankingAgentSetup#brainFor}); the
 * outputs merge into the rule-based {@link BankingVerifierFunction}, which advances the cross-turn
 * {@link BankingPhase} and emits the {@link A2AResponse} back onto the bridge's response sink.
 *
 * <p>One A2A turn = Router → Path → Verifier (a clean DAG, no Flink cycle); multi-step <em>chaining</em>
 * happens across turns via the shared {@link PhaseStore}/{@link ConversationMemory}. All operators
 * are keyed by A2A {@code contextId} so concurrent sessions are isolated. Reserving the LLM for the
 * path brains (router/verifier are rule-based) keeps the model-call count per turn flat.
 */
public final class BankingAgentGraph {

  /** Repeated-tool-call dedupe window per turn (matches the single-operator default). */
  private static final int DEDUPE_WINDOW = 8;

  private BankingAgentGraph() {}

  /**
   * Wire the role's graph from {@code bridge.requestChannel()} to {@code bridge.responseSink()} on
   * {@code env}. Call {@code env.execute(...)} afterwards to run it.
   */
  public static void wire(StreamExecutionEnvironment env, A2ABridge bridge, BankingAgentSetup setup) {
    wire(
        env,
        bridge,
        setup.role(),
        setup::brainFor,
        setup.cs(),
        setup.maxRoundTrips(),
        setup.maxIterations(),
        setup.turnDeadlineMs());
  }

  /**
   * Brain-provider overload — the wiring of record, decoupled from {@link BankingAgentSetup} so
   * tests can inject deterministic per-path {@link TurnBrain} stubs (and so any brain source works).
   *
   * @param brains path → brain ({@code null} brain = pass-through, e.g. REFUSE)
   */
  public static void wire(
      StreamExecutionEnvironment env,
      A2ABridge bridge,
      BankingAgentSetup.Role role,
      Function<BankingPath, TurnBrain> brains,
      BankingTurnContext.CustomerServiceClient cs,
      int maxRoundTrips,
      int maxIterations,
      long turnDeadlineMs) {
    boolean personal = role == BankingAgentSetup.Role.PERSONAL;
    String agentId = personal ? "personal" : "cs";

    DataStream<A2ARequest> requests;
    try {
      requests = bridge.requestChannel().open(env);
    } catch (Exception e) {
      throw new RuntimeException("Failed to open A2A request channel", e);
    }

    TypeInformation<BankingTurn> turnType = TypeInformation.of(BankingTurn.class);

    DataStream<BankingTurn> routed =
        requests
            .keyBy(A2ARequest::key)
            .process(new BankingRouterFunction(agentId, personal))
            .name("router:" + agentId)
            .returns(turnType);

    // One keyed path operator per BankingPath (REFUSE = pass-through; brain null), so any router
    // decision matches a branch. Unused-by-role paths are simply never routed to.
    Map<String, KeyedProcessFunction<String, BankingTurn, BankingTurn>> pathFns =
        new LinkedHashMap<>();
    List<String> paths = new ArrayList<>();
    for (BankingPath p : BankingPath.values()) {
      pathFns.put(
          p.name(),
          new BankingPathFunction(
              p, brains.apply(p), cs, maxRoundTrips, maxIterations, turnDeadlineMs, DEDUPE_WINDOW));
      paths.add(p.name());
    }

    DataStream<A2AResponse> responses =
        RoutedAgentGraph.wire(
            routed,
            paths,
            BankingTurn::pathName,
            BankingTurn::getContextId,
            pathFns,
            new BankingVerifierFunction(),
            turnType,
            A2AJsonTypeInfo.of(A2AResponse.class));

    responses.sinkTo(bridge.responseSink()).name("a2a-response-sink");
  }
}

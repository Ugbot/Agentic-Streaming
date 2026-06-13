package org.agentic.flink.example.banking.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.a2a.bridge.InProcA2ABridge;
import org.agentic.flink.example.banking.BankingAgentSetup;
import org.agentic.flink.example.banking.BankingTurnContext;
import org.agentic.flink.example.banking.TurnBrain;
import org.agentic.flink.example.banking.env.TurnSignals;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test of the routed banking graph on a Flink minicluster over the in-proc A2A bridge:
 * the rule-based router fans turns to per-path stub brains and the rule-based verifier advances the
 * cross-turn {@link BankingPhase}. Asserts routing, multi-turn phase progression NEW→…→DONE, the
 * {@link org.agentic.flink.example.banking.safety.RoutingBudget} bound, and taskId correlation —
 * all without a real LLM (deterministic {@link TurnBrain} stubs).
 */
final class BankingAgentGraphTest {

  private static final AtomicInteger CS_CALLS = new AtomicInteger();

  private JobClient job;

  @AfterEach
  void tearDown() throws Exception {
    if (job != null) {
      job.cancel().get();
      job = null;
    }
    InProcA2ABridge.Hub.reset();
  }

  /** Per-path stub: replies "PATH:<name>"; the ACTION path also "performs" a mutating env tool. */
  private static Function<BankingPath, TurnBrain> markerBrains() {
    return path ->
        (TurnBrain)
            (userText, ctx) -> {
              if (path == BankingPath.ACTION) {
                TurnSignals.recordEnvToolCall("apply_for_credit_card", false);
              }
              return "PATH:" + path.name();
            };
  }

  private static A2ARequest request(String contextId, String text) {
    return new A2ARequest(
        UUID.randomUUID().toString(),
        contextId,
        "agent",
        A2AMessage.userText(UUID.randomUUID().toString(), text),
        false,
        null,
        null);
  }

  private StreamExecutionEnvironment localEnv() {
    return StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
  }

  @Test
  @DisplayName("personal: NEW→DELEGATE→NEED_INFO→GATHER→READY_TO_ACT→ACTION→DONE across turns")
  void personalActionFlowChainsToDone() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("req-" + UUID.randomUUID(), "resp-" + UUID.randomUUID());
    String ctx = "ctx-" + UUID.randomUUID();
    PhaseStore.clear(ctx);
    ConversationMemory.clear(ctx);

    try (A2AGatewayConnector connector = bridge.openGateway()) {
      StreamExecutionEnvironment env = localEnv();
      BankingAgentGraph.wire(
          env, bridge, BankingAgentSetup.Role.PERSONAL, markerBrains(), null, 4, 12, 60_000L);
      job = env.executeAsync("banking-graph-personal");
      Thread.sleep(300); // let the source start

      // Turn 1: a product question while NEW -> DELEGATE; verifier advances NEW -> NEED_INFO.
      A2ARequest t1 = request(ctx, "which credit card is best for me?");
      connector.publishRequest(t1);
      A2AResponse r1 = connector.awaitFinal(t1.getTaskId(), 20_000);
      assertNotNull(r1, "no response for turn 1");
      assertEquals(t1.getTaskId(), r1.getTaskId());
      assertEquals(A2ATaskState.COMPLETED, r1.getState());
      assertTrue(reply(r1).contains("DELEGATE"), reply(r1));
      assertEquals(BankingPhase.NEED_INFO, PhaseStore.get(ctx));

      // Turn 2: user supplies details while NEED_INFO -> GATHER; advances -> READY_TO_ACT.
      A2ARequest t2 = request(ctx, "my name is Dana Lee and I earn 60000 a year");
      connector.publishRequest(t2);
      A2AResponse r2 = connector.awaitFinal(t2.getTaskId(), 20_000);
      assertTrue(reply(r2).contains("GATHER"), reply(r2));
      assertEquals(BankingPhase.READY_TO_ACT, PhaseStore.get(ctx));

      // Turn 3: user says proceed while READY_TO_ACT -> ACTION; env action fires -> DONE.
      A2ARequest t3 = request(ctx, "go ahead and apply for it");
      connector.publishRequest(t3);
      A2AResponse r3 = connector.awaitFinal(t3.getTaskId(), 20_000);
      assertEquals(t3.getTaskId(), r3.getTaskId());
      assertTrue(reply(r3).contains("ACTION"), reply(r3));
      assertEquals(BankingPhase.DONE, PhaseStore.get(ctx), "performed action should reach DONE");
    }
  }

  @Test
  @DisplayName("cs: a knowledge question routes to KNOWLEDGE and returns the right taskId")
  void csKnowledgeRouting() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("req-" + UUID.randomUUID(), "resp-" + UUID.randomUUID());
    String ctx = "ctx-" + UUID.randomUUID();
    PhaseStore.clear(ctx);
    ConversationMemory.clear(ctx);

    try (A2AGatewayConnector connector = bridge.openGateway()) {
      StreamExecutionEnvironment env = localEnv();
      BankingAgentGraph.wire(
          env, bridge, BankingAgentSetup.Role.CS, markerBrains(), null, 4, 12, 60_000L);
      job = env.executeAsync("banking-graph-cs");
      Thread.sleep(300);

      A2ARequest t = request(ctx, "what is the cash back rate and annual fee on the Blue card?");
      connector.publishRequest(t);
      A2AResponse r = connector.awaitFinal(t.getTaskId(), 20_000);
      assertNotNull(r);
      assertEquals(t.getTaskId(), r.getTaskId());
      assertTrue(reply(r).contains("KNOWLEDGE"), reply(r));
    }
  }

  @Test
  @DisplayName("a runaway path brain is bounded by the RoutingBudget; the turn still completes")
  void runawayBoundedByBudget() throws Exception {
    CS_CALLS.set(0);
    InProcA2ABridge bridge =
        new InProcA2ABridge("req-" + UUID.randomUUID(), "resp-" + UUID.randomUUID());
    String ctx = "ctx-" + UUID.randomUUID();
    PhaseStore.clear(ctx);
    ConversationMemory.clear(ctx);

    int maxRoundTrips = 3;
    // The personal agent's NEW turn routes to DELEGATE; this DELEGATE brain loops on CS forever.
    Function<BankingPath, TurnBrain> runaway =
        path ->
            (TurnBrain)
                (userText, ctx2) -> {
                  int calls = 0;
                  while (!ctx2.budgetExhausted()) {
                    String resp = ctx2.askCustomerService("escalate: " + userText);
                    if (resp.startsWith("[customer-service round-trip budget")) {
                      break;
                    }
                    if (++calls > 1000) {
                      break; // test safety net
                    }
                  }
                  return "handled after " + calls + " cs round-trip(s)";
                };
    BankingTurnContext.CustomerServiceClient cs =
        (cid, msg) -> {
          CS_CALLS.incrementAndGet();
          return "cs reply";
        };

    try (A2AGatewayConnector connector = bridge.openGateway()) {
      StreamExecutionEnvironment env = localEnv();
      BankingAgentGraph.wire(
          env, bridge, BankingAgentSetup.Role.PERSONAL, runaway, cs, maxRoundTrips, 50, 60_000L);
      job = env.executeAsync("banking-graph-runaway");
      Thread.sleep(300);

      A2ARequest t = request(ctx, "which account should I open?");
      connector.publishRequest(t);
      A2AResponse r = connector.awaitFinal(t.getTaskId(), 30_000);
      assertNotNull(r, "runaway turn must still complete");
      assertEquals(A2ATaskState.COMPLETED, r.getState());
      assertEquals(maxRoundTrips, CS_CALLS.get(), "CS round-trips must be capped by the budget");
      assertTrue(reply(r).contains("handled after " + maxRoundTrips), reply(r));
    }
  }

  private static String reply(A2AResponse r) {
    return r.getArtifacts().isEmpty() ? "" : r.getArtifacts().get(0).textContent();
  }
}

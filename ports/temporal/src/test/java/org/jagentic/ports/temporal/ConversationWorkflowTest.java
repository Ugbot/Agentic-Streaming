package org.jagentic.ports.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Agent;
import org.jagentic.core.Banking;
import org.jagentic.core.Brain;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;

import org.jagentic.ports.temporal.TurnMessages.TurnReply;
import org.jagentic.ports.temporal.TurnMessages.TurnRequest;

/**
 * Runs the banking entity workflow on an in-memory {@link TestWorkflowEnvironment} (no
 * external Temporal server): proves routing + durable, event-sourced state across turns
 * (C1), and — the extensibility check — that an EXTENDED core graph (a new path + tool,
 * built only from public jagentic-core abstractions) flows through the same workflow
 * with no change to the workflow class, injected via a worker factory. The core
 * counterpart is {@code org.jagentic.core.ExtensibilityTest}.
 */
class ConversationWorkflowTest {

  private static final String TQ = "agentic-test";
  private TestWorkflowEnvironment env;

  @BeforeEach
  void setUp() {
    env = TestWorkflowEnvironment.newInstance();
  }

  @AfterEach
  void tearDown() {
    env.close();
  }

  private ConversationWorkflow startConversation(String cid) {
    WorkflowOptions opts = WorkflowOptions.newBuilder().setTaskQueue(TQ).setWorkflowId(cid).build();
    ConversationWorkflow wf = env.getWorkflowClient().newWorkflowStub(ConversationWorkflow.class, opts);
    WorkflowClient.start(wf::run);
    return wf;
  }

  @Test
  void routesPersistsStateAndCallsToolAcrossTurns() {
    Worker worker = env.newWorker(TQ);
    worker.registerWorkflowImplementationTypes(ConversationWorkflowImpl.class);
    env.start();

    ConversationWorkflow c1 = startConversation("c1");
    TurnReply t1 = c1.turn(new TurnRequest("what card types do you offer?", "demo"));
    TurnReply t2 = c1.turn(new TurnRequest("tell me about crypto cash-back", "demo"));
    assertEquals("cards", t1.path);
    assertEquals("cards", t2.path);
    assertTrue(t1.ok);
    // C1: the same durable execution accumulated both turns (user+assistant x2).
    assertEquals(4, t2.messageCount);
    assertEquals(4, c1.messageCount()); // via @QueryMethod

    ConversationWorkflow c2 = startConversation("c2");
    TurnReply bal = c2.turn(new TurnRequest("what is my balance?", "demo"));
    assertEquals("payments", bal.path);
    assertTrue(bal.reply.contains("1234.56")); // the get_balance tool executed
  }

  @Test
  void extendedCoreGraphFlowsThroughTheWorkflow() {
    ToolRegistry tools = Banking.defaultTools()
        .register("freeze_card", "Freeze the user's card", p -> "FRZ-" + p.get("user"));
    Brain fraud = (userText, ctx) -> {
      Object ref = ctx.callTool("freeze_card", Map.of("user", ctx.userId));
      return "[fraud] Your card is frozen (ref " + ref + ").";
    };
    Map<String, Agent> paths = new LinkedHashMap<>();
    paths.put("cards", new Agent("cards", "cards", new Banking.RuleBrain("cards")));
    paths.put("payments", new Agent("payments", "payments", new Banking.RuleBrain("payments")));
    paths.put("general", new Agent("general", "general", new Banking.RuleBrain("general")));
    paths.put("fraud", new Agent("fraud", "fraud", fraud));
    RoutedGraph.Router router = (event, ctx) -> {
      String low = event.text().toLowerCase();
      return (low.contains("stolen") || low.contains("freeze")) ? "fraud" : Banking.router(event, ctx);
    };
    RoutedGraph extended = new RoutedGraph(router, paths,
        (reply, ctx) -> new RoutedGraph.Verifier.Result(reply.startsWith("["), reply));

    Worker worker = env.newWorker(TQ);
    // Inject the extended graph by registering a factory for the workflow impl.
    worker.registerWorkflowImplementationFactory(
        ConversationWorkflow.class,
        () -> new ConversationWorkflowImpl(extended, tools, Banking.retriever()));
    env.start();

    ConversationWorkflow cf = startConversation("c-fraud");
    TurnReply res = cf.turn(new TurnRequest("my card was stolen, please freeze it", "alice"));
    assertEquals("fraud", res.path);
    assertTrue(res.reply.contains("FRZ-alice")); // the new tool executed

    // original paths still routable through the same injected graph
    ConversationWorkflow ok = startConversation("c-ok");
    assertTrue(ok.turn(new TurnRequest("what is my balance?", "alice")).reply.contains("1234.56"));
  }
}

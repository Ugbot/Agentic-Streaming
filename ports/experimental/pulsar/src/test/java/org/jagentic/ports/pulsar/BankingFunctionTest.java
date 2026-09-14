package org.jagentic.ports.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pulsar.functions.api.Context;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Agent;
import org.jagentic.core.AgentContext;
import org.jagentic.core.Banking;
import org.jagentic.core.Brain;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;

/**
 * Adapter-level checks for the Pulsar Function port: that the banking essence runs on
 * the Pulsar {@link Context} state seam, that state is durable across turns (C1), and
 * — the decisive extensibility check — that an EXTENDED core graph (a new path + a new
 * tool built only from the public jagentic-core abstractions) flows through this same
 * adapter with no change to the seam. The core counterpart is
 * {@code org.jagentic.core.ExtensibilityTest}.
 */
class BankingFunctionTest {

  /** One in-memory Pulsar Context per turn, sharing the given state map. */
  private static String process(BankingFunction fn, Map<String, byte[]> state,
                                String cid, String text, String userId) {
    Context ctx = InMemoryContext.create(InMemoryContext.record(cid, text, userId), state);
    return fn.process(text, ctx);
  }

  private static String persistedPath(Map<String, byte[]> state, String cid) {
    Context probe = InMemoryContext.create(InMemoryContext.record(cid, "", ""), state);
    return new PulsarStateConversationStore(BankingFunction.stateBytes(probe))
        .getAttribute(cid, RoutedGraph.PATH_ATTR).orElse(null);
  }

  @Test
  void runsBankingGraphAndPersistsStateAcrossTurns() {
    BankingFunction fn = new BankingFunction(); // default = shared Banking essence
    Map<String, byte[]> state = new ConcurrentHashMap<>();

    String r1 = process(fn, state, "c1", "what card types do you offer?", "demo");
    String r2 = process(fn, state, "c2", "what is my balance?", "demo");
    String r3 = process(fn, state, "c1", "tell me about crypto cash-back", "demo");

    assertTrue(r1.startsWith("[cards]"));
    assertTrue(r2.contains("1234.56"));
    assertTrue(r3.startsWith("[cards]"));
    assertEquals("cards", persistedPath(state, "c1"));
    assertEquals("payments", persistedPath(state, "c2"));

    // C1: c1's two turns (user+assistant x2) are recovered from the Pulsar state store.
    Context probe = InMemoryContext.create(InMemoryContext.record("c1", "", ""), state);
    assertEquals(4, new PulsarStateConversationStore(BankingFunction.stateBytes(probe)).messageCount("c1"));
  }

  @Test
  void extendedCoreGraphFlowsThroughThePulsarSeam() {
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

    BankingFunction fn = new BankingFunction(extended, tools, Banking.retriever());
    Map<String, byte[]> state = new ConcurrentHashMap<>();

    String reply = process(fn, state, "c-fraud", "my card was stolen, please freeze it", "alice");
    assertTrue(reply.startsWith("[fraud]"), reply);
    assertTrue(reply.contains("FRZ-alice"), reply);            // the new tool executed
    assertEquals("fraud", persistedPath(state, "c-fraud"));    // new path persisted via Pulsar state

    // original paths still routable through the same injected graph
    assertTrue(process(fn, state, "c-ok", "what is my balance?", "alice").contains("1234.56"));
    assertEquals("payments", persistedPath(state, "c-ok"));
  }
}

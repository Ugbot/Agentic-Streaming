package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Extensibility guard for jagentic-core — the Java mirror of
 * {@code pyagentic/tests/test_extensibility.py}.
 *
 * <p>Every JVM adapter (Kafka Streams, Pekko, Pulsar, Spring, Quarkus) consumes the
 * core factories ({@code Banking.buildGraph()/defaultTools()/retriever()}) and runs
 * {@code RoutedGraph.handle} — none reimplements routing, tools, or retrieval. So a
 * new tool / new path added through the public API must "just work", and by
 * construction every adapter then inherits it. This test adds a brand-new
 * {@code freeze_card} tool and a brand-new {@code fraud} path using only the public
 * abstractions (no framework edits) and proves the core routes to and invokes them.
 * The adapter-level counterpart lives in the pulsar module's {@code BankingFunctionTest}.
 */
class ExtensibilityTest {

  /** The core's default tools PLUS a brand-new one — registered via the public API. */
  static ToolRegistry extendedTools() {
    return Banking.defaultTools()
        .register("freeze_card", "Freeze the user's card", p -> "FRZ-" + p.get("user"));
  }

  /** A new brain added purely from the public {@link Brain} interface; it calls the new tool. */
  static final class FraudBrain implements Brain {
    @Override
    public String turn(String userText, AgentContext ctx) {
      Object ref = ctx.callTool("freeze_card", Map.of("user", ctx.userId));
      return "[fraud] Your card is frozen (ref " + ref + "). A specialist will call you.";
    }
  }

  /** The banking graph PLUS a brand-new 'fraud' path, with a router that prefers it.
   * Reuses the framework's {@link RoutedGraph}/{@link Agent} verbatim. */
  static RoutedGraph extendedGraph() {
    RoutedGraph base = Banking.buildGraph();
    Map<String, Agent> paths = new LinkedHashMap<>();
    // Re-declare the banking paths (the public factory doesn't expose its map) ...
    paths.put("cards", new Agent("cards", "You answer card questions.", new Banking.RuleBrain("cards")));
    paths.put("payments", new Agent("payments", "You answer payment questions.", new Banking.RuleBrain("payments")));
    paths.put("general", new Agent("general", "You answer general questions.", new Banking.RuleBrain("general")));
    // ... and add the new one.
    paths.put("fraud", new Agent("fraud", "You handle fraud and stolen cards.", new FraudBrain()));

    RoutedGraph.Router router = (event, ctx) -> {
      String low = event.text().toLowerCase();
      if (low.contains("stolen") || low.contains("fraud") || low.contains("freeze")) {
        return "fraud";
      }
      return Banking.router(event, ctx); // delegate to the framework's router otherwise
    };
    RoutedGraph.Verifier verifier =
        (reply, ctx) -> new RoutedGraph.Verifier.Result(reply != null && reply.startsWith("["), reply);
    return new RoutedGraph(router, paths, verifier);
  }

  private LocalRuntime runtime() {
    return new LocalRuntime(extendedGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), extendedTools(), Banking.retriever());
  }

  @Test
  void newPathAndToolReachableViaPublicApi() {
    LocalRuntime rt = runtime();
    String cid = "c-" + UUID.randomUUID();
    TurnResult res = rt.submit(new Event(cid, "alice", "my card was stolen, please freeze it"));
    assertEquals("fraud", res.path);
    assertTrue(res.ok);
    assertTrue(res.reply.toLowerCase().contains("frozen"));
    assertTrue(res.toolCalls.contains("freeze_card"));
    assertTrue(res.reply.contains("FRZ-alice")); // the new tool actually executed
    // the framework persists the routed path/phase regardless of which path is new
    assertEquals("fraud", rt.store().getAttribute(cid, "graph.path").orElseThrow());
    assertEquals("done", rt.store().getAttribute(cid, "graph.phase").orElseThrow());
  }

  @Test
  void existingPathsStillWorkAfterExtension() {
    LocalRuntime rt = runtime();
    assertEquals("cards", rt.submit(new Event("c1", "bob", "what card types do you offer?")).path);
    TurnResult pay = rt.submit(new Event("c2", "bob", "what is my balance?"));
    assertEquals("payments", pay.path);
    assertTrue(pay.toolCalls.contains("get_balance")); // original tool untouched
    assertEquals("general", rt.submit(new Event("c3", "bob", "hello there")).path);
  }
}

package org.jagentic.ports.pekko;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Agent;
import org.jagentic.core.Banking;
import org.jagentic.core.Brain;
import org.jagentic.core.Event;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;

import org.jagentic.ports.pekko.ConversationActor.Command;
import org.jagentic.ports.pekko.ConversationActor.ProcessTurn;
import org.jagentic.ports.pekko.ConversationActor.Reply;

/** Extended-graph-through-the-Pekko-seam test: a new fraud path + tool, injected into
 * ConversationActor, runs end-to-end via the ask pattern on a real (single-node) actor. */
class ConversationActorTest {

  private static final ActorTestKit TESTKIT = ActorTestKit.create();

  @AfterAll
  static void shutdown() {
    TESTKIT.shutdownTestKit();
  }

  private Reply ask(ActorRef<Command> actor, String cid, String text) {
    TestProbe<Reply> probe = TESTKIT.createTestProbe(Reply.class);
    actor.tell(new ProcessTurn(new Event(cid, "alice", text), probe.getRef()));
    return probe.receiveMessage();
  }

  @Test
  void defaultBankingGraphRoutes() {
    ActorRef<Command> actor = TESTKIT.spawn(ConversationActor.create("c1"));
    Reply r = ask(actor, "c1", "what is my balance?");
    assertEquals("payments", r.path());
    assertTrue(r.reply().contains("1234.56"));
  }

  @Test
  void extendedGraphFlowsThroughThePekkoSeam() {
    ToolRegistry tools = Banking.defaultTools()
        .register("freeze_card", "Freeze the user's card", p -> "FRZ-" + p.get("user"));
    Brain fraud = (userText, ctx) -> {
      Object ref = ctx.callTool("freeze_card", Map.of("user", ctx.userId));
      return "[fraud] Your card is frozen (ref " + ref + ").";
    };
    Map<String, Agent> paths = new LinkedHashMap<>();
    paths.put("cards", new Agent("cards", "c", new Banking.RuleBrain("cards")));
    paths.put("payments", new Agent("payments", "p", new Banking.RuleBrain("payments")));
    paths.put("general", new Agent("general", "g", new Banking.RuleBrain("general")));
    paths.put("fraud", new Agent("fraud", "f", fraud));
    RoutedGraph extended = new RoutedGraph(
        (ev, ctx) -> {
          String low = ev.text().toLowerCase();
          return (low.contains("stolen") || low.contains("freeze")) ? "fraud" : Banking.router(ev, ctx);
        },
        paths,
        (reply, ctx) -> new RoutedGraph.Verifier.Result(reply.startsWith("["), reply));

    ActorRef<Command> actor =
        TESTKIT.spawn(ConversationActor.create("c-fraud", extended, tools, Banking.retriever()));
    Reply r = ask(actor, "c-fraud", "my card was stolen, please freeze it");
    assertEquals("fraud", r.path());
    assertTrue(r.reply().contains("FRZ-alice"), r.reply());
  }
}

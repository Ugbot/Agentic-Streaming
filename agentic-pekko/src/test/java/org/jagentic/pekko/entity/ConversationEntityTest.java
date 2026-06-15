package org.jagentic.pekko.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.typesafe.config.ConfigFactory;

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
import org.jagentic.pekko.runtime.AgentDeps;

import org.jagentic.pekko.entity.ConversationEntity.Command;
import org.jagentic.pekko.entity.ConversationEntity.GetState;
import org.jagentic.pekko.entity.ConversationEntity.ProcessTurn;
import org.jagentic.pekko.entity.ConversationEntity.StateSnapshot;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;

/** The event-sourced conversation entity: routing through the banking graph, multi-turn
 * transcript accumulation, and an extended graph injected through the Pekko seam — all on a
 * real (single-node) actor with the in-memory persistence journal. */
class ConversationEntityTest {

  private static final ActorTestKit TESTKIT = ActorTestKit.create(ConfigFactory.parseString(
      "pekko.persistence.journal.plugin = \"pekko.persistence.journal.inmem\"\n"
          + "pekko.persistence.snapshot-store.plugin = \"pekko.persistence.snapshot-store.local\"\n"
          + "pekko.persistence.snapshot-store.local.dir = \"target/pekko-snapshots-test\"\n"));

  @AfterAll
  static void shutdown() {
    TESTKIT.shutdownTestKit();
  }

  private TurnReply ask(ActorRef<Command> actor, String cid, String text) {
    return askWithId(actor, UUID.randomUUID().toString(), cid, text);
  }

  private TurnReply askWithId(ActorRef<Command> actor, String turnId, String cid, String text) {
    TestProbe<TurnReply> probe = TESTKIT.createTestProbe(TurnReply.class);
    actor.tell(new ProcessTurn(turnId, new Event(cid, "alice", text), probe.getRef()));
    return probe.receiveMessage();
  }

  private StateSnapshot state(ActorRef<Command> actor) {
    TestProbe<StateSnapshot> probe = TESTKIT.createTestProbe(StateSnapshot.class);
    actor.tell(new GetState(probe.getRef()));
    return probe.receiveMessage();
  }

  /** Single-path graph whose brain counts invocations — to prove the pipeline is NOT re-run on
   * recovery and IS skipped on a deduped turn. */
  private static AgentDeps countingDeps(AtomicInteger counter) {
    Brain counting = (text, ctx) -> {
      counter.incrementAndGet();
      return "[count] " + text;
    };
    Map<String, Agent> paths = new LinkedHashMap<>();
    paths.put("count", new Agent("count", "counts turns", counting));
    RoutedGraph g = new RoutedGraph(
        (ev, ctx) -> "count",
        paths,
        (reply, ctx) -> new RoutedGraph.Verifier.Result(true, reply));
    return new AgentDeps(g, Banking.defaultTools(), Banking.retriever());
  }

  @Test
  void defaultBankingGraphRoutes() {
    ActorRef<Command> actor = TESTKIT.spawn(ConversationEntity.create("c1", AgentDeps.banking()));
    TurnReply r = ask(actor, "c1", "what is my balance?");
    assertEquals("payments", r.path());
    assertTrue(r.reply().contains("1234.56"), r.reply());
  }

  @Test
  void multiTurnAccumulatesPersistedTranscript() {
    ActorRef<Command> actor = TESTKIT.spawn(ConversationEntity.create("c2", AgentDeps.banking()));
    ask(actor, "c2", "what card types do you offer?");
    ask(actor, "c2", "tell me about crypto cash-back");
    // 2 turns × (user + assistant) = 4 messages, durably committed via TurnCommitted events
    assertEquals(4, state(actor).messageCount());
  }

  @Test
  void recoversTranscriptWithoutReplayingThePipeline() {
    AtomicInteger turns = new AtomicInteger();
    AgentDeps deps = countingDeps(turns);

    ActorRef<Command> first = TESTKIT.spawn(ConversationEntity.create("rec", deps));
    ask(first, "rec", "one");
    ask(first, "rec", "two");
    assertEquals(2, turns.get());
    assertEquals(4, state(first).messageCount());
    TESTKIT.stop(first);

    // Same persistenceId → the entity recovers by replaying TurnCommitted events.
    ActorRef<Command> recovered = TESTKIT.spawn(ConversationEntity.create("rec", deps));
    assertEquals(4, state(recovered).messageCount(), "transcript must survive restart");
    assertEquals(2, turns.get(), "the LLM-calling pipeline must NOT re-run during recovery");
  }

  @Test
  void duplicateTurnIdIsDeduped() {
    AtomicInteger turns = new AtomicInteger();
    ActorRef<Command> actor = TESTKIT.spawn(ConversationEntity.create("dedupe", countingDeps(turns)));
    TurnReply r1 = askWithId(actor, "fixed-turn-1", "dedupe", "hello");
    TurnReply r2 = askWithId(actor, "fixed-turn-1", "dedupe", "hello again");
    assertEquals(1, turns.get(), "the second (same turnId) must be deduped, not re-run");
    assertEquals(r1.reply(), r2.reply());
  }

  @Test
  void extendedGraphFlowsThroughTheSeam() {
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

    AgentDeps deps = new AgentDeps(extended, tools, Banking.retriever());
    ActorRef<Command> actor = TESTKIT.spawn(ConversationEntity.create("c-fraud", deps));
    TurnReply r = ask(actor, "c-fraud", "my card was stolen, please freeze it");
    assertEquals("fraud", r.path());
    assertTrue(r.reply().contains("FRZ-alice"), r.reply());
  }
}

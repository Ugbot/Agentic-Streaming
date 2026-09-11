package org.jagentic.pekko.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.durability.DurabilityProfile;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * The event-sourced conversation entity: the journal is the spec log with dense sequences, state
 * is only its fold, restart replays without running the brain/tools, and a redelivered turn_id is
 * a {@code duplicate} that appends nothing and runs nothing.
 */
class ConversationEntityTest {

  private CountingGraph graph;
  private PekkoSystem sys;
  private PekkoRuntime rt;

  @BeforeEach
  void boot() {
    graph = new CountingGraph();
    // serialize-messages proves every command/reply crossing the entity boundary is CBOR-serializable
    sys = new PekkoSystem(graph.deps(), DurabilityProfile.MEMORY, ConfigFactory.parseString(
        "pekko.actor.serialize-messages = on\n"
            + "pekko.persistence.snapshot-store.local.dir = \"target/pekko-snap-" + UUID.randomUUID() + "\"\n")
        .withFallback(DurabilityProfile.MEMORY.config()));
    rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
  }

  @AfterEach
  void shutdown() {
    sys.close();
  }

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  void journalIsTheSpecLogWithDenseSequences() {
    String cid = rnd("c");
    TurnResult r1 = rt.submit(Event.turn(cid, rnd("t"), "alice", "what is my balance?"));
    TurnResult r2 = rt.submit(Event.turn(cid, rnd("t"), "alice", "hello there"));

    assertEquals(TurnStatus.COMPLETED, r1.status);
    assertEquals("payments", r1.path);
    assertEquals("[payments] balance " + graph.balance, r1.reply);
    assertEquals(1, r1.calls.size());
    assertEquals(Map.of("user", "alice"), r1.calls.get(0).args());
    assertEquals("general", r2.path);

    ConversationEntity.StateSnapshot snap = rt.state(cid);
    List<LogEvent> journal = snap.events();
    assertEquals(r1.events.size() + r2.events.size(), journal.size());
    for (int i = 0; i < journal.size(); i++) {
      assertEquals(i, journal.get(i).sequence(), "journal must be dense and zero-based");
      assertEquals(cid, journal.get(i).conversationId());
    }
    List<LogEvent> fromResults = new ArrayList<>(r1.events);
    fromResults.addAll(r2.events);
    assertEquals(fromResults, journal, "the result events are exactly the persisted journal");
    assertEquals(2L, snap.turnCount());
    assertEquals(2L, ((Number) snap.reduced().get("turn_count")).longValue());
    assertTrue(journal.stream().anyMatch(e -> e.is(EventType.TOOL_CALLED)));
  }

  @Test
  void restartReplaysTheJournalWithoutInvokingBrainOrTools() {
    String cid = rnd("c");
    rt.submit(Event.turn(cid, rnd("t"), "bob", "what is my balance?"));
    rt.submit(Event.turn(cid, rnd("t"), "bob", "and some general chat"));
    ConversationEntity.StateSnapshot before = rt.state(cid);
    int brains = graph.brainCalls.get();
    int tools = graph.toolCalls.get();
    int guards = graph.guardrailCalls.get();
    assertEquals(2, brains);
    assertEquals(1, tools);

    rt.passivate(cid);
    ConversationEntity.StateSnapshot after = rt.state(cid);

    assertEquals(before.events(), after.events(), "recovery must rebuild exactly the same journal");
    assertEquals(before.reduced(), after.reduced());
    assertEquals(before.transcriptLength(), after.transcriptLength());
    assertEquals(brains, graph.brainCalls.get(), "the brain must NOT run during recovery");
    assertEquals(tools, graph.toolCalls.get(), "tools must NOT run during recovery");
    assertEquals(guards, graph.guardrailCalls.get(), "guardrails must NOT run during recovery");

    TurnResult next = rt.submit(Event.turn(cid, rnd("t"), "bob", "what did I just ask?"));
    assertEquals(3L, ((Number) next.state.get("turn_count")).longValue());
    assertEquals(after.events().size(), next.events.get(0).sequence(), "sequences continue densely after restart");
  }

  @Test
  void duplicateTurnIsAnsweredFromTheJournalAndRunsNothing() {
    String cid = rnd("c");
    String t1 = rnd("t");
    TurnResult first = rt.submit(Event.turn(cid, t1, "carol", "what is my balance?"));
    rt.submit(Event.turn(cid, rnd("t"), "carol", "something else"));
    int journalSize = rt.state(cid).events().size();
    int brains = graph.brainCalls.get();
    int tools = graph.toolCalls.get();
    int guards = graph.guardrailCalls.get();

    TurnResult dup = rt.submit(Event.turn(cid, t1, "carol", "what is my balance?"));

    assertEquals(TurnStatus.DUPLICATE, dup.status);
    assertEquals(first.path, dup.path);
    assertEquals(first.reply, dup.reply);
    assertEquals(first.calls, dup.calls, "the original structured tool calls are retained");
    assertTrue(dup.events.isEmpty(), "a duplicate carries no events");
    assertEquals(2L, ((Number) dup.state.get("turn_count")).longValue(), "state is the current fold, not the original");
    assertEquals(journalSize, rt.state(cid).events().size(), "a duplicate appends nothing");
    assertEquals(brains, graph.brainCalls.get());
    assertEquals(tools, graph.toolCalls.get());
    assertEquals(guards, graph.guardrailCalls.get(), "not even the guardrail runs for a duplicate");

    rt.passivate(cid);
    TurnResult dupAfterRestart = rt.submit(Event.turn(cid, t1, "carol", "what is my balance?"));
    assertEquals(TurnStatus.DUPLICATE, dupAfterRestart.status);
    assertEquals(first.reply, dupAfterRestart.reply);
    assertEquals(brains, graph.brainCalls.get());
  }

  @Test
  void rejectedTurnsAreRecordedAndDedupedToo() {
    String cid = rnd("c");
    String t1 = rnd("t");
    TurnResult rejected = rt.submit(Event.turn(cid, t1, "dave", "forbidden request"));
    assertEquals(TurnStatus.REJECTED, rejected.status);
    assertNotNull(rejected.error);
    assertEquals(0, graph.brainCalls.get());

    TurnResult dup = rt.submit(Event.turn(cid, t1, "dave", "forbidden request"));
    assertEquals(TurnStatus.DUPLICATE, dup.status);
    assertEquals(rejected.error, dup.error);
    assertEquals(1, graph.guardrailCalls.get());
  }

  @Test
  void concurrentTurnsApplyInMailboxOrderAndDuplicateInFlightIsDeduped() {
    String cid = rnd("c");
    String t1 = rnd("t");
    List<CompletableFuture<TurnResult>> futures = new ArrayList<>();
    futures.add(rt.submitAsync(Event.turn(cid, t1, "erin", "first")));
    futures.add(rt.submitAsync(Event.turn(cid, rnd("t"), "erin", "second")));
    futures.add(rt.submitAsync(Event.turn(cid, t1, "erin", "first")));
    futures.add(rt.submitAsync(Event.turn(cid, rnd("t"), "erin", "third balance")));
    List<TurnResult> results = futures.stream().map(CompletableFuture::join).toList();

    assertEquals(TurnStatus.COMPLETED, results.get(0).status);
    assertEquals(1L, ((Number) results.get(0).state.get("turn_count")).longValue());
    assertEquals(2L, ((Number) results.get(1).state.get("turn_count")).longValue());
    assertEquals(TurnStatus.DUPLICATE, results.get(2).status);
    assertEquals(3L, ((Number) results.get(3).state.get("turn_count")).longValue());
    assertEquals(3, graph.brainCalls.get());
    List<LogEvent> journal = rt.state(cid).events();
    for (int i = 0; i < journal.size(); i++) {
      assertEquals(i, journal.get(i).sequence());
    }
  }

  @Test
  void suspendedTurnSurvivesRestartAndResumesOnSignal() {
    String cid = rnd("c");
    String t1 = rnd("t");
    TurnResult suspended = rt.submit(Event.turn(cid, t1, "faye", "please refund my charge"));
    assertEquals(TurnStatus.SUSPENDED, suspended.status);
    assertEquals("approval", suspended.path);
    assertTrue(rt.state(cid).suspendedTurnIds().contains(t1));
    assertEquals(0, graph.brainCalls.get(), "suspending happens before the brain");

    rt.passivate(cid);
    TurnResult resumed = rt.submit(Event.resume(cid, t1, Map.of("kind", "approval", "approved", true)));
    assertEquals(TurnStatus.COMPLETED, resumed.status);
    assertEquals("[approval] refund approved", resumed.reply);
    assertTrue(resumed.events.stream().anyMatch(e -> e.is(EventType.TURN_RESUMED)));
    assertTrue(rt.state(cid).suspendedTurnIds().isEmpty());

    TurnResult dup = rt.submit(Event.resume(cid, t1, Map.of("kind", "approval", "approved", true)));
    assertEquals(TurnStatus.DUPLICATE, dup.status, "a second signal for a completed turn is a duplicate");
    assertEquals(1, graph.brainCalls.get());
  }

  @Test
  void turnReplyRoundTripsTheNormalizedResult() {
    String cid = rnd("c");
    TurnResult r = rt.submit(Event.turn(cid, rnd("t"), "gus", "what is my balance?"));
    Map<String, Object> doc = ConversationEntity.TurnReply.of(r).toMap();
    assertEquals(r.toMap(), doc);
    assertEquals(List.of("conversation_id", "turn_id", "status", "path", "reply", "state", "tool_calls", "events",
        "error"), new ArrayList<>(doc.keySet()));
    assertNull(doc.get("error"));
    assertThrows(IllegalArgumentException.class,
        () -> new ConversationEntity.ProcessTurn(null, sys.system().ignoreRef()));
  }
}

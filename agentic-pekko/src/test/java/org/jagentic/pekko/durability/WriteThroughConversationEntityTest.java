package org.jagentic.pekko.durability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity.Command;
import org.jagentic.pekko.entity.ConversationEntity.GetState;
import org.jagentic.pekko.entity.ConversationEntity.ProcessTurn;
import org.jagentic.pekko.entity.ConversationEntity.StateSnapshot;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;
import org.jagentic.pekko.runtime.AgentDeps;

/** The Redis write-through entity, exercised against an in-memory {@link ConversationStore} as a
 * stand-in for Redis (no broker): turns route correctly, write through to the durable store, and a
 * fresh entity over the SAME store sees the prior transcript — the write-through durability
 * contract. (The live Redis round-trip is an opt-in integration test.) */
class WriteThroughConversationEntityTest {

  private static final ActorTestKit TESTKIT = ActorTestKit.create();

  @AfterAll
  static void shutdown() {
    TESTKIT.shutdownTestKit();
  }

  private TurnReply ask(ActorRef<Command> actor, String cid, String text) {
    TestProbe<TurnReply> probe = TESTKIT.createTestProbe(TurnReply.class);
    actor.tell(new ProcessTurn(UUID.randomUUID().toString(), new Event(cid, "u", text), probe.getRef()));
    return probe.receiveMessage();
  }

  @Test
  void writesTranscriptThroughToTheDurableStore() {
    // shared "durable" store (stands in for RedisConversationStore)
    ConversationStore durable = new ConversationStore.InMemory();
    AgentDeps deps = AgentDeps.banking();

    ActorRef<Command> entity =
        TESTKIT.spawn(WriteThroughConversationEntity.create("wt1", deps, durable));
    TurnReply r = ask(entity, "wt1", "what is my balance?");
    assertEquals("payments", r.path());
    assertTrue(r.reply().contains("1234.56"), r.reply());
    ask(entity, "wt1", "tell me about crypto cash-back");

    // the transcript is in the durable store, not the actor's heap
    assertEquals(4, durable.messageCount("wt1"));

    // a brand-new entity over the same store sees the prior transcript (write-through continuity)
    ActorRef<Command> fresh =
        TESTKIT.spawn(WriteThroughConversationEntity.create("wt1", deps, durable));
    TestProbe<StateSnapshot> probe = TESTKIT.createTestProbe(StateSnapshot.class);
    fresh.tell(new GetState(probe.getRef()));
    assertEquals(4, probe.receiveMessage().messageCount());
  }
}

package org.jagentic.pekko.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;

import com.typesafe.config.ConfigFactory;

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Join;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.entity.ConversationEntity.Command;
import org.jagentic.pekko.entity.ConversationEntity.StateSnapshot;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * Forms a single-node Pekko cluster and proves turns route through Cluster Sharding (the production
 * distributed single-writer) to one event-sourced entity per conversation id, keeping the explicit
 * {@code turn_id}, the normalized reply, duplicate detection and the folded state.
 */
class ConversationShardingTest {

  private static final String CONF =
      "pekko.actor.provider = cluster\n"
          + "pekko.actor.serialize-messages = on\n"
          + "pekko.remote.artery.canonical.hostname = \"127.0.0.1\"\n"
          + "pekko.remote.artery.canonical.port = 0\n"
          + "pekko.persistence.journal.plugin = \"pekko.persistence.journal.inmem\"\n"
          + "pekko.persistence.snapshot-store.plugin = \"pekko.persistence.snapshot-store.local\"\n"
          + "pekko.persistence.snapshot-store.local.dir = \"target/pekko-snap-shard-" + UUID.randomUUID() + "\"\n";

  private static ActorTestKit kit;
  private static CountingGraph graph;

  @BeforeAll
  static void formClusterAndInitSharding() throws Exception {
    kit = ActorTestKit.create("AgenticPekko", ConfigFactory.parseString(CONF)
        .withFallback(ConfigFactory.load("application.conf")));
    Cluster cluster = Cluster.get(kit.system());
    cluster.manager().tell(Join.create(cluster.selfMember().address()));
    long deadline = System.currentTimeMillis() + 15_000L;
    while (!cluster.selfMember().status().equals(MemberStatus.up())
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    assertEquals(MemberStatus.up(), cluster.selfMember().status(), "single-node cluster did not form");
    graph = new CountingGraph();
    ConversationSharding.init(kit.system(), graph.deps());
  }

  @AfterAll
  static void shutdown() {
    if (kit != null) {
      kit.shutdownTestKit();
    }
  }

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  void routesTurnsThroughClusterShardingWithExplicitTurnIds() {
    String cid = rnd("s");
    String t1 = rnd("t");
    EntityRef<Command> ref = ConversationSharding.entityRef(kit.system(), cid);
    TestProbe<TurnReply> probe = kit.createTestProbe(TurnReply.class);

    ref.tell(new ConversationEntity.ProcessTurn(Event.turn(cid, t1, "u", "what is my balance?"), probe.getRef()));
    TurnReply r = probe.receiveMessage(Duration.ofSeconds(15));
    assertEquals(cid, r.conversationId());
    assertEquals(t1, r.turnId(), "the caller's turn_id is preserved across the shard region");
    assertEquals(TurnStatus.COMPLETED, r.status());
    assertEquals("payments", r.path());
    assertTrue(r.reply().contains(Double.toString(graph.balance)), r.reply());
    assertEquals(1L, ((Number) r.state().get("turn_count")).longValue());
    for (int i = 0; i < r.events().size(); i++) {
      assertEquals(i, r.events().get(i).sequence());
    }

    int brains = graph.brainCalls.get();
    ref.tell(new ConversationEntity.ProcessTurn(Event.turn(cid, t1, "u", "what is my balance?"), probe.getRef()));
    TurnReply dup = probe.receiveMessage(Duration.ofSeconds(15));
    assertEquals(TurnStatus.DUPLICATE, dup.status());
    assertEquals(r.reply(), dup.reply());
    assertTrue(dup.events().isEmpty());
    assertEquals(brains, graph.brainCalls.get(), "a redelivered turn through sharding runs no brain");

    TestProbe<StateSnapshot> stateProbe = kit.createTestProbe(StateSnapshot.class);
    ref.tell(new ConversationEntity.GetState(stateProbe.getRef()));
    StateSnapshot snap = stateProbe.receiveMessage(Duration.ofSeconds(15));
    assertEquals(r.events(), snap.events(), "the sharded entity's journal is exactly the turn's events");
  }

  @Test
  void conversationsAreIsolatedEntities() {
    String a = rnd("s");
    String b = rnd("s");
    TestProbe<TurnReply> probe = kit.createTestProbe(TurnReply.class);
    ConversationSharding.entityRef(kit.system(), a)
        .tell(new ConversationEntity.ProcessTurn(Event.turn(a, rnd("t"), "u", "hello"), probe.getRef()));
    ConversationSharding.entityRef(kit.system(), a)
        .tell(new ConversationEntity.ProcessTurn(Event.turn(a, rnd("t"), "u", "again"), probe.getRef()));
    ConversationSharding.entityRef(kit.system(), b)
        .tell(new ConversationEntity.ProcessTurn(Event.turn(b, rnd("t"), "u", "hi"), probe.getRef()));
    long aCount = 0;
    long bCount = 0;
    for (int i = 0; i < 3; i++) {
      TurnReply r = probe.receiveMessage(Duration.ofSeconds(15));
      if (r.conversationId().equals(a)) {
        aCount = Math.max(aCount, ((Number) r.state().get("turn_count")).longValue());
      } else {
        bCount = ((Number) r.state().get("turn_count")).longValue();
      }
    }
    assertEquals(2L, aCount);
    assertEquals(1L, bCount);
  }
}

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
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.entity.ConversationEntity.Command;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;
import org.jagentic.pekko.runtime.AgentDeps;

/** Forms a single-node Pekko cluster and proves turns route through Cluster Sharding (the
 * production distributed single-writer) to an event-sourced entity per conversationId. */
class ConversationShardingTest {

  private static final String CONF =
      "pekko.actor.provider = cluster\n"
          + "pekko.remote.artery.canonical.hostname = \"127.0.0.1\"\n"
          + "pekko.remote.artery.canonical.port = 0\n"
          + "pekko.persistence.journal.plugin = \"pekko.persistence.journal.inmem\"\n"
          + "pekko.persistence.snapshot-store.plugin = \"pekko.persistence.snapshot-store.local\"\n"
          + "pekko.persistence.snapshot-store.local.dir = \"target/pekko-snap-shard\"\n";

  private static ActorTestKit kit;

  @BeforeAll
  static void formClusterAndInitSharding() throws Exception {
    kit = ActorTestKit.create("AgenticPekko", ConfigFactory.parseString(CONF));
    Cluster cluster = Cluster.get(kit.system());
    cluster.manager().tell(Join.create(cluster.selfMember().address()));
    long deadline = System.currentTimeMillis() + 15_000L;
    while (!cluster.selfMember().status().equals(MemberStatus.up())
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    ConversationSharding.init(kit.system(), AgentDeps.banking());
  }

  @AfterAll
  static void shutdown() {
    if (kit != null) {
      kit.shutdownTestKit();
    }
  }

  @Test
  void routesTurnsThroughClusterSharding() {
    EntityRef<Command> ref = ConversationSharding.entityRef(kit.system(), "s1");
    TestProbe<TurnReply> probe = kit.createTestProbe(TurnReply.class);
    ref.tell(new ConversationEntity.ProcessTurn(
        UUID.randomUUID().toString(), new Event("s1", "u", "what is my balance?"), probe.getRef()));
    TurnReply r = probe.receiveMessage(Duration.ofSeconds(15));
    assertEquals("payments", r.path());
    assertTrue(r.reply().contains("1234.56"), r.reply());
  }
}

package org.jagentic.pekko;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.typed.Cluster;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.cluster.ConversationSharding;
import org.jagentic.pekko.durability.DurabilityProfile;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * The cluster entrypoint: {@link PekkoSystem#clustered} forms a single-node cluster on the chosen
 * durability profile, registers the {@code Conversation} entity type and routes every
 * {@link PekkoRuntime} turn through the shard region, and {@link ClusterMain} exposes that from the
 * command line. Loopback and an ephemeral Artery port, no external services.
 */
class ClusterMainTest {

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static final Config LOOPBACK = ConfigFactory.parseString(
      "pekko.remote.artery.canonical.hostname = \"127.0.0.1\"\n"
      + "pekko.remote.artery.canonical.port = 0\n"
      + "pekko.cluster.jmx.multi-mbeans-in-same-jvm = on\n");

  @Test
  void clusteredSystemRoutesTurnsThroughTheShardedEntity() throws Exception {
    CountingGraph graph = new CountingGraph();
    try (PekkoSystem sys = PekkoSystem.clustered(graph.deps(), DurabilityProfile.MEMORY, LOOPBACK)) {
      assertTrue(sys.sharded());
      assertEquals(DurabilityProfile.MEMORY, sys.profile());
      assertEquals("pekko.persistence.journal.inmem", sys.journalPlugin());
      assertEquals("cluster", sys.system().settings().config().getString("pekko.actor.provider"));
      Cluster cluster = Cluster.get(sys.system());
      assertEquals(MemberStatus.up(), cluster.selfMember().status());
      assertEquals(1, cluster.state().getMembers().spliterator().getExactSizeIfKnown());

      String cid = rnd("c");
      String tid = rnd("t");
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(15));
      TurnResult r = rt.submit(Event.turn(cid, tid, "u", "what is my balance?"));
      assertTrue(r.ok, r.reply);
      assertEquals(tid, r.turnId);
      assertEquals("payments", r.path);
      assertEquals(1, graph.brainCalls.get());

      // The turn lives in the sharded entity: ask the shard region directly for its journal.
      ConversationEntity.StateSnapshot snap = AskPattern.<ConversationEntity.Command, ConversationEntity.StateSnapshot>ask(
              ConversationSharding.entityRef(sys.system(), cid),
              ConversationEntity.GetState::new, Duration.ofSeconds(15), sys.system().scheduler())
          .toCompletableFuture().get(20, TimeUnit.SECONDS);
      assertEquals(1L, snap.turnCount());
      assertEquals(cid, snap.conversationId());
      assertFalse(snap.events().isEmpty());
      assertTrue(snap.events().stream().anyMatch(e -> tid.equals(e.turnId())), "the turn's events are in the sharded journal");
      assertNotNull(ClusterSharding.get(sys.system()).shardState());

      // Redelivery through the same route dedupes in the sharded entity.
      TurnResult dup = rt.submit(Event.turn(cid, tid, "u", "what is my balance?"));
      assertEquals(TurnStatus.DUPLICATE, dup.status);
      assertEquals(r.reply, dup.reply);
      assertEquals(1, graph.brainCalls.get());

      // Passivation goes through the shard; the entity recovers from the journal on the next turn.
      rt.passivate(cid);
      TurnResult after = rt.submit(Event.turn(cid, rnd("t"), "u", "thanks"));
      assertTrue(after.ok, after.reply);
      assertEquals(2L, rt.state(cid).turnCount());
      assertEquals(2, graph.brainCalls.get());
    }
  }

  @Test
  void mainRunsTheDemoTurnsOnASingleNodeClusterAndExits() throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    ClusterMain.Options opts = ClusterMain.Options.parse(
        new String[] {"--profile", "memory", "--host", "127.0.0.1", "--port", "0"});
    assertEquals(DurabilityProfile.MEMORY, opts.profile());
    assertEquals("127.0.0.1", opts.host());
    assertEquals(0, opts.port());
    assertNull(opts.text());
    assertEquals(-1, opts.httpPort());

    List<TurnResult> results = ClusterMain.run(opts, new PrintStream(buf, true, StandardCharsets.UTF_8), new CountDownLatch(0));

    assertEquals(4, results.size());
    assertTrue(results.stream().allMatch(r -> r.ok), results.toString());
    String out = buf.toString(StandardCharsets.UTF_8);
    assertTrue(out.contains("status=Up"), out);
    assertTrue(out.contains("sharded=true"), out);
    assertTrue(out.contains("journal=pekko.persistence.journal.inmem"), out);
    assertTrue(out.contains("[c1] path="), out);
  }

  @Test
  void mainRefusesAnUnconfiguredDurableProfileBeforeFormingACluster() {
    ClusterMain.Options opts = ClusterMain.Options.parse(new String[] {"--profile", "postgres", "--port", "0"});
    assertEquals(DurabilityProfile.POSTGRES, opts.profile());
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> ClusterMain.run(opts, new PrintStream(new ByteArrayOutputStream()), new CountDownLatch(0)));
    assertTrue(e.getMessage().contains("AGENTIC_PG_URL"), e.getMessage());
  }

  @Test
  void optionsRejectUnknownFlags() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> ClusterMain.Options.parse(new String[] {"--" + rnd("flag")}));
    assertTrue(e.getMessage().contains("--profile"), e.getMessage());
  }
}

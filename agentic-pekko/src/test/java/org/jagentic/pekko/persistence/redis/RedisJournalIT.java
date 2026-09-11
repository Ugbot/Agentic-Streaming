package org.jagentic.pekko.persistence.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.durability.DurabilityProfile;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.testing.CountingGraph;
import org.jagentic.pekko.testing.RedisContainer;

import redis.clients.jedis.Jedis;

/**
 * The REDIS profile against a real Redis (Testcontainers on Podman): the guarantees the entity
 * tests prove on the in-memory journal must hold unchanged when the journal is Redis, and the
 * profile must refuse a cache-only server. Run with {@code -P integration-tests}.
 */
@Tag("integration")
class RedisJournalIT {

  private static RedisContainer redis;

  @BeforeAll
  static void startRedis() {
    redis = RedisContainer.durable();
    redis.start();
  }

  @AfterAll
  static void stopRedis() {
    redis.stop();
  }

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static void awaitUntil(BooleanSupplier cond, Duration max) throws InterruptedException {
    long deadline = System.nanoTime() + max.toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("condition not met within " + max);
      }
      Thread.sleep(25);
    }
  }

  /** A fresh actor system on the REDIS profile sharing {@code prefix}: a JVM restart, as far as the journal can tell. */
  private static PekkoSystem boot(CountingGraph graph, String prefix) {
    return new PekkoSystem(graph.deps(), DurabilityProfile.REDIS, redis.profileConfig(prefix));
  }

  private static void assertDense(List<LogEvent> events) {
    for (int i = 0; i < events.size(); i++) {
      assertEquals(i, events.get(i).sequence(), "journal must be dense at " + i);
    }
  }

  @Test
  void profileSelectsTheRedisJournalAndPersistsDenseEvents() {
    String prefix = rnd("p");
    CountingGraph graph = new CountingGraph();
    try (PekkoSystem sys = boot(graph, prefix)) {
      assertEquals("agentic-redis-journal", sys.journalPlugin());
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
      String cid = rnd("c");
      int turns = ThreadLocalRandom.current().nextInt(2, 6);
      for (int i = 0; i < turns; i++) {
        TurnResult r = rt.submit(Event.turn(cid, rnd("t"), "ann", "what is my balance"));
        assertEquals(TurnStatus.COMPLETED, r.status);
        assertTrue(r.reply.contains(Double.toString(graph.balance)));
      }
      ConversationEntity.StateSnapshot snap = rt.state(cid);
      assertDense(snap.events());
      assertEquals(turns, ((Number) snap.reduced().get("turn_count")).longValue());

      try (Jedis j = new Jedis(redis.getHost(), redis.getMappedPort(6379))) {
        long stored = j.hlen(prefix + ":j:" + persistenceIdOf(sys, cid));
        assertEquals(snap.events().size(), stored, "every spec event is one journal entry in Redis");
        assertEquals(Long.toString(snap.events().size()), j.get(prefix + ":hi:" + persistenceIdOf(sys, cid)));
        Set<String> keys = j.keys(prefix + ":*");
        assertTrue(keys.stream().allMatch(k -> k.startsWith(prefix + ":j:") || k.startsWith(prefix + ":hi:")
            || k.startsWith(prefix + ":s:")), "no state lives outside the journal/snapshot keys: " + keys);
      }
    }
  }

  /** The entity's persistenceId as the journal sees it: the Redis hash exists under exactly one key for this conversation. */
  private static String persistenceIdOf(PekkoSystem sys, String conversationId) {
    try (Jedis j = new Jedis(redis.getHost(), redis.getMappedPort(6379))) {
      List<String> matches = j.keys("*:j:*" + conversationId + "*").stream().toList();
      assertEquals(1, matches.size(), "exactly one journal key for " + conversationId + ": " + matches);
      String key = matches.get(0);
      return key.substring(key.indexOf(":j:") + 3);
    }
  }

  @Test
  void restartOfTheWholeSystemReplaysFromRedisWithoutRunningBrainToolsOrGuardrails() {
    String prefix = rnd("p");
    String cid = rnd("c");
    String t1 = rnd("t");
    String t2 = rnd("t");
    List<LogEvent> before;
    CountingGraph first = new CountingGraph();
    try (PekkoSystem sys = boot(first, prefix)) {
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
      rt.submit(Event.turn(cid, t1, "bob", "balance?"));
      assertEquals(TurnStatus.SUSPENDED, rt.submit(Event.turn(cid, t2, "bob", "refund me")).status);
      before = rt.state(cid).events();
      assertEquals(1, first.brainCalls.get(), "balance runs the payments brain; refund suspends before one");
      assertEquals(1, first.toolCalls.get());
    }

    CountingGraph second = new CountingGraph();
    try (PekkoSystem sys = boot(second, prefix)) {
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
      ConversationEntity.StateSnapshot recovered = rt.state(cid);
      assertEquals(before, recovered.events(), "the replayed log is byte-for-byte the one written before the restart");
      assertDense(recovered.events());
      assertEquals(0, second.brainCalls.get(), "recovery must not invoke a brain");
      assertEquals(0, second.toolCalls.get(), "recovery must not invoke a tool");
      assertEquals(0, second.guardrailCalls.get(), "recovery must not invoke a guardrail");

      TurnResult dup = rt.submit(Event.turn(cid, t1, "bob", "balance?"));
      assertEquals(TurnStatus.DUPLICATE, dup.status);
      assertTrue(dup.events.isEmpty());
      assertEquals(0, second.brainCalls.get(), "a duplicate turn after restart runs nothing");
      assertEquals(before.size(), rt.state(cid).events().size(), "a duplicate appends nothing");

      TurnResult fresh = rt.submit(Event.turn(cid, rnd("t"), "bob", "balance?"));
      assertEquals(TurnStatus.COMPLETED, fresh.status);
      assertEquals(1, second.brainCalls.get());
      assertEquals(before.size(), fresh.events.get(0).sequence(), "new events continue the dense sequence");
    }
  }

  @Test
  void durableTimerSurvivesASystemRestartAndFiresExactlyOnce() throws InterruptedException {
    String prefix = rnd("p");
    String cid = rnd("c");
    String t1 = rnd("t");
    String timerId = rnd("timer");
    Event fire = Event.resume(cid, t1, Map.of("kind", "approval", "approved", true, "via", "timer"));
    CountingGraph first = new CountingGraph();
    try (PekkoSystem sys = boot(first, prefix)) {
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
      assertEquals(TurnStatus.SUSPENDED, rt.submit(Event.turn(cid, t1, "cat", "refund please")).status);
      ConversationEntity.TimerAck ack = rt.scheduleTimer(cid, timerId, Duration.ofSeconds(2), fire);
      assertFalse(ack.alreadyScheduled());
      assertTrue(rt.state(cid).pendingTimers().containsKey(timerId));
    }

    CountingGraph second = new CountingGraph();
    try (PekkoSystem sys = boot(second, prefix)) {
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
      awaitUntil(() -> rt.state(cid).suspendedTurnIds().isEmpty(), Duration.ofSeconds(15));
      ConversationEntity.StateSnapshot done = rt.state(cid);
      List<LogEvent> events = done.events();
      assertDense(events);
      assertTrue(done.pendingTimers().isEmpty());
      assertEquals(1, events.stream().filter(e -> e.is(EventType.TIMER_SCHEDULED)).count());
      assertEquals(1, events.stream().filter(e -> e.is(EventType.TIMER_FIRED)).count(), "fires exactly once");
      assertTrue(events.stream().anyMatch(e -> e.is(EventType.TURN_RESUMED)));
      assertTrue(events.stream().anyMatch(e -> e.is(EventType.TURN_COMPLETED)));
      assertEquals(1, second.brainCalls.get(), "only the resumed turn runs a brain, not recovery");

      assertEquals(TurnStatus.DUPLICATE, rt.submit(Event.resume(cid, t1, Map.of("kind", "approval", "approved", true))).status);
      assertTrue(rt.scheduleTimer(cid, timerId, Duration.ZERO, fire).alreadyScheduled());
    }

    CountingGraph third = new CountingGraph();
    try (PekkoSystem sys = boot(third, prefix)) {
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
      TimeUnit.MILLISECONDS.sleep(300);
      assertEquals(1, rt.state(cid).events().stream().filter(e -> e.is(EventType.TIMER_FIRED)).count(),
          "a consumed timer is not re-fired by another recovery");
      assertEquals(0, third.brainCalls.get());
    }
  }

  @Test
  void refusesACacheOnlyRedisWithAnActionableMessage() {
    RedisContainer cache = RedisContainer.cacheOnly();
    cache.start();
    try {
      CountingGraph graph = new CountingGraph();
      IllegalStateException e = assertThrows(IllegalStateException.class, () -> {
        try (PekkoSystem sys = new PekkoSystem(graph.deps(), DurabilityProfile.REDIS, cache.profileConfig(rnd("p")))) {
          new PekkoRuntime(sys.system(), Duration.ofSeconds(20)).submit(Event.turn(rnd("c"), rnd("t"), "dan", "balance"));
        }
      });
      assertTrue(e.getMessage().contains("appendonly=no"), e.getMessage());
      assertTrue(e.getMessage().contains("--appendonly yes"), e.getMessage());
    } finally {
      cache.stop();
    }
  }

  @Test
  void refusesToStartWithoutAConnectionUrl() {
    IllegalStateException e = assertThrows(IllegalStateException.class, DurabilityProfile.REDIS::config);
    assertTrue(e.getMessage().contains("AGENTIC_REDIS_URL"), e.getMessage());
  }
}

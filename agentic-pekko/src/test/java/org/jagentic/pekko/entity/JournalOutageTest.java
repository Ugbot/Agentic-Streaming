package org.jagentic.pekko.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jagentic.core.EffectJournal;
import org.jagentic.core.Event;
import org.jagentic.core.LogicalClock;
import org.jagentic.core.TurnProbe;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.junit.jupiter.api.Test;

/** A journal outage leaves the turn unspent: no effect, nothing persisted, and a redelivery runs it. */
class JournalOutageTest {

  @Test
  void anOutageDoesNotSpendTheTurnAndARedeliveryRunsIt() throws Exception {
    AtomicBoolean down = new AtomicBoolean(true);
    EffectJournal.InMemory held = new EffectJournal.InMemory();
    EffectJournal journal = new EffectJournal() {
      @Override public Optional<Entry> get(Key key) {
        return held.get(key);
      }

      @Override public void put(Entry entry) {
        held.put(entry);
      }

      @Override public Optional<Entry> putIfAbsent(Entry entry) {
        if (down.get()) {
          throw new Unavailable("the store is down", null);
        }
        return held.putIfAbsent(entry);
      }
    };
    Map<String, Object> workflow = Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
            "paths", Map.of("p", Map.of("brain", "rule", "tool_triggers", Map.of("pay", "pay"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(Map.of("id", "pay", "kind", "counting", "effect", "non_idempotent",
            "x-world", "outage-" + UUID.randomUUID())));
    GraphBuilder.Built built = GraphBuilder.build(workflow, null);
    AgentDeps deps = new AgentDeps(built.graph(), built.tools(), built.retriever(), LogicalClock.system(),
        TurnProbe.NONE, journal);
    String conversation = "c-" + UUID.randomUUID();
    Event pay = Event.turn(conversation, "t1", "u", "pay");
    try (PekkoSystem sys = new PekkoSystem(deps);
         PekkoRuntime runtime = new PekkoRuntime(sys.system(), Duration.ofSeconds(2))) {
      assertThrows(Exception.class, () -> runtime.submitAsync(pay).get(10, TimeUnit.SECONDS));
      down.set(false);
      TurnResult r = null;
      for (int i = 0; i < 40 && r == null; i++) {
        try {
          r = runtime.submitAsync(pay).get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException restarting) {
          Thread.sleep(250); // the entity is coming back from its supervisor's backoff
        }
      }
      assertEquals(TurnStatus.COMPLETED, r.status, "the outage spent nothing: " + r);
      assertEquals(1, r.calls.size());
      assertEquals(1, r.calls.get(0).result(), "one call: " + r);
    }
  }
}

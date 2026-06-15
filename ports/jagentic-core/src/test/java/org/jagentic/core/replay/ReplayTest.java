package org.jagentic.core.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.TurnResult;
import org.jagentic.core.stream.SeedChannel;
import org.jagentic.core.stream.StreamRuntime;

/** Phase 5: replay / time-travel. The event log is the source of truth; replaying it reproduces the
 * run (determinism) and replaying a prefix gives the state as-of that point. */
class ReplayTest {

  private static LocalRuntime banking() {
    return new LocalRuntime(Banking.buildGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), Banking.defaultTools(), Banking.retriever());
  }

  private static List<Event> turns() {
    return List.of(
        new Event("c1", "alice", "what is my balance?"),
        new Event("c2", "bob", "tell me about crypto cash-back"),
        new Event("c1", "alice", "hello there"));
  }

  private static List<String> paths(List<TurnResult> rs) {
    return rs.stream().map(r -> r.path).toList();
  }

  @Test
  void streamRecordsEveryEventToTheLog() {
    EventLog log = new EventLog.InMemory();
    new StreamRuntime(banking()).observe(log::record).run(new SeedChannel<>(turns()));
    assertEquals(turns(), log.events(), "the observer seam records the inbound stream");
    assertEquals(2, log.eventsFor("c1").size());
  }

  @Test
  void replayReproducesTheRun() {
    EventLog log = new EventLog.InMemory();
    List<TurnResult> original =
        new StreamRuntime(banking()).observe(log::record).run(new SeedChannel<>(turns()));

    // replay the log through a FRESH runtime (new stores) → identical routing.
    List<TurnResult> replayed = Replayer.replay(log.events(), banking());
    assertEquals(paths(original), paths(replayed));
    assertEquals(List.of("payments", "cards", "general"), paths(replayed));
  }

  @Test
  void replayUntilGivesStateAsOfAPoint() {
    EventLog log = new EventLog.InMemory();
    new StreamRuntime(banking()).observe(log::record).run(new SeedChannel<>(turns()));

    List<TurnResult> asOfTwo = Replayer.replayUntil(log.events(), 2, banking());
    assertEquals(2, asOfTwo.size());
    assertEquals(List.of("payments", "cards"), paths(asOfTwo));
  }
}

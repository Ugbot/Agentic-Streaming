package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.runtime.testkit.FixtureProcessingClock;
import org.agentic.flink.runtime.testkit.Workflows;
import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ManualProcessingClock}: one reading per clock id shared JVM-wide, carried through Java
 * serialization by id only, and continuous across a stop-with-savepoint restart of a job on the
 * local cluster so a pending workflow timer fires exactly once after the restore.
 */
class ManualProcessingClockTest {

  private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

  @TempDir
  static Path savepoints;

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  private static List<String> types(TurnResult r) {
    return r.events.stream().map(LogEvent::type).toList();
  }

  private static Map<String, Object> timerWorkflow(long afterMs, Map<String, Object> payload) {
    Map<String, Object> wf = new LinkedHashMap<>(Workflows.billing());
    Map<String, Object> timer = new LinkedHashMap<>();
    timer.put("id", "followup");
    timer.put("after_ms", afterMs);
    timer.put("clock", "processing");
    timer.put("tool", "lookup_charge");
    timer.put("payload", payload);
    wf.put("timers", List.of(timer));
    return wf;
  }

  @Test
  void handlesWithTheSameIdShareOneReading() {
    String id = rnd("clock");
    assertFalse(ManualProcessingClock.isRegistered(id));
    ManualProcessingClock a = ManualProcessingClock.named(id);
    ManualProcessingClock b = ManualProcessingClock.named(id);
    try {
      assertTrue(ManualProcessingClock.isRegistered(id));
      assertEquals(0L, a.nowMs());
      assertEquals(a, b);
      assertEquals(a.hashCode(), b.hashCode());
      long step = RND.nextLong(1L, 100_000L);
      assertEquals(step, a.advance(step));
      assertEquals(step, b.nowMs(), "a second handle reads the shared registry");
      long more = RND.nextLong(0L, 100_000L);
      b.advance(more);
      assertEquals(step + more, a.nowMs(null));
      assertEquals(step + more, ManualProcessingClock.named(id).nowMs(), "named() never resets a registered clock");
    } finally {
      a.release();
    }
    assertFalse(ManualProcessingClock.isRegistered(id));
    assertThrows(IllegalStateException.class, a::nowMs);
    assertThrows(IllegalStateException.class, () -> b.advance(1L));
  }

  @Test
  void clocksAreIndependentAndNeverMoveBackwards() {
    ManualProcessingClock a = ManualProcessingClock.create();
    ManualProcessingClock b = ManualProcessingClock.create();
    try {
      assertNotEquals(a, b);
      long step = RND.nextLong(1L, 1_000_000L);
      a.advance(step);
      assertEquals(0L, b.nowMs());
      assertThrows(IllegalArgumentException.class, () -> a.advance(-RND.nextLong(1L, 1_000L)));
      assertEquals(step, a.nowMs());
      assertEquals(step, a.advance(0L));
    } finally {
      a.release();
      b.release();
    }
    assertThrows(IllegalArgumentException.class, () -> ManualProcessingClock.named(" "));
  }

  @Test
  void serializedCopyReadsTheSameRegistryEntry() throws Exception {
    ManualProcessingClock clock = ManualProcessingClock.create();
    try {
      long before = RND.nextLong(1L, 1_000_000L);
      clock.advance(before);
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
        out.writeObject(FlinkRuntimeOptions.DEFAULTS.withManualClock(clock.clockId()));
      }
      FlinkRuntimeOptions restored;
      try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        restored = (FlinkRuntimeOptions) in.readObject();
      }
      ManualProcessingClock copy = assertInstanceOf(ManualProcessingClock.class, restored.processingClock());
      assertEquals(clock, copy);
      assertEquals(before, copy.nowMs(null), "the reading is not part of the serialized form");
      long after = RND.nextLong(1L, 1_000_000L);
      clock.advance(after);
      assertEquals(before + after, copy.nowMs(null), "the copy follows advances made through the original");
      assertEquals(before + after, restored.processingClock().nowMs(null));
    } finally {
      clock.release();
    }
  }

  @Test
  void fromSpecKeepsTheOperatorClockUnlessAManualClockIsSelected() {
    Map<String, Object> wf = Workflows.billing();
    assertEquals(ProcessingClock.flink(), FlinkRuntimeOptions.fromSpec(wf).processingClock());
    String id = rnd("clock");
    try {
      FlinkRuntimeOptions manual = FlinkRuntimeOptions.fromSpec(wf).withManualClock(id);
      assertEquals(ManualProcessingClock.named(id), manual.processingClock());
      assertTrue(ManualProcessingClock.isRegistered(id));
    } finally {
      ManualProcessingClock.named(id).release();
    }
  }

  @Test
  void fixtureClockIsAHandleOnTheManualClock() {
    try (FixtureProcessingClock fixture = new FixtureProcessingClock()) {
      ManualProcessingClock same = ManualProcessingClock.named(fixture.clockId());
      long step = RND.nextLong(1L, 1_000_000L);
      fixture.advance(step);
      assertEquals(step, same.nowMs());
      assertEquals(step, fixture.nowMs(null));
      same.advance(step);
      assertEquals(2 * step, fixture.nowMs());
      try (FixtureProcessingClock other = new FixtureProcessingClock()) {
        assertNotEquals(fixture, other);
      }
    }
  }

  @Test
  void closingTheFixtureClockReleasesTheRegistryEntry() {
    String id;
    try (FixtureProcessingClock fixture = new FixtureProcessingClock()) {
      id = fixture.clockId();
      assertTrue(ManualProcessingClock.isRegistered(id));
    }
    assertFalse(ManualProcessingClock.isRegistered(id));
  }

  @Test
  void readingSurvivesStopWithSavepointAndTheRestoredTimerFiresOnce() throws Exception {
    long after = RND.nextLong(1_000L, 100_000L);
    long beforeRestart = RND.nextLong(1L, after);
    Map<String, Object> payload = Map.of("channel", rnd("ch"));
    Map<String, Object> wf = timerWorkflow(after, payload);
    String cid = rnd("c");
    int parallelism = RND.nextInt(1, 4);
    ManualProcessingClock clock = ManualProcessingClock.create();
    try (LocalWorkflowSession s = new LocalWorkflowSession(wf,
        FlinkRuntimeOptions.fromSpec(wf).withManualClock(clock.clockId()), parallelism, null,
        savepoints.resolve(rnd("sp")), Duration.ofSeconds(60), rnd("job"))) {
      s.start();
      TurnResult first = s.submit(Event.turn(cid, rnd("t1"), "u", "hello"));
      assertTrue(types(first).contains("timer_scheduled"), types(first).toString());
      assertEquals(after, ((Number) first.events.get(1).payload().get("due_ms")).longValue());

      clock.advance(beforeRestart);
      s.restart();
      assertEquals(beforeRestart, clock.nowMs(), "the reading outlives the job that read it");

      TurnResult notYet = s.submit(Event.turn(cid, rnd("t2"), "u", "hello"));
      assertTrue(notYet.calls.isEmpty(), "the deadline has not been reached: " + notYet.calls);
      assertFalse(types(notYet).contains("timer_scheduled"), "restore never re-schedules: " + types(notYet));
      assertFalse(types(notYet).contains("timer_fired"));
      assertEquals(beforeRestart, ((Number) notYet.events.get(0).payload().get("processing_time_ms")).longValue());

      clock.advance(after - beforeRestart);
      TurnResult fired = s.submit(Event.turn(cid, rnd("t3"), "u", "hello"));
      assertEquals(List.of("followup"), fired.state.get("fired_timers"));
      assertEquals(1, fired.calls.size(), fired.calls.toString());
      assertEquals(payload, fired.calls.get(0).args());

      s.restart();
      clock.advance(RND.nextLong(1L, after));
      TurnResult again = s.submit(Event.turn(cid, rnd("t4"), "u", "hello"));
      assertTrue(again.calls.isEmpty(), "a fired timer never fires again: " + again.calls);
      assertEquals(List.of("followup"), again.state.get("fired_timers"));
      assertTrue(types(again).stream().noneMatch(t -> t.equals("timer_scheduled") || t.equals("timer_fired")),
          types(again).toString());
      assertEquals(2, s.restarts());
    } finally {
      clock.release();
    }
  }
}

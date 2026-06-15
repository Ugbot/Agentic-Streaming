package org.jagentic.core.windows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;

/** Phase 3: portable keyed windows — sliding (VelocityDetector), tumbling, session. */
class WindowsTest {

  @Test
  void slidingCountFiresAtThresholdAndEvictsOld() {
    SlidingWindow w = new SlidingWindow(60_000); // 60s
    // five events within 60s on one host → count reaches 5
    int last = 0;
    for (int i = 1; i <= 5; i++) {
      last = w.add("h1", i * 1000L).count();
    }
    assertEquals(5, last, ">=5 in 60s fires");
    // a 6th event far in the future evicts all earlier ones
    assertEquals(1, w.add("h1", 200_000L).count());
    // keys are independent
    assertEquals(1, w.add("h2", 1000L).count());
  }

  @Test
  void slidingSumsValues() {
    SlidingWindow w = new SlidingWindow(10_000);
    w.add("acct", 0, 100.0);
    WindowState s = w.add("acct", 5_000, 250.0);
    assertEquals(2, s.count());
    assertEquals(350.0, s.sum());
  }

  @Test
  void tumblingEmitsClosedBucket() {
    TumblingWindow w = new TumblingWindow(1_000);
    assertTrue(w.add("k", 100, 1.0).isEmpty());   // bucket 0 opens
    assertTrue(w.add("k", 200, 1.0).isEmpty());   // still bucket 0
    Optional<TumblingWindow.Bucket> emitted = w.add("k", 1_500, 1.0); // bucket 1 → closes bucket 0
    assertTrue(emitted.isPresent());
    assertEquals(0L, emitted.get().start());
    assertEquals(2, emitted.get().count());
    // flush the open bucket 1
    TumblingWindow.Bucket flushed = w.close("k").orElseThrow();
    assertEquals(1_000L, flushed.start());
    assertEquals(1, flushed.count());
  }

  @Test
  void sessionGroupsByGap() {
    SessionWindow w = new SessionWindow(5_000); // 5s gap
    assertTrue(w.add("u", 0).isEmpty());
    assertTrue(w.add("u", 2_000).isEmpty());     // within gap → same session
    Optional<SessionWindow.Session> emitted = w.add("u", 10_000); // gap > 5s → closes prior session
    assertTrue(emitted.isPresent());
    assertEquals(0L, emitted.get().start());
    assertEquals(2_000L, emitted.get().end());
    assertEquals(2, emitted.get().count());
    // close the new session
    assertEquals(1, w.close("u").orElseThrow().count());
    assertFalse(w.close("u").isPresent());
  }
}

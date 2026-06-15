package org.jagentic.core.cep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;

/** Phase 4 (headline): portable CEP. The incident golden — "3 anomalies within 5 minutes on one
 * host" — must match identically to the Flink IncidentAgentExample, plus strict/relaxed contiguity
 * and within-expiry. */
class CepMatcherTest {

  private static final long FIVE_MIN = 5 * 60 * 1000L;

  /** anomaly event for a host (text carries the host for the key). */
  private static Event anomaly(String host) {
    return new Event(host, "monitor", "anomaly", Map.of("metric", "cpu"));
  }

  private static Pattern incidentPattern() {
    return Pattern.begin("first", Condition.any())
        .followedBy("second", Condition.any())
        .followedBy("third", Condition.any())
        .within(FIVE_MIN);
  }

  @Test
  void threeAnomaliesInFiveMinutesFireOneIncident() {
    CepMatcher m = new CepMatcher(incidentPattern());
    assertTrue(m.match("h1", 0, anomaly("h1")).isEmpty());
    assertTrue(m.match("h1", 60_000, anomaly("h1")).isEmpty());
    List<Match> done = m.match("h1", 120_000, anomaly("h1")); // 3rd within 5 min
    assertEquals(1, done.size());
    Match incident = done.get(0);
    assertEquals(3, incident.events().size());
    assertEquals(List.of("first", "second", "third"), new ArrayList<>(incident.named().keySet()));
  }

  @Test
  void keysAreIndependent() {
    CepMatcher m = new CepMatcher(incidentPattern());
    m.match("h1", 0, anomaly("h1"));
    m.match("h2", 0, anomaly("h2"));
    m.match("h1", 1000, anomaly("h1"));
    assertTrue(m.match("h2", 1000, anomaly("h2")).isEmpty(), "h2 only has 2");
    assertEquals(1, m.match("h1", 2000, anomaly("h1")).size(), "h1 reaches 3");
  }

  @Test
  void withinExpiryPreventsLateCompletion() {
    CepMatcher m = new CepMatcher(incidentPattern());
    m.match("h1", 0, anomaly("h1"));
    m.match("h1", 60_000, anomaly("h1"));
    // 3rd arrives after the 5-minute window from the first → partials expired, no completion
    assertTrue(m.match("h1", FIVE_MIN + 1, anomaly("h1")).isEmpty());
    // flushExpired surfaces the timed-out partial(s)
    List<List<Event>> timedOut = m.flushExpired("h1", FIVE_MIN + 100_000);
    assertTrue(timedOut.size() >= 1, "the stalled partial is reported as timed out");
  }

  @Test
  void strictContiguityDropsOnInterleavedNonMatch() {
    // begin A, NEXT B(text=="b"): an interleaved non-"b" event breaks the strict partial.
    Pattern p = Pattern.begin("a", Condition.of(e -> e.text().equals("a")))
        .next("b", Condition.of(e -> e.text().equals("b")));
    CepMatcher m = new CepMatcher(p);
    m.match("k", 0, new Event("k", "u", "a"));
    assertTrue(m.match("k", 1, new Event("k", "u", "x")).isEmpty(), "strict: 'x' breaks it");
    assertTrue(m.match("k", 2, new Event("k", "u", "b")).isEmpty(), "partial already dropped");
  }

  @Test
  void relaxedContiguitySkipsNonMatches() {
    Pattern p = Pattern.begin("a", Condition.of(e -> e.text().equals("a")))
        .followedBy("b", Condition.of(e -> e.text().equals("b")));
    CepMatcher m = new CepMatcher(p);
    m.match("k", 0, new Event("k", "u", "a"));
    assertTrue(m.match("k", 1, new Event("k", "u", "x")).isEmpty(), "relaxed: 'x' is skipped");
    assertEquals(1, m.match("k", 2, new Event("k", "u", "b")).size(), "'b' completes after the skip");
  }

  @Test
  void iterativeConditionSeesMatchedSoFar() {
    // second event must have the same host (conversationId) as the first.
    Pattern p = Pattern.begin("a", Condition.any())
        .followedBy("b", (e, soFar) -> e.conversationId().equals(soFar.get(0).conversationId()));
    CepMatcher m = new CepMatcher(p);
    m.match("h1", 0, anomaly("h1"));
    assertEquals(1, m.match("h1", 1, anomaly("h1")).size());
  }

  @Test
  void observerBridgeFiresHandlerOnMatch() {
    AtomicInteger incidents = new AtomicInteger();
    CepObserver obs = new CepObserver(incidentPattern(),
        Event::conversationId,
        e -> Long.parseLong(e.metadata().getOrDefault("ts", "0")),
        match -> incidents.incrementAndGet());
    for (long t : new long[] {0, 1000, 2000}) {
      obs.onEvent(new Event("h1", "monitor", "anomaly", Map.of("ts", Long.toString(t))));
    }
    assertEquals(1, incidents.get());
  }
}

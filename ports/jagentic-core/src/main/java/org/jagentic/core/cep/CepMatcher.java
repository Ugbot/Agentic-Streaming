package org.jagentic.core.cep;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.Event;

/**
 * A portable, keyed NFA matcher over an event stream — the cross-engine equivalent of Flink CEP.
 * Feed events per key with {@link #match}; it advances partial matches and emits completed {@link
 * Match}es. {@code within} is enforced by expiring partials whose first event is older than the bound
 * (also exposed via {@link #flushExpired} for timer-driven expiry).
 *
 * <p>Semantics per event (deterministic relaxed/strict, no {@code followedByAny} non-determinism):
 * existing partials advance one stage if the next stage's condition matches; on a non-match a
 * {@code NEXT} (strict) partial is dropped and a {@code FOLLOWED_BY} (relaxed) partial waits; every
 * event may also start a new partial at stage 0. A completed partial is emitted and not reused.</p>
 */
public final class CepMatcher {

  private record Partial(List<Event> events, int stage, long startTs) {}

  private final Pattern pattern;
  private final Map<String, List<Partial>> byKey = new LinkedHashMap<>();

  public CepMatcher(Pattern pattern) {
    this.pattern = pattern;
  }

  /** Feed one event for {@code key} at logical time {@code ts}; return any completed matches. */
  public synchronized List<Match> match(String key, long ts, Event event) {
    List<Pattern.Stage> stages = pattern.stages();
    long within = pattern.withinMillis();
    int last = stages.size() - 1;

    List<Partial> partials = byKey.computeIfAbsent(key, k -> new ArrayList<>());
    if (within > 0) {
      partials.removeIf(p -> ts - p.startTs() > within);
    }

    List<Match> completed = new ArrayList<>();
    List<Partial> survivors = new ArrayList<>();

    for (Partial p : partials) {
      int nextStage = p.stage() + 1;
      Pattern.Stage stage = stages.get(nextStage);
      if (stage.condition().test(event, p.events())) {
        List<Event> advanced = append(p.events(), event);
        if (nextStage == last) {
          completed.add(toMatch(advanced, stages));
        } else {
          survivors.add(new Partial(advanced, nextStage, p.startTs()));
        }
      } else if (stage.contiguity() == Pattern.Contiguity.FOLLOWED_BY) {
        survivors.add(p); // relaxed: skip this event, keep waiting
      }
      // NEXT (strict) + non-match → drop p
    }

    Pattern.Stage first = stages.get(0);
    if (first.condition().test(event, List.of())) {
      List<Event> ev = List.of(event);
      if (last == 0) {
        completed.add(toMatch(ev, stages));
      } else {
        survivors.add(new Partial(ev, 0, ts));
      }
    }

    byKey.put(key, survivors);
    return completed;
  }

  /** Remove and return the matched-events of partials that have exceeded {@code within} as of
   * {@code now} (the portable form of Flink's timed-out partial matches). Empty if unbounded. */
  public synchronized List<List<Event>> flushExpired(String key, long now) {
    long within = pattern.withinMillis();
    List<List<Event>> out = new ArrayList<>();
    if (within <= 0) {
      return out;
    }
    List<Partial> partials = byKey.get(key);
    if (partials == null) {
      return out;
    }
    List<Partial> survivors = new ArrayList<>();
    for (Partial p : partials) {
      if (now - p.startTs() > within) {
        out.add(p.events());
      } else {
        survivors.add(p);
      }
    }
    byKey.put(key, survivors);
    return out;
  }

  private static List<Event> append(List<Event> events, Event e) {
    List<Event> out = new ArrayList<>(events);
    out.add(e);
    return List.copyOf(out);
  }

  private static Match toMatch(List<Event> events, List<Pattern.Stage> stages) {
    Map<String, Event> named = new LinkedHashMap<>();
    for (int i = 0; i < events.size() && i < stages.size(); i++) {
      named.put(stages.get(i).name(), events.get(i));
    }
    return new Match(events, named);
  }
}

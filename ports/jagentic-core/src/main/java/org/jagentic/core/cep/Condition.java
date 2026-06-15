package org.jagentic.core.cep;

import java.util.List;

import org.jagentic.core.Event;

/**
 * A CEP stage predicate. Simple conditions ignore {@code matchedSoFar}; iterative conditions inspect
 * the events already matched in this partial (the portable form of Flink's {@code SimpleCondition} /
 * {@code IterativeCondition}) — e.g. "this anomaly is on the same host as the first", or an
 * aggregate threshold across the partial match.
 */
@FunctionalInterface
public interface Condition {
  boolean test(Event event, List<Event> matchedSoFar);

  /** A simple, event-only predicate. */
  static Condition of(java.util.function.Predicate<Event> p) {
    return (event, matchedSoFar) -> p.test(event);
  }

  /** Always matches. */
  static Condition any() {
    return (event, matchedSoFar) -> true;
  }
}

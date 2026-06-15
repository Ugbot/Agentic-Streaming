package org.jagentic.core.cep;

import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.ToolRegistry;

/**
 * One declarative CEP rule wired from a pipeline's {@code cep:} section: a {@link CepMatcher} plus the
 * key/timestamp extractors and the action to fire on a match. {@link #onEvent} feeds an inbound event
 * to the matcher and fires the action for each completed match. Events the action itself produced are
 * tagged ({@link #DERIVED}) and skipped, so a {@code submit} action cannot recurse.
 */
public final class CepWiring {

  /** Metadata flag marking an event injected by a CEP action (so CEP does not re-match it). */
  public static final String DERIVED = "__cep_derived__";

  /** What to do when the pattern matches. */
  @FunctionalInterface
  public interface Action {
    void fire(Match match, String key, Runtime runtime, ToolRegistry tools);
  }

  private final String name;
  private final CepMatcher matcher;
  private final Function<Event, String> keyFn;
  private final ToLongFunction<Event> tsFn;
  private final Action action;

  public CepWiring(String name, CepMatcher matcher, Function<Event, String> keyFn,
                   ToLongFunction<Event> tsFn, Action action) {
    this.name = name;
    this.matcher = matcher;
    this.keyFn = keyFn;
    this.tsFn = tsFn;
    this.action = action;
  }

  public String name() {
    return name;
  }

  /** Feed one inbound event; fire the action for every completed match. */
  public void onEvent(Event event, Runtime runtime, ToolRegistry tools) {
    if (event.metadata() != null && event.metadata().containsKey(DERIVED)) {
      return; // don't re-match events a CEP action produced
    }
    String key = keyFn.apply(event);
    long ts = tsFn.applyAsLong(event);
    for (Match match : matcher.match(key, ts, event)) {
      action.fire(match, key, runtime, tools);
    }
  }
}

package org.jagentic.core.cep;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.jagentic.core.Event;
import org.jagentic.core.stream.EventObserver;

/**
 * Bridges a {@link CepMatcher} onto the event stream: register it on a {@code StreamRuntime} (or call
 * {@link #onEvent} directly) and it keys + timestamps each event, feeds the matcher, and invokes
 * {@code onMatch} for every completed match — the portable equivalent of routing Flink CEP matches to
 * a {@code PatternProcessFunction}. The handler typically submits a derived event (e.g. an incident)
 * as a new agent turn.
 */
public final class CepObserver implements EventObserver {

  private final CepMatcher matcher;
  private final Function<Event, String> keyFn;
  private final ToLongFunction<Event> tsFn;
  private final Consumer<Match> onMatch;

  public CepObserver(Pattern pattern, Function<Event, String> keyFn, ToLongFunction<Event> tsFn,
                     Consumer<Match> onMatch) {
    this.matcher = new CepMatcher(pattern);
    this.keyFn = keyFn;
    this.tsFn = tsFn;
    this.onMatch = onMatch;
  }

  @Override
  public void onEvent(Event event) {
    for (Match match : matcher.match(keyFn.apply(event), tsFn.applyAsLong(event), event)) {
      onMatch.accept(match);
    }
  }
}

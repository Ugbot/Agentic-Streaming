package org.jagentic.core.stream;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** A bounded channel that replays a fixed list of events in order, then is empty forever. */
public final class SeedChannel<T> implements Channel<T> {

  private final Iterator<T> it;

  public SeedChannel(List<T> events) {
    this.it = List.copyOf(events).iterator();
  }

  @SafeVarargs
  public static <T> SeedChannel<T> of(T... events) {
    return new SeedChannel<>(List.of(events));
  }

  @Override
  public Optional<T> poll() {
    return it.hasNext() ? Optional.of(it.next()) : Optional.empty();
  }
}

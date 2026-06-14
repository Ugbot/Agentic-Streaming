package org.jagentic.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

/** Compensation / saga — register reversible steps; if a later step fails, run the
 * recorded compensations in reverse. Portable analogue of the Flink CompensationHandler. */
public final class Saga {

  public record CompensationAction(String name, Runnable undo) {}

  private final Deque<CompensationAction> done = new ArrayDeque<>();

  /** Run {@code doFn}; on success record {@code undo} for rollback and return the result.
   * If {@code doFn} throws, compensate everything recorded so far and re-throw. */
  public <T> T step(String name, Supplier<T> doFn, Runnable undo) {
    T result;
    try {
      result = doFn.get();
    } catch (RuntimeException e) {
      compensate();
      throw e;
    }
    done.push(new CompensationAction(name, undo));
    return result;
  }

  /** Run all recorded compensations in reverse order; returns the names compensated. */
  public List<String> compensate() {
    List<String> names = new ArrayList<>();
    while (!done.isEmpty()) {
      CompensationAction a = done.pop();
      if (a.undo() != null) {
        a.undo().run();
      }
      names.add(a.name());
    }
    return names;
  }
}

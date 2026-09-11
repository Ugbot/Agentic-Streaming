package org.jagentic.core.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Spec rule 7 for every configured external store: an unreachable store fails the build unless its
 * section sets {@code on_unavailable: degrade}, in which case the in-memory fallback is used and the
 * degradation is recorded here so it is visible on the built system rather than silent.
 */
public final class StoreAvailability {

  /** Raised when a configured store cannot be reached and may not degrade. */
  public static final class StoreUnavailableException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public StoreUnavailableException(String path, Throwable cause) {
      super(path + ": configured store is unreachable and on_unavailable is fail ("
          + (cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage()) + ")", cause);
    }
  }

  private final List<String> degradations = Collections.synchronizedList(new ArrayList<>());

  /** {@code on_unavailable: degrade} on the section; anything else (including absent) means fail. */
  public static boolean degrades(Map<String, Object> section) {
    return section != null && "degrade".equals(section.get("on_unavailable"));
  }

  public static StoreUnavailableException unavailable(String path, Throwable cause) {
    return new StoreUnavailableException(path, cause);
  }

  /**
   * Applies rule 7 to one connection attempt: returns the connected store, or the fallback when
   * the section allows degradation (recording it), or throws.
   */
  public <T> T connect(String path, Map<String, Object> section, java.util.function.Supplier<T> connect,
                       java.util.function.Supplier<T> fallback) {
    try {
      return connect.get();
    } catch (RuntimeException e) {
      if (degrades(section)) {
        record(path, e);
        return fallback.get();
      }
      throw unavailable(path, e);
    }
  }

  public void record(String path, Throwable cause) {
    degradations.add(path + " degraded to memory: "
        + (cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage()));
  }

  /** Stores that degraded to memory during the build, as {@code "<path> degraded to memory: <why>"}. */
  public List<String> degradations() {
    return List.copyOf(degradations);
  }
}

package org.jagentic.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The outside world of a conformance fixture, in miniature. A {@code counting} tool answers with how
 * many calls or distinct keys it has seen, so a repeated effect shows in the reply. It has to outlive
 * a crash, a job restart and a rebuilt graph, as the real outside world would, so it is held per
 * process and named by the {@code x-world} a harness writes into the document.
 */
public final class FixtureWorld {
  private static final Map<String, List<String>> SEEN = new ConcurrentHashMap<>();

  private FixtureWorld() {}

  /** Records one call and returns how many this counter has seen. */
  public static int count(String world, String counter, String key, boolean distinctKeys) {
    List<String> seen = SEEN.computeIfAbsent(world + "\u0000" + counter, k -> new ArrayList<>());
    synchronized (seen) {
      if (!distinctKeys || !seen.contains(key)) {
        seen.add(key);
      }
      return seen.size();
    }
  }
}

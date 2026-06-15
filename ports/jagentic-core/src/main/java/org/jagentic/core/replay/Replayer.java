package org.jagentic.core.replay;

import java.util.ArrayList;
import java.util.List;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.TurnResult;

/**
 * Re-materialize agent state by replaying recorded events through a runtime. Replaying through a
 * <i>fresh</i> runtime over the same graph reproduces the outcomes (determinism); replaying through a
 * runtime built on a <i>new</i> graph version answers "what would the new prompts/routing have done";
 * {@link #replayUntil} stops early to inspect the state as-of a point in the log (time-travel — the
 * portable form of Datomic {@code as-of} / a checkpoint restore).
 */
public final class Replayer {

  private Replayer() {}

  /** Submit each event to {@code runtime} in order; return the turn results. */
  public static List<TurnResult> replay(List<Event> events, Runtime runtime) {
    List<TurnResult> out = new ArrayList<>(events.size());
    for (Event e : events) {
      out.add(runtime.submit(e));
    }
    return out;
  }

  /** Replay only the first {@code count} events — state as-of that point in the log. */
  public static List<TurnResult> replayUntil(List<Event> events, int count, Runtime runtime) {
    int n = Math.max(0, Math.min(count, events.size()));
    return replay(events.subList(0, n), runtime);
  }
}

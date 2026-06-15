package org.jagentic.core.stream;

import org.jagentic.core.Event;

/**
 * Sees every event the {@link StreamRuntime} drives, before it becomes a turn. The seam later phases
 * plug into — CEP matchers, window aggregators, and tracers observe the raw event stream here without
 * the agent graph needing to know about them.
 */
@FunctionalInterface
public interface EventObserver {
  void onEvent(Event event);
}

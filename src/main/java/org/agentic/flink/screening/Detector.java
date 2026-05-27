package org.agentic.flink.screening;

import java.io.Serializable;

/**
 * A single screening check. Returns a {@link Signal} when it fires, or {@code null} when it does
 * not. Detectors are Serializable (they ship in the Flink job graph) and must read any state they
 * need from the supplied {@link ScreenContext} rather than holding mutable per-key state.
 */
public interface Detector extends Serializable {

  /** Inspect the current item (which is also {@code ctx.recent(item.key())}'s last element). */
  Signal inspect(ScreenItem item, ScreenContext ctx);

  String name();
}

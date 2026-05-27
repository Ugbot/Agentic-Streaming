package org.agentic.flink.screening;

import java.util.List;

/**
 * Read-only view of recent history for a key, handed to each {@link Detector} so stateful detectors
 * (repeat, velocity) can inspect prior items without owning state themselves. The item currently
 * being screened is the last element of {@link #recent}.
 */
public interface ScreenContext {

  /** Recent items for the given key, oldest first, including the current item. */
  List<ScreenItem> recent(String key);

  /** "Now" for window math — the timestamp of the item being screened (never wall-clock). */
  long now();
}

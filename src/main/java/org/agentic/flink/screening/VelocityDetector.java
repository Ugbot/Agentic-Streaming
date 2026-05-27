package org.agentic.flink.screening;

import java.time.Duration;
import java.util.List;

/**
 * Velocity / rate screen: fires when at least {@code n} items for a key occur within a sliding
 * {@code window} (by event timestamp) — e.g. five charges on one account inside a minute.
 */
public final class VelocityDetector implements Detector {
  private static final long serialVersionUID = 1L;

  private final int n;
  private final long windowMillis;
  private final double weight;

  public VelocityDetector(int n, Duration window, double weight) {
    if (n < 2) throw new IllegalArgumentException("n must be >= 2");
    this.n = n;
    this.windowMillis = window.toMillis();
    this.weight = weight;
  }

  @Override
  public Signal inspect(ScreenItem item, ScreenContext ctx) {
    List<ScreenItem> recent = ctx.recent(item.key());
    long cutoff = ctx.now() - windowMillis;
    int inWindow = 0;
    for (ScreenItem r : recent) {
      if (r.ts() >= cutoff) inWindow++;
    }
    if (inWindow < n) return null;
    return new Signal(
        name(), Phase.VELOCITY, weight,
        String.format("%d items within %dms for key '%s'", inWindow, windowMillis, item.key()));
  }

  @Override
  public String name() {
    return "velocity";
  }
}

package org.agentic.flink.screening;

import java.util.List;
import java.util.function.BiPredicate;

/**
 * "Repeated screen": fires when the last {@code n} items for a key are equal under a configurable
 * equality — e.g. the same payment submitted three times in a row.
 *
 * <p>Default equality is key-identity (all recent items share the key, which they do by
 * construction), so the default effectively means "n consecutive items on this key". Pass a stricter
 * {@link BiPredicate} (e.g. same value AND same merchant) to detect true duplicates.
 */
public final class RepeatDetector implements Detector {
  private static final long serialVersionUID = 1L;

  private final int n;
  private final double weight;
  private final BiPredicate<ScreenItem, ScreenItem> sameAs;

  public RepeatDetector(int n, double weight) {
    this(n, weight, (a, b) -> true); // key-identity (recent() is already per-key)
  }

  public RepeatDetector(int n, double weight, BiPredicate<ScreenItem, ScreenItem> sameAs) {
    if (n < 2) throw new IllegalArgumentException("n must be >= 2");
    this.n = n;
    this.weight = weight;
    this.sameAs = sameAs;
  }

  /** Convenience: repeats where the value is identical (within epsilon) — e.g. stuck sensor. */
  public static RepeatDetector sameValue(int n, double weight) {
    return new RepeatDetector(n, weight, (a, b) -> Math.abs(a.value() - b.value()) < 1e-9);
  }

  @Override
  public Signal inspect(ScreenItem item, ScreenContext ctx) {
    List<ScreenItem> recent = ctx.recent(item.key());
    if (recent.size() < n) return null;
    ScreenItem last = recent.get(recent.size() - 1);
    for (int i = recent.size() - n; i < recent.size() - 1; i++) {
      if (!sameAs.test(recent.get(i), last)) return null;
    }
    return new Signal(
        name(), Phase.REPEAT, weight,
        String.format("%d identical items in a row for key '%s'", n, item.key()));
  }

  @Override
  public String name() {
    return "repeat";
  }
}

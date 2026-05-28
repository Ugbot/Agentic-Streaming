package org.agentic.flink.screening;

import java.util.List;

/**
 * Statistical "anomaly" screen: fires when the current item's value (or a named numeric attribute)
 * is more than {@code zThreshold} standard deviations away from the recent per-key mean. Reads the
 * rolling window from {@link ScreenContext#recent(String)} — no extra state of its own.
 *
 * <p>A lightweight, framework-consistent stand-in for the "ML tier" of {@link ScreeningPipeline}
 * over numeric features (mirrors the inline z-score detector in
 * {@code IncidentAgentExample}'s {@code GenericInferenceModel}). For a trained anomaly model swap
 * in {@code DjlInferenceConnection}; the {@link ScreeningPipeline} keeps the same surface.
 *
 * <p>Two modes:
 * <ul>
 *   <li>Default — read {@link ScreenItem#value()}.</li>
 *   <li>{@link #onAttr(String, Phase, double, int, double)} — read a numeric value from
 *       {@link ScreenItem#attrs()} by key (parsed as a double). Lets a single screening pipeline
 *       z-score several features carried in one item (e.g. spread, depth, volume).</li>
 * </ul>
 */
public final class ZScoreDetector implements Detector {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final Phase phase;
  private final double zThreshold;
  private final int minWarmup;
  private final double weight;
  private final String attrName; // null → use item.value()

  public ZScoreDetector(String name, Phase phase, double zThreshold, int minWarmup, double weight) {
    this(name, phase, zThreshold, minWarmup, weight, null);
  }

  private ZScoreDetector(
      String name, Phase phase, double zThreshold, int minWarmup, double weight, String attrName) {
    if (minWarmup < 2) throw new IllegalArgumentException("minWarmup must be >= 2");
    this.name = name;
    this.phase = phase;
    this.zThreshold = zThreshold;
    this.minWarmup = minWarmup;
    this.weight = weight;
    this.attrName = attrName;
  }

  /** Score a numeric attribute (looked up in {@link ScreenItem#attrs()}) instead of the value. */
  public static ZScoreDetector onAttr(
      String attrName, Phase phase, double zThreshold, int minWarmup, double weight) {
    return new ZScoreDetector("z-" + attrName, phase, zThreshold, minWarmup, weight, attrName);
  }

  @Override
  public Signal inspect(ScreenItem item, ScreenContext ctx) {
    List<ScreenItem> recent = ctx.recent(item.key());
    if (recent.size() < minWarmup) return null;

    double current = readValue(item);
    if (Double.isNaN(current)) return null;

    // Baseline excludes the current item (the recent list includes it as the last element).
    int last = recent.size() - 1;
    double sum = 0.0;
    int n = 0;
    for (int i = 0; i < last; i++) {
      double v = readValue(recent.get(i));
      if (Double.isNaN(v)) continue;
      sum += v;
      n++;
    }
    if (n < minWarmup - 1) return null;
    double mean = sum / n;
    double sq = 0.0;
    for (int i = 0; i < last; i++) {
      double v = readValue(recent.get(i));
      if (Double.isNaN(v)) continue;
      double d = v - mean;
      sq += d * d;
    }
    double std = Math.sqrt(sq / n);
    if (std == 0.0) return null; // flat baseline — z undefined; do not fire
    double z = (current - mean) / std;
    if (Math.abs(z) < zThreshold) return null;
    return new Signal(
        name, phase, weight,
        String.format(
            "%s=%.2f is %.2fσ from baseline mean %.2f (window=%d)",
            attrName == null ? "value" : attrName, current, z, mean, n));
  }

  private double readValue(ScreenItem item) {
    if (attrName == null) return item.value();
    String raw = item.attrs().get(attrName);
    if (raw == null) return Double.NaN;
    try {
      return Double.parseDouble(raw);
    } catch (NumberFormatException e) {
      return Double.NaN;
    }
  }

  @Override
  public String name() {
    return name;
  }
}

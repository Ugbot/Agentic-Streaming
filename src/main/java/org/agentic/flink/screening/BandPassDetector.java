package org.agentic.flink.screening;

/**
 * Band-pass / band-reject screen on {@link ScreenItem#value()}.
 *
 * <p>Default (pass band) mode fires when the value falls <b>outside</b> {@code [min, max]} — the
 * value should stay within the band, so out-of-band is suspicious. In {@code reject} mode it fires
 * when the value falls <b>inside</b> a forbidden band instead. Stateless.
 */
public final class BandPassDetector implements Detector {
  private static final long serialVersionUID = 1L;

  private final double min;
  private final double max;
  private final double weight;
  private final boolean reject;

  public BandPassDetector(double min, double max, double weight) {
    this(min, max, weight, false);
  }

  public BandPassDetector(double min, double max, double weight, boolean reject) {
    this.min = min;
    this.max = max;
    this.weight = weight;
    this.reject = reject;
  }

  @Override
  public Signal inspect(ScreenItem item, ScreenContext ctx) {
    boolean inside = item.value() >= min && item.value() <= max;
    boolean fire = reject == inside; // reject: fire when inside; pass: fire when outside
    if (!fire) return null;
    String how =
        reject
            ? String.format("value %.2f inside forbidden band [%.2f, %.2f]", item.value(), min, max)
            : String.format("value %.2f outside band [%.2f, %.2f]", item.value(), min, max);
    return new Signal(name(), Phase.BAND_PASS, weight, how);
  }

  @Override
  public String name() {
    return "band-pass";
  }
}

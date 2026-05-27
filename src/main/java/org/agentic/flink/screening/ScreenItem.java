package org.agentic.flink.screening;

import java.io.Serializable;
import java.util.Map;

/**
 * One item flowing through a {@link ScreeningPipeline}.
 *
 * @param key dedup / velocity key — items sharing a key are compared for repeats and rate
 * @param value numeric value the band-pass phase inspects (amount, reading, score, …)
 * @param label free-text the classifier/LLM tiers read (memo, message, description)
 * @param ts event timestamp in epoch millis (drives velocity windows; never wall-clock)
 * @param attrs extra context handed to the LLM tier
 */
public record ScreenItem(String key, double value, String label, long ts, Map<String, String> attrs)
    implements Serializable {

  public ScreenItem {
    attrs = attrs == null ? Map.of() : Map.copyOf(attrs);
  }

  public static ScreenItem of(String key, double value, String label, long ts) {
    return new ScreenItem(key, value, label, ts, Map.of());
  }
}

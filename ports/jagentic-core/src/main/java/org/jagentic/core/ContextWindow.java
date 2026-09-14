package org.jagentic.core;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The workflow-level {@code context} block (spec/v1/workflow.schema.json {@code $defs.context}) as
 * the v1 fixtures observe it: {@code compaction: window} with {@code max_items: N} bounds the
 * model-visible transcript to the most recent {@code N} messages, in log order. The log itself is
 * never compacted; only the transcript the fold retains, and with it {@code state.transcript_length},
 * is. {@code none} retains everything. {@code moscow} and {@code max_tokens} are runtime-specific
 * token budgeting handled by {@link ContextWindowManager}; they leave the retained transcript alone.
 */
public record ContextWindow(Compaction compaction, int maxItems) implements Serializable {

  public enum Compaction { NONE, MOSCOW, WINDOW }

  /** No {@code context} block: every written message stays visible. */
  public static final ContextWindow NONE = new ContextWindow(Compaction.NONE, 0);

  public ContextWindow {
    compaction = compaction == null ? Compaction.NONE : compaction;
    if (compaction == Compaction.WINDOW && maxItems < 1) {
      throw new IllegalArgumentException("context.compaction window requires max_items >= 1");
    }
  }

  public static ContextWindow window(int maxItems) {
    return new ContextWindow(Compaction.WINDOW, maxItems);
  }

  /** Parses a (schema-valid) {@code context} map; {@code null} yields {@link #NONE}. */
  public static ContextWindow fromMap(Map<String, Object> m) {
    if (m == null) {
      return NONE;
    }
    Compaction compaction = Compaction.valueOf(
        String.valueOf(m.getOrDefault("compaction", "none")).toUpperCase());
    int maxItems = m.get("max_items") instanceof Number n ? n.intValue() : 0;
    if (compaction == Compaction.WINDOW && maxItems < 1) {
      throw new IllegalArgumentException("context.compaction window requires context.max_items");
    }
    return new ContextWindow(compaction, maxItems);
  }

  /** True when the retained transcript is bounded by {@link #maxItems()}. */
  public boolean bounded() {
    return compaction == Compaction.WINDOW;
  }

  /** The retained tail of {@code messages}: the most recent {@code max_items}, order preserved. */
  public <T> List<T> retain(List<T> messages) {
    if (!bounded() || messages.size() <= maxItems) {
      return messages;
    }
    return messages.subList(messages.size() - maxItems, messages.size());
  }

  /** {@code transcript_length} for {@code written} messages appended so far. */
  public long retainedLength(long written) {
    return bounded() ? Math.min(written, maxItems) : written;
  }
}

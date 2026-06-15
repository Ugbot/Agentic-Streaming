package org.jagentic.tools.ingest;

import java.util.ArrayList;
import java.util.List;

/**
 * LangChain-style recursive text splitter: prefer paragraph boundaries, then sentence
 * boundaries, then word boundaries, then characters. Targets {@code maxChars} per chunk with an
 * optional overlap that helps retrieval recall when a relevant span straddles a chunk boundary.
 *
 * <p>Token-count estimates use the conservative 4-char-per-token heuristic.
 */
public final class RecursiveTextChunker implements Chunker {
  private static final long serialVersionUID = 1L;

  private static final String[] SEPARATORS = {"\n\n", "\n", ". ", " ", ""};

  private final int maxChars;
  private final int overlapChars;

  public RecursiveTextChunker(int maxChars) {
    this(maxChars, Math.max(0, maxChars / 10));
  }

  public RecursiveTextChunker(int maxChars, int overlapChars) {
    if (maxChars <= 0) throw new IllegalArgumentException("maxChars must be positive");
    if (overlapChars < 0 || overlapChars >= maxChars) {
      throw new IllegalArgumentException("overlapChars must be in [0, maxChars)");
    }
    this.maxChars = maxChars;
    this.overlapChars = overlapChars;
  }

  @Override
  public List<Chunk> chunk(String sourceId, String text) {
    if (text == null || text.isEmpty()) return List.of();
    List<String> pieces = recursiveSplit(text, 0);
    List<String> merged = mergeWithOverlap(pieces);
    List<Chunk> out = new ArrayList<>(merged.size());
    for (int i = 0; i < merged.size(); i++) {
      String t = merged.get(i);
      out.add(new Chunk(sourceId + "::" + i, t, sourceId, i, estimateTokens(t)));
    }
    return out;
  }

  private List<String> recursiveSplit(String text, int sepIdx) {
    if (text.length() <= maxChars) return List.of(text);
    if (sepIdx >= SEPARATORS.length) {
      // Fallback: hard split on character boundary.
      List<String> out = new ArrayList<>();
      for (int i = 0; i < text.length(); i += maxChars) {
        out.add(text.substring(i, Math.min(text.length(), i + maxChars)));
      }
      return out;
    }
    String sep = SEPARATORS[sepIdx];
    if (sep.isEmpty()) {
      return recursiveSplit(text, sepIdx + 1);
    }
    String[] parts = text.split(java.util.regex.Pattern.quote(sep));
    List<String> out = new ArrayList<>();
    for (String p : parts) {
      if (p.length() <= maxChars) {
        out.add(p);
      } else {
        out.addAll(recursiveSplit(p, sepIdx + 1));
      }
    }
    return out;
  }

  /** Merge consecutive pieces into chunks that respect {@code maxChars} and add overlap. */
  private List<String> mergeWithOverlap(List<String> pieces) {
    List<String> out = new ArrayList<>();
    StringBuilder buf = new StringBuilder();
    for (String p : pieces) {
      if (buf.length() + p.length() + 1 > maxChars) {
        if (buf.length() > 0) {
          out.add(buf.toString());
          if (overlapChars > 0) {
            int from = Math.max(0, buf.length() - overlapChars);
            buf = new StringBuilder(buf.substring(from));
          } else {
            buf = new StringBuilder();
          }
        }
      }
      if (buf.length() > 0) buf.append(' ');
      buf.append(p);
    }
    if (buf.length() > 0) out.add(buf.toString());
    return out;
  }

  private static int estimateTokens(String s) {
    return Math.max(1, s.length() / 4);
  }
}

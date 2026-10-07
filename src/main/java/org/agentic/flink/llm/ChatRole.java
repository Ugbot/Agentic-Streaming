package org.agentic.flink.llm;

import org.agentic.flink.annotation.Public;

/**
 * Speaker role for a {@link ChatMessage}.
 *
 * <p>Kept intentionally small. Provider-specific roles ({@code function}, {@code developer},
 * vendor-prefixed system messages, etc.) map to one of these at the edge.
 */
@Public
public enum ChatRole {
  SYSTEM,
  USER,
  ASSISTANT,
  TOOL
}

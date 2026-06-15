package org.jagentic.tools;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Minimal, Flink-free tool contract — the analogue of the Flink {@code ToolExecutor}, used
 * by lifted tool classes (web/Tika) so their bodies port unchanged. A {@link ToolPack} adapts
 * these into a {@code jagentic-core} ToolRegistry. */
public interface OneShotTool {

  /** Execute asynchronously with the given parameters. */
  CompletableFuture<Object> execute(Map<String, Object> parameters);

  /** Stable tool id. */
  String getToolId();

  /** Human-readable description. */
  String getDescription();

  /** Optional parameter validation (default: non-null map). */
  default boolean validateParameters(Map<String, Object> parameters) {
    return parameters != null;
  }
}

package org.jagentic.tools;

import java.util.List;

import org.jagentic.core.ToolRegistry;

/** A named bundle of self-describing tools. A pack registers its tools (with input
 * JSON-schemas) into a {@link ToolRegistry}; that registry is then exposed over any
 * transport (MCP / REST / gRPC / pub-sub) by {@code tool-services-app}. Packs are
 * Flink-free and depend only on {@code jagentic-core} + their own tool libraries. */
public interface ToolPack {

  /** Short, stable pack name (e.g. {@code "util"}, {@code "web"}). */
  String name();

  /** Register this pack's tools into {@code registry}; return the registered tool ids. */
  List<String> register(ToolRegistry registry);
}

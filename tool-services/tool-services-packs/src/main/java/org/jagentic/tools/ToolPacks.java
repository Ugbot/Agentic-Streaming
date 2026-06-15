package org.jagentic.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.ToolRegistry;
import org.jagentic.tools.util.UtilityPack;
import org.jagentic.tools.web.WebDocumentPack;

/** Discovery + assembly of {@link ToolPack}s. Build a {@link ToolRegistry} from a set of
 * pack names (e.g. the {@code TOOL_PACKS=util,web} config the service reads). New packs
 * register here. */
public final class ToolPacks {

  private ToolPacks() {}

  /** All packs known to the library, keyed by {@link ToolPack#name()}. Web/RAG/inference
   * packs are added here as they land (Phases 2 & 4). */
  public static Map<String, ToolPack> available() {
    Map<String, ToolPack> packs = new LinkedHashMap<>();
    register(packs, new UtilityPack());
    register(packs, new WebDocumentPack());
    return packs;
  }

  private static void register(Map<String, ToolPack> packs, ToolPack pack) {
    packs.put(pack.name(), pack);
  }

  /** Build a registry containing the named packs (in order). Unknown names throw. An empty
   * or null selection includes all available packs. */
  public static ToolRegistry buildRegistry(List<String> packNames) {
    Map<String, ToolPack> available = available();
    List<String> names = (packNames == null || packNames.isEmpty())
        ? new ArrayList<>(available.keySet()) : packNames;
    ToolRegistry registry = new ToolRegistry();
    for (String raw : names) {
      String name = raw.trim();
      if (name.isEmpty()) {
        continue;
      }
      ToolPack pack = available.get(name);
      if (pack == null) {
        throw new IllegalArgumentException("unknown tool pack: " + name
            + " (available: " + available.keySet() + ")");
      }
      pack.register(registry);
    }
    return registry;
  }

  /** Convenience: parse a CSV pack selection (e.g. {@code "util,web"}). */
  public static ToolRegistry buildRegistryFromCsv(String csv) {
    if (csv == null || csv.isBlank()) {
      return buildRegistry(null);
    }
    return buildRegistry(List.of(csv.split("\\s*,\\s*")));
  }
}

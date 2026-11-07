package org.jagentic.core.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Which commands a document may start as an MCP server. {@code mcp[].command} runs a process on
 * this machine, so the list is the operator's and the document has no say in it: {@code
 * AGENTIC_MCP_ALLOWED_COMMANDS}, or the system property {@code agentic.mcp.allowed_commands}. Entries
 * are separated by {@code ;}, and each is a command with the arguments it must start with, split on
 * spaces. Deny by default: with no list, no document starts anything.
 *
 * <p>An entry of a bare interpreter, such as {@code python3}, allows whatever that interpreter will
 * run, {@code -c} included. Name the script.
 */
public final class McpCommands {

  private McpCommands() {}

  /** Thrown before anything starts, for a command the operator has not allowed. */
  public static final class NotAllowed extends RuntimeException {
    private static final long serialVersionUID = 1L;

    NotAllowed(String message) {
      super(message);
    }
  }

  /** The list in {@code settings}, which maps a property name to its value. */
  static List<List<String>> allowed(UnaryOperator<String> settings) {
    String raw = settings.apply("agentic.mcp.allowed_commands");
    List<List<String>> entries = new ArrayList<>();
    for (String entry : raw == null ? new String[0] : raw.split(";")) {
      if (!entry.isBlank()) {
        entries.add(List.of(entry.trim().split("\\s+")));
      }
    }
    return entries;
  }

  /** Throws {@link NotAllowed} unless {@code command} starts with an entry, word for word. */
  static void check(List<String> command, UnaryOperator<String> settings) {
    for (List<String> entry : allowed(settings)) {
      if (command.size() >= entry.size() && command.subList(0, entry.size()).equals(entry)) {
        return;
      }
    }
    throw new NotAllowed("mcp command " + command + " is not in AGENTIC_MCP_ALLOWED_COMMANDS");
  }

  /** As above, under this process's settings. */
  public static void check(List<String> command) {
    check(command, key -> {
      String set = System.getProperty(key);
      return set != null ? set : System.getenv(key.toUpperCase().replace('.', '_'));
    });
  }
}

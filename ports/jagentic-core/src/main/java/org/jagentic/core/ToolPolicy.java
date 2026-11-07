package org.jagentic.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The tool boundary: one decision for every tool call, whatever asked for it (spec section 11).
 * {@link AgentContext} asks it before anything else about a call, so a brain, a saga, a timer and a
 * CEP pattern cannot differ. It denies unless told otherwise.
 */
@FunctionalInterface
public interface ToolPolicy {

  /** What is being asked for. {@code effect} is null when no such tool is registered. */
  record Request(String tool, ToolOrigin origin, String path, String effect, Map<String, Object> args,
                 Principal principal) {}

  /** The answer, with the reason a refusal gives. */
  record Decision(boolean allowed, String reason) {
    public static final Decision ALLOW = new Decision(true, "");

    public static Decision deny(String reason) {
      return new Decision(false, reason);
    }
  }

  Decision decide(Request request);

  /** What every context starts with, so a graph that says nothing can call nothing. */
  ToolPolicy DENY_ALL = r -> Decision.deny("no tool policy: tool " + r.tool() + " is refused");

  /** For code that builds a graph itself and owns every tool in it. A document can never reach this. */
  ToolPolicy ALLOW_ALL = r -> Decision.ALLOW;

  /** What a document says: a path's tools, and what timers and CEP patterns may call. */
  static ToolPolicy of(Map<String, Set<String>> pathTools, Set<String> unattended) {
    Map<String, Set<String>> paths = new LinkedHashMap<>(pathTools);
    Set<String> background = Set.copyOf(unattended);
    return r -> {
      if (r.origin().unattended()) {
        return background.contains(r.tool()) ? Decision.ALLOW
            : Decision.deny("tool " + r.tool() + " is not in policies.unattended_tools");
      }
      return paths.getOrDefault(r.path(), Set.of()).contains(r.tool()) ? Decision.ALLOW
          : Decision.deny("tool " + r.tool() + " is not in the tools of path " + r.path());
    };
  }
}

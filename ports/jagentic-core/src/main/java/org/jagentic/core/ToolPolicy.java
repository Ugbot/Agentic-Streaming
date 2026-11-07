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

  /** The answer, with the reason a refusal gives. {@code needsPerson}: allowed once an approver says so. */
  record Decision(boolean allowed, String reason, boolean needsPerson) {
    public static final Decision ALLOW = new Decision(true, "", false);
    public static final Decision ASK_A_PERSON = new Decision(true, "", true);

    public static Decision deny(String reason) {
      return new Decision(false, reason, false);
    }
  }

  Decision decide(Request request);

  /**
   * True when the answer can change between two asks. A call already begun is then not asked about
   * again, so a re-run takes the path the first run took.
   */
  default boolean outsideThisProcess() {
    return false;
  }

  /** Both must allow: {@code first} is asked first, so a refusal it gives never leaves the process. */
  default ToolPolicy and(ToolPolicy second) {
    ToolPolicy first = this;
    return new ToolPolicy() {
      @Override
      public Decision decide(Request request) {
        Decision d = first.decide(request);
        if (!d.allowed()) {
          return d;
        }
        Decision then = second.decide(request);
        return then.allowed() && d.needsPerson() ? Decision.ASK_A_PERSON : then;
      }

      @Override
      public boolean outsideThisProcess() {
        return first.outsideThisProcess() || second.outsideThisProcess();
      }
    };
  }

  /** What every context starts with, so a graph that says nothing can call nothing. */
  ToolPolicy DENY_ALL = r -> Decision.deny("no tool policy: tool " + r.tool() + " is refused");

  /** For code that builds a graph itself and owns every tool in it. A document can never reach this. */
  ToolPolicy ALLOW_ALL = r -> Decision.ALLOW;

  /** What a document says: a path's tools, and what timers and CEP patterns may call. */
  static ToolPolicy of(Map<String, Set<String>> pathTools, Set<String> unattended) {
    return of(pathTools, unattended, Set.of(), Set.of());
  }

  /** As above, with what still needs a person ({@code policies.tool_approval}), by id and by effect. */
  static ToolPolicy of(Map<String, Set<String>> pathTools, Set<String> unattended,
                       Set<String> gatedTools, Set<String> gatedEffects) {
    Map<String, Set<String>> paths = new LinkedHashMap<>(pathTools);
    Set<String> background = Set.copyOf(unattended);
    return r -> {
      boolean listed = r.origin().unattended() ? background.contains(r.tool())
          : paths.getOrDefault(r.path(), Set.of()).contains(r.tool());
      if (!listed) {
        return Decision.deny(r.origin().unattended()
            ? "tool " + r.tool() + " is not in policies.unattended_tools"
            : "tool " + r.tool() + " is not in the tools of path " + r.path());
      }
      return gatedTools.contains(r.tool()) || gatedEffects.contains(r.effect())
          ? Decision.ASK_A_PERSON : Decision.ALLOW;
    };
  }
}

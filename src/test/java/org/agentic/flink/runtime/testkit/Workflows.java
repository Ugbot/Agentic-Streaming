package org.agentic.flink.runtime.testkit;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Small {@code agentic/v1} workflow documents for the Flink runtime tests. */
public final class Workflows {
  private Workflows() {}

  /** Keyword router: {@code balance} goes to the billing path which calls one constant tool. */
  public static Map<String, Object> billing() {
    Map<String, Object> billing = new LinkedHashMap<>();
    billing.put("brain", "rule");
    billing.put("prompt", "You answer billing questions.");
    billing.put("tools", List.of("lookup_charge"));
    billing.put("tool_triggers", Map.of("balance", "lookup_charge"));
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "support");
    agent.put(
        "router",
        Map.of(
            "kind",
            "keyword",
            "default",
            "general",
            "rules",
            Map.of("billing", List.of("balance", "charge"))));
    agent.put(
        "paths",
        Map.of(
            "billing",
            billing,
            "general",
            Map.of("brain", "rule", "prompt", "You answer general questions.")));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> wf = base(agent);
    wf.put(
        "tools",
        List.of(
            Map.of(
                "id",
                "lookup_charge",
                "kind",
                "constant",
                "effect",
                "read",
                "description",
                "Last charge",
                "value",
                42.5)));
    return wf;
  }

  /** Every {@code charge} turn calls a tool that always fails; policies end the turn as failed. */
  public static Map<String, Object> failingTool() {
    Map<String, Object> main = new LinkedHashMap<>();
    main.put("brain", "rule");
    main.put("prompt", "You answer charge questions.");
    main.put("tools", List.of("broken_tool"));
    main.put("tool_triggers", Map.of("charge", "broken_tool"));
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "failing");
    agent.put(
        "router",
        Map.of("kind", "keyword", "default", "main", "rules", Map.of("main", List.of("charge"))));
    agent.put("paths", Map.of("main", main));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> wf = base(agent);
    wf.put(
        "policies",
        Map.of("on_tool_error", "fail", "retry", Map.of("kind", "none", "max_attempts", 1)));
    wf.put(
        "tools",
        List.of(
            Map.of(
                "id",
                "broken_tool",
                "kind",
                "failing",
                "effect",
                "idempotent",
                "description",
                "Always raises")));
    return wf;
  }

  /** The {@code main} path suspends until an {@code approval} signal. */
  public static Map<String, Object> approval() {
    Map<String, Object> main = new LinkedHashMap<>();
    main.put("brain", "rule");
    main.put("prompt", "You handle refunds.");
    main.put("x-suspend-until", "approval");
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "approval");
    agent.put(
        "router",
        Map.of("kind", "keyword", "default", "main", "rules", Map.of("main", List.of("refund"))));
    agent.put("paths", Map.of("main", main));
    agent.put("verifier", Map.of("kind", "none"));
    return base(agent);
  }

  /** {@link #billing()} with every path on a model, as {@code llm} declares it. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> billingOnAModel(Map<String, Object> llm) {
    Map<String, Object> wf = new LinkedHashMap<>(billing());
    Map<String, Object> agent = new LinkedHashMap<>((Map<String, Object>) wf.get("agent"));
    agent.put(
        "paths", Map.of("general", Map.of("brain", "llm"), "billing", Map.of("brain", "llm")));
    wf.put("agent", agent);
    wf.put("llm", llm);
    return wf;
  }

  /** Adds a {@code runtime.flink} block to a workflow. */
  /** The same workflow with these tools on every path, for a saga that runs whichever path a turn takes. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> onEveryPath(Map<String, Object> wf, String... tools) {
    Map<String, Object> agent = new java.util.LinkedHashMap<>((Map<String, Object>) wf.get("agent"));
    Map<String, Object> paths = new java.util.LinkedHashMap<>();
    ((Map<String, Object>) agent.get("paths"))
        .forEach(
            (name, spec) -> {
              Map<String, Object> path = new java.util.LinkedHashMap<>((Map<String, Object>) spec);
              List<Object> granted =
                  new java.util.ArrayList<>((List<Object>) path.getOrDefault("tools", List.of()));
              granted.addAll(List.of(tools));
              path.put("tools", granted);
              paths.put(name, path);
            });
    agent.put("paths", paths);
    Map<String, Object> out = new java.util.LinkedHashMap<>(wf);
    out.put("agent", agent);
    return out;
  }

  public static Map<String, Object> withFlink(Map<String, Object> wf, Map<String, Object> flink) {
    Map<String, Object> copy = new LinkedHashMap<>(wf);
    copy.put("runtime", Map.of("flink", flink));
    return copy;
  }

  private static Map<String, Object> base(Map<String, Object> agent) {
    Map<String, Object> wf = new HashMap<>();
    wf.put("spec_version", "agentic/v1");
    wf.put("backend", "local");
    wf.put("agent", agent);
    return wf;
  }
}

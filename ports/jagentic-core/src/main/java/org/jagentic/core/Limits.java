package org.jagentic.core;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * {@code policies.limits}: per-principal rates, budgets and quotas, and the kill switch (spec section
 * 11). The counters live in this object, so they are per process: on Flink, per subtask, and gone
 * after a restart. A limit here slows a principal down; it is not a billing record. Tokens are an
 * estimate, four characters to the token, because the clients here read no provider usage.
 */
public final class Limits {

  private static final long MINUTE = 60_000;
  private static final long DAY = 86_400_000;

  public static final Limits NONE = new Limits(Map.of(), key -> null);

  /** What was exceeded, as {@code limit_exceeded} records it. */
  public record Violation(String limit, Map<String, Object> detail) {}

  private final Map<String, Object> settings;
  private final UnaryOperator<String> environment;
  private final Map<List<Object>, Double> counts = new ConcurrentHashMap<>();
  private final Map<List<Object>, java.util.Set<String>> turns = new java.util.HashMap<>();
  private long day = -1;

  private Limits(Map<String, Object> settings, UnaryOperator<String> environment) {
    this.settings = settings;
    this.environment = environment;
  }

  /** Reads {@code policies.limits}; the operator's kill switch applies whatever the document says. */
  @SuppressWarnings("unchecked")
  public static Limits fromPolicies(Map<String, Object> policies) {
    return fromPolicies(policies, System::getenv);
  }

  @SuppressWarnings("unchecked")
  static Limits fromPolicies(Map<String, Object> policies, UnaryOperator<String> environment) {
    Map<String, Object> block = policies != null && policies.get("limits") instanceof Map<?, ?> m
        ? (Map<String, Object>) m : Map.of();
    return new Limits(block, environment);
  }

  /** Who the counters are kept for. */
  public static String who(Principal principal, String userId) {
    if (principal != null && principal.tenant() != null) {
      return principal.tenant() + "/" + principal.subject();
    }
    return principal != null ? principal.subject() : userId == null ? "anonymous" : userId;
  }

  /** The document's switch, {@code AGENTIC_KILL_SWITCH=true}, or the {@code AGENTIC_KILL_SWITCH_FILE} existing. */
  public boolean stopped() {
    if (Boolean.TRUE.equals(settings.get("kill_switch"))
        || "true".equalsIgnoreCase(environment.apply("AGENTIC_KILL_SWITCH"))) {
      return true;
    }
    String file = environment.apply("AGENTIC_KILL_SWITCH_FILE");
    return file != null && !file.isBlank() && Files.exists(Path.of(file));
  }

  /** A turn counts against the rate once, however many times a crash redelivers it. */
  public Violation admitTurn(String who, String turnId, long nowMs) {
    if (stopped()) {
      return new Violation("kill_switch", Map.of());
    }
    if (settings.get("turns_per_minute") instanceof Number rate && !admitted(who, turnId, nowMs / MINUTE, rate.intValue())) {
      return new Violation("turns_per_minute", Map.of("max", rate));
    }
    return null;
  }

  private synchronized boolean admitted(String who, String turnId, long minute, int rate) {
    turns.keySet().removeIf(slot -> (long) slot.get(1) < minute);
    java.util.Set<String> seen = turns.computeIfAbsent(List.of(who, minute), k -> new java.util.HashSet<>());
    return seen.contains(turnId) || seen.size() < rate && seen.add(turnId);
  }

  /** A model call needs budget left. A budget is spent by the call that crosses it, not refused by it. */
  public Violation admitModel(String who, long nowMs) {
    if (stopped()) {
      return new Violation("kill_switch", Map.of());
    }
    for (String[] budget : new String[][] {{"tokens_per_day", "tokens"}, {"cost_per_day", "cost"}}) {
      if (settings.get(budget[0]) instanceof Number max
          && counts.getOrDefault(List.of(budget[1], who, nowMs / DAY), 0.0) >= max.doubleValue()) {
        return new Violation(budget[0], Map.of("max", max));
      }
    }
    return null;
  }

  /** What a model call came to, by its size. */
  public void chargeModel(String who, long nowMs, int characters) {
    long tokens = Math.max(1, characters / 4);
    long today = today(nowMs);
    counts.merge(List.of("tokens", who, today), (double) tokens, Double::sum);
    if (settings.get("cost_per_1k_tokens") instanceof Number rate) {
      counts.merge(List.of("cost", who, today), tokens / 1000.0 * rate.doubleValue(), Double::sum);
    }
  }

  /**
   * The day of {@code nowMs}. When it is a new one, the counters of days that are over are dropped,
   * so they do not grow with every principal that ever called.
   */
  private synchronized long today(long nowMs) {
    long now = nowMs / DAY;
    if (now > day) {
      day = now;
      counts.keySet().removeIf(slot -> (long) slot.get(slot.size() - 1) < now);
    }
    return now;
  }

  /** How many counters are kept, for a test. */
  int counters() {
    return counts.size();
  }

  /** A tool call that has not begun counts against the quota. */
  public Violation admitTool(String who, long nowMs, String tool, boolean begun) {
    if (stopped()) {
      return new Violation("kill_switch", detail(tool, null));
    }
    Object quota = settings.get("tool_calls_per_day") instanceof Map<?, ?> m ? m.get(tool) : null;
    if (!begun && quota instanceof Number max && !take(List.of("tool", who, tool, today(nowMs)), max.doubleValue())) {
      return new Violation("tool_calls_per_day", detail(tool, max));
    }
    return null;
  }

  /** True when a quota applies, so the caller knows whether to look up if the call has begun. */
  public boolean hasQuota(String tool) {
    return settings.get("tool_calls_per_day") instanceof Map<?, ?> m && m.containsKey(tool);
  }

  private static Map<String, Object> detail(String tool, Number max) {
    Map<String, Object> d = new LinkedHashMap<>();
    d.put("tool", tool);
    if (max != null) {
      d.put("max", max);
    }
    return d;
  }

  private synchronized boolean take(List<Object> slot, double max) {
    double used = counts.getOrDefault(slot, 0.0);
    if (used + 1 > max) {
      return false;
    }
    counts.put(slot, used + 1);
    return true;
  }
}

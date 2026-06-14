package org.jagentic.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The canonical topology: classify (router) -> dispatch to a specialized path agent
 * -> validate (verifier). The chosen path + phase are persisted to the
 * ConversationStore so a multi-turn conversation stays on its path and the next turn
 * can resume — what the Flink {@code BankingAgentGraph} does with routed keyed state.
 */
public final class RoutedGraph {

  /** router(event, ctx) -> path key. */
  @FunctionalInterface
  public interface Router extends BiFunction<Event, AgentContext, String> {}

  /** verifier(reply, ctx) -> [ok, annotatedReply]. */
  @FunctionalInterface
  public interface Verifier {
    /** @return ok flag (index 0 as Boolean) packed with the possibly-annotated reply. */
    Result verify(String reply, AgentContext ctx);

    record Result(boolean ok, String reply) {}
  }

  public static final String PHASE_ATTR = "graph.phase";
  public static final String PATH_ATTR = "graph.path";

  private final Router router;
  private final Map<String, Agent> paths;
  private final Verifier verifier; // may be null
  private final java.util.List<Guardrail> guardrails;
  private final java.util.List<AgentListener> listeners;

  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier) {
    this(router, paths, verifier, java.util.List.of(), java.util.List.of());
  }

  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier,
                     java.util.List<Guardrail> guardrails, java.util.List<AgentListener> listeners) {
    if (paths == null || paths.isEmpty()) {
      throw new IllegalArgumentException("RoutedGraph requires at least one path");
    }
    this.router = router;
    this.paths = new LinkedHashMap<>(paths);
    this.verifier = verifier;
    this.guardrails = java.util.List.copyOf(guardrails == null ? java.util.List.of() : guardrails);
    this.listeners = java.util.List.copyOf(listeners == null ? java.util.List.of() : listeners);
  }

  public TurnResult handle(Event event, AgentContext ctx) {
    String cid = event.conversationId();
    ctx.listeners = listeners; // so callTool can fire tool-call hooks
    for (AgentListener l : listeners) {
      l.onTurnStart(event, ctx);
    }

    // Input guardrails: short-circuit a blocked turn before routing.
    for (Guardrail g : guardrails) {
      String reason = g.checkInput(event.text());
      if (reason != null) {
        ctx.store.putAttribute(cid, PHASE_ATTR, "blocked");
        TurnResult blocked = new TurnResult(cid, "[blocked] " + reason, java.util.List.of());
        blocked.path = "blocked";
        blocked.ok = false;
        for (AgentListener l : listeners) {
          l.onGuardrailBlock(reason, ctx);
          l.onTurnEnd(blocked, ctx);
        }
        return blocked;
      }
    }

    ctx.store.putAttribute(cid, PHASE_ATTR, "router");
    String path = router.apply(event, ctx);
    if (!paths.containsKey(path)) {
      path = paths.keySet().iterator().next(); // fall back to first declared path
    }
    ctx.store.putAttribute(cid, PATH_ATTR, path);
    ctx.store.putAttribute(cid, PHASE_ATTR, "path:" + path);
    for (AgentListener l : listeners) {
      l.onRouted(path, ctx);
    }

    TurnResult result = paths.get(path).turn(event, ctx);
    result.path = path;

    if (verifier != null) {
      ctx.store.putAttribute(cid, PHASE_ATTR, "verifier");
      Verifier.Result v = verifier.verify(result.reply, ctx);
      result.ok = v.ok();
      result.reply = v.reply();
    }

    // Output guardrails: redact/replace a disallowed reply.
    for (Guardrail g : guardrails) {
      String reason = g.checkOutput(result.reply);
      if (reason != null) {
        result.reply = "[blocked] " + reason;
        result.ok = false;
        for (AgentListener l : listeners) {
          l.onGuardrailBlock(reason, ctx);
        }
      }
    }

    ctx.store.putAttribute(cid, PHASE_ATTR, "done");
    for (AgentListener l : listeners) {
      l.onTurnEnd(result, ctx);
    }
    return result;
  }
}

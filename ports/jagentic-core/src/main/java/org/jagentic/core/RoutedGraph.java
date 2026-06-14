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

  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier) {
    if (paths == null || paths.isEmpty()) {
      throw new IllegalArgumentException("RoutedGraph requires at least one path");
    }
    this.router = router;
    this.paths = new LinkedHashMap<>(paths);
    this.verifier = verifier;
  }

  public TurnResult handle(Event event, AgentContext ctx) {
    String cid = event.conversationId();
    ctx.store.putAttribute(cid, PHASE_ATTR, "router");
    String path = router.apply(event, ctx);
    if (!paths.containsKey(path)) {
      path = paths.keySet().iterator().next(); // fall back to first declared path
    }
    ctx.store.putAttribute(cid, PATH_ATTR, path);
    ctx.store.putAttribute(cid, PHASE_ATTR, "path:" + path);

    TurnResult result = paths.get(path).turn(event, ctx);
    result.path = path;

    if (verifier != null) {
      ctx.store.putAttribute(cid, PHASE_ATTR, "verifier");
      Verifier.Result v = verifier.verify(result.reply, ctx);
      result.ok = v.ok();
      result.reply = v.reply();
    }
    ctx.store.putAttribute(cid, PHASE_ATTR, "done");
    return result;
  }
}

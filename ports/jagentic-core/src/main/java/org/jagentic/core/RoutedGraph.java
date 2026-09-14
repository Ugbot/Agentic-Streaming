package org.jagentic.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The canonical topology: guardrails -> classify (router) -> dispatch to a path agent -> verify,
 * with the v1 turn semantics layered on top: every step appends to the conversation log through the
 * {@link AgentContext}, a redelivered {@code turn_id} is answered from the log without re-running
 * anything, the verifier may reject up to {@code policies.verification.max_attempts} drafts, a
 * declared {@code saga} runs its steps and compensates completed ones in reverse order on failure,
 * and a path marked {@code x-suspend-until} parks the turn until a signal with the same
 * {@code turn_id} resumes it.
 *
 * <p>The chosen path and phase are also mirrored into {@link ConversationStore} attributes so
 * pre-spec callers that read {@link #PATH_ATTR}/{@link #PHASE_ATTR} keep working; the log remains
 * the source of truth.</p>
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

    /** Accepts every reply unchanged: what a declared {@code kind: none} means. */
    Verifier ACCEPT = (reply, ctx) -> new Result(true, reply);
  }

  public static final String PHASE_ATTR = "graph.phase";
  public static final String PATH_ATTR = "graph.path";
  /** Path extension key: the turn suspends after routing until a signal arrives. */
  public static final String SUSPEND_UNTIL = "x-suspend-until";

  private final Router router;
  private final Map<String, Agent> paths;
  private final Verifier verifier; // may be null
  private final Map<String, Verifier> pathVerifiers;
  private final List<Guardrail> guardrails;
  private final List<AgentListener> listeners;
  private final Policies policies;
  private final SagaPlan saga; // may be null
  private final Map<String, String> suspendUntil;
  private final ContextWindow contextWindow;
  private final List<org.jagentic.core.cep.SequencePattern> cep;
  private final List<TimerSpec> timers;

  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier) {
    this(router, paths, verifier, List.of(), List.of());
  }

  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier,
                     List<Guardrail> guardrails, List<AgentListener> listeners) {
    this(router, paths, verifier, guardrails, listeners, Policies.DEFAULTS, null, Map.of());
  }

  /**
   * @param suspendUntil path name -> reason for paths that suspend after routing
   *     ({@code x-suspend-until}); empty when no path suspends
   */
  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier,
                     List<Guardrail> guardrails, List<AgentListener> listeners, Policies policies,
                     SagaPlan saga, Map<String, String> suspendUntil) {
    this(router, paths, verifier, Map.of(), guardrails, listeners, policies, saga, suspendUntil);
  }

  /**
   * @param verifier the workflow-level verifier ({@code agent.verifier}) for paths without their own;
   *     null means no verification on those paths
   * @param pathVerifiers path name -> that path's own verifier ({@code agent.paths.<name>.verifier}),
   *     which replaces {@code verifier} for turns routed to it; a path absent here falls back
   */
  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier,
                     Map<String, Verifier> pathVerifiers, List<Guardrail> guardrails,
                     List<AgentListener> listeners, Policies policies, SagaPlan saga,
                     Map<String, String> suspendUntil) {
    this(router, paths, verifier, pathVerifiers, guardrails, listeners, policies, saga, suspendUntil,
        ContextWindow.NONE);
  }

  /**
   * @param contextWindow the workflow's {@code context} block; bounds the transcript every turn's
   *     {@link AgentContext#conversationState()} folds, and with it {@code state.transcript_length}
   */
  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier,
                     Map<String, Verifier> pathVerifiers, List<Guardrail> guardrails,
                     List<AgentListener> listeners, Policies policies, SagaPlan saga,
                     Map<String, String> suspendUntil, ContextWindow contextWindow) {
    this(router, paths, verifier, pathVerifiers, guardrails, listeners, policies, saga, suspendUntil,
        contextWindow, List.of());
  }

  /**
   * @param cep the workflow's sequence patterns with {@code on_match.kind: tool}, evaluated over
   *     the conversation log after {@code routed} on every turn; empty when the workflow has none
   */
  public RoutedGraph(Router router, Map<String, Agent> paths, Verifier verifier,
                     Map<String, Verifier> pathVerifiers, List<Guardrail> guardrails,
                     List<AgentListener> listeners, Policies policies, SagaPlan saga,
                     Map<String, String> suspendUntil, ContextWindow contextWindow,
                     List<org.jagentic.core.cep.SequencePattern> cep) {
    if (paths == null || paths.isEmpty()) {
      throw new IllegalArgumentException("RoutedGraph requires at least one path");
    }
    this.router = router;
    this.paths = new LinkedHashMap<>(paths);
    this.verifier = verifier;
    this.pathVerifiers = pathVerifiers == null ? Map.of() : Map.copyOf(pathVerifiers);
    for (String name : this.pathVerifiers.keySet()) {
      if (!this.paths.containsKey(name)) {
        throw new IllegalArgumentException("verifier declared for unknown path " + name);
      }
    }
    this.guardrails = List.copyOf(guardrails == null ? List.of() : guardrails);
    this.listeners = List.copyOf(listeners == null ? List.of() : listeners);
    this.policies = policies == null ? Policies.DEFAULTS : policies;
    this.saga = saga;
    this.suspendUntil = suspendUntil == null ? Map.of() : Map.copyOf(suspendUntil);
    this.contextWindow = contextWindow == null ? ContextWindow.NONE : contextWindow;
    this.cep = List.copyOf(cep == null ? List.of() : cep);
    this.timers = List.of();
  }

  private RoutedGraph(RoutedGraph base, List<TimerSpec> timers) {
    this.router = base.router;
    this.paths = base.paths;
    this.verifier = base.verifier;
    this.pathVerifiers = base.pathVerifiers;
    this.guardrails = base.guardrails;
    this.listeners = base.listeners;
    this.policies = base.policies;
    this.saga = base.saga;
    this.suspendUntil = base.suspendUntil;
    this.contextWindow = base.contextWindow;
    this.cep = base.cep;
    this.timers = List.copyOf(timers == null ? List.of() : timers);
  }

  /**
   * The same graph with the workflow's {@code timers} (spec section 8): scheduled on a conversation's
   * first turn and fired, before {@code turn_received}, on the first later turn delivered at or past
   * their deadline, reading {@link AgentContext#clock} for processing time and the folded watermark
   * for event time.
   */
  public RoutedGraph withTimers(List<TimerSpec> timers) {
    return new RoutedGraph(this, timers);
  }

  /** The sequence patterns this graph evaluates in-turn (empty when the workflow declares none). */
  public List<org.jagentic.core.cep.SequencePattern> cep() {
    return cep;
  }

  public Policies policies() {
    return policies;
  }

  public SagaPlan saga() {
    return saga;
  }

  public ContextWindow contextWindow() {
    return contextWindow;
  }

  /** The workflow's declared timers, empty when it has none. */
  public List<TimerSpec> timers() {
    return timers;
  }

  public List<AgentListener> listeners() {
    return listeners;
  }

  public java.util.Set<String> pathNames() {
    return paths.keySet();
  }

  /**
   * The verifier that judges turns routed to {@code path}: the path's own when it declares one,
   * else the workflow-level one; null when neither verifies.
   */
  public Verifier verifierFor(String path) {
    Verifier own = pathVerifiers.get(path);
    return own != null ? own : verifier;
  }

  public TurnResult handle(Event event, AgentContext ctx) {
    ctx.listeners = listeners; // so callTool can fire tool-call hooks
    ctx.contextWindow = contextWindow;
    ConversationState before = ctx.conversationState();

    if (event.isResume() && before.suspended().containsKey(event.turnId())) {
      return resume(event, ctx, before.suspended().get(event.turnId()));
    }
    ConversationState.TurnRecord prior = before.turn(event.turnId());
    if (policies.idempotency() == Policies.Idempotency.TURN_ID && prior != null
        && prior.status() != null) {
      return recorded(ctx, prior).asDuplicate(before.reduced());
    }

    for (AgentListener l : listeners) {
      l.onTurnStart(event, ctx);
    }
    Long eventTime = WorkflowTimers.eventTimeOf(event);
    Long watermark = before.timers().watermarkAfter(eventTime);
    Long processingNow = timers.isEmpty() ? null : WorkflowTimers.processingNow(timers, ctx);
    boolean firstTurn = before.nextSequence() == 0;
    if (!firstTurn && processingNow != null) {
      WorkflowTimers.fireDue(timers, before.timers(), ctx, processingNow, watermark);
    }
    Map<String, Object> received = org.jagentic.core.cep.EventTime.annotate(
        map("turn_id", event.turnId(), "text", event.text()), event);
    if (processingNow != null) {
      received.put(LogicalClock.PROCESSING_TIME_KEY, processingNow);
    }
    ctx.record(EventType.TURN_RECEIVED, received);
    if (firstTurn && processingNow != null) {
      WorkflowTimers.schedule(timers, ctx, processingNow, watermark);
    }

    for (Guardrail g : guardrails) {
      String reason = g.checkInput(event.text());
      if (reason != null) {
        ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "blocked");
        String reply = "[blocked] " + reason;
        ctx.record(EventType.GUARDRAIL_REJECTED, map("reason", reason, "reply", reply));
        for (AgentListener l : listeners) {
          l.onGuardrailBlock(reason, ctx);
        }
        return finish(ctx, TurnStatus.REJECTED, null, reply,
            new TurnError(TurnError.ErrorClass.GUARDRAIL, reason), null);
      }
    }

    ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "router");
    String path = router.apply(event, ctx);
    if (!paths.containsKey(path)) {
      return finish(ctx, TurnStatus.FAILED, null, null,
          new TurnError(TurnError.ErrorClass.VALIDATION, "router selected unknown path " + path),
          EventType.TURN_FAILED);
    }
    ctx.store.putAttribute(ctx.conversationId, PATH_ATTR, path);
    ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "path:" + path);
    ctx.record(EventType.ROUTED, map("path", path));
    for (AgentListener l : listeners) {
      l.onRouted(path, ctx);
    }
    if (!cep.isEmpty()) {
      try {
        org.jagentic.core.cep.TurnPatterns.evaluate(cep, ctx);
      } catch (ToolFailure e) {
        return fail(ctx, path, TurnError.ErrorClass.TOOL, e);
      } catch (IllegalArgumentException e) {
        return fail(ctx, path, TurnError.ErrorClass.VALIDATION, e);
      }
    }

    String until = suspendUntil.get(path);
    if (until != null) {
      ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "suspended");
      Map<String, Object> p = map("turn_id", event.turnId(), "path", path);
      p.put("text", event.text());
      p.put("until", until);
      ctx.record(EventType.TURN_SUSPENDED, p);
      return finish(ctx, TurnStatus.SUSPENDED, path, null, null, null);
    }

    if (saga != null) {
      return runSaga(event, ctx, path);
    }
    return runBrain(event, ctx, path);
  }

  private TurnResult resume(Event signal, AgentContext ctx, ConversationState.Suspended pending) {
    Event original = new Event(signal.conversationId(), signal.turnId(), signal.userId(),
        pending.text(), signal.metadata(), null);
    for (AgentListener l : listeners) {
      l.onTurnStart(original, ctx);
    }
    ctx.record(EventType.TURN_RESUMED, map("turn_id", signal.turnId(), "signal", signal.signal()));
    ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "path:" + pending.path());
    return runBrain(original, ctx, pending.path());
  }

  private TurnResult runBrain(Event event, AgentContext ctx, String path) {
    Agent agent = paths.get(path);
    if (event.userId() != null) {
      ctx.store.associateUser(event.conversationId(), event.userId());
    }
    ctx.record(EventType.BRAIN_STARTED, map("path", path));
    Policies.VerificationPolicy vp = policies.verification();
    Verifier pathVerifier = verifierFor(path);
    String reply = null;
    for (int attempt = 1; attempt <= vp.maxAttempts(); attempt++) {
      try {
        reply = agent.brain.turn(event.text(), ctx);
      } catch (ToolFailure e) {
        return fail(ctx, path, TurnError.ErrorClass.TOOL, e);
      } catch (ToolRegistry.UnknownTool | ToolNotPermitted e) {
        return fail(ctx, path, TurnError.ErrorClass.VALIDATION, e);
      } catch (RuntimeException e) {
        for (AgentListener l : listeners) {
          l.onError("brain:" + path, e, ctx);
        }
        return fail(ctx, path, TurnError.ErrorClass.FATAL, e);
      }
      ctx.record(EventType.REPLY_DRAFTED, map("reply", reply));
      boolean ok = true;
      if (pathVerifier != null) {
        ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "verifier");
        Verifier.Result v = pathVerifier.verify(reply, ctx);
        ok = v.ok();
        reply = v.reply();
      }
      if (ok) {
        for (Guardrail g : guardrails) {
          String reason = g.checkOutput(reply);
          if (reason != null) {
            String blocked = "[blocked] " + reason;
            ctx.record(EventType.GUARDRAIL_REJECTED, map("reason", reason, "reply", blocked));
            for (AgentListener l : listeners) {
              l.onGuardrailBlock(reason, ctx);
            }
            return finish(ctx, TurnStatus.REJECTED, path, blocked,
                new TurnError(TurnError.ErrorClass.GUARDRAIL, reason), null);
          }
        }
        return complete(event, ctx, path, reply);
      }
      ctx.record(EventType.VERIFICATION_FAILED, map("reply", reply, "attempt", attempt));
    }
    TurnStatus status = vp.onExhausted() == Policies.VerificationPolicy.OnExhausted.FAIL
        ? TurnStatus.FAILED : TurnStatus.UNVERIFIED;
    return finish(ctx, status, path, reply,
        new TurnError(TurnError.ErrorClass.VERIFICATION, "verifier rejected the reply"), null);
  }

  private TurnResult runSaga(Event event, AgentContext ctx, String path) {
    ctx.store.associateUser(event.conversationId(), event.userId());
    List<SagaPlan.Step> done = new ArrayList<>();
    for (SagaPlan.Step step : saga.steps()) {
      try {
        ctx.invoke(step.tool(), step.args(), null);
      } catch (ToolFailure | ToolRegistry.UnknownTool e) {
        ctx.record(EventType.COMPENSATION_STARTED, map("failed_step", step.name()));
        for (int i = done.size() - 1; i >= 0; i--) {
          SagaPlan.Step completed = done.get(i);
          String undo = completed.compensateWith();
          if (undo != null) {
            ctx.invoke(undo, Map.of(), EventType.COMPENSATION_STEP);
          }
        }
        ctx.record(EventType.COMPENSATION_COMPLETED, map("steps", done.size()));
        return fail(ctx, path, e instanceof ToolFailure ? TurnError.ErrorClass.TOOL
            : TurnError.ErrorClass.VALIDATION, e);
      }
      done.add(step);
    }
    return complete(event, ctx, path, "[" + path + "] saga completed");
  }

  private TurnResult fail(AgentContext ctx, String path, TurnError.ErrorClass cls, RuntimeException e) {
    String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    return finish(ctx, TurnStatus.FAILED, path, null, new TurnError(cls, message), EventType.TURN_FAILED);
  }

  private TurnResult complete(Event event, AgentContext ctx, String path, String reply) {
    List<Map<String, Object>> messages = new ArrayList<>(2);
    messages.add(map("role", "user", "text", event.text()));
    messages.add(map("role", "assistant", "text", reply));
    ctx.record(EventType.MEMORY_WRITTEN, map("messages", messages));
    ctx.store.append(event.conversationId(), ChatMessage.user(event.text()));
    ctx.store.append(event.conversationId(), ChatMessage.assistant(reply));
    return finish(ctx, TurnStatus.COMPLETED, path, reply, null, EventType.TURN_COMPLETED);
  }

  /** Appends the terminal event (if any) and builds the result from this turn's events. */
  private TurnResult finish(AgentContext ctx, TurnStatus status, String path, String reply,
                            TurnError error, EventType terminal) {
    if (terminal == EventType.TURN_COMPLETED) {
      ctx.record(terminal, map("reply", reply));
    } else if (terminal == EventType.TURN_FAILED) {
      Map<String, Object> p = map("status", status.wire(), "reason", error.message());
      p.put("error_class", error.errorClass().wire());
      ctx.record(terminal, p);
    } else if (status == TurnStatus.REJECTED || status == TurnStatus.UNVERIFIED
        || status == TurnStatus.FAILED) {
      Map<String, Object> p = map("status", status.wire(), "reason", error.message());
      p.put("error_class", error.errorClass().wire());
      ctx.record(EventType.TURN_FAILED, p);
    }
    ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR,
        status == TurnStatus.SUSPENDED ? "suspended" : "done");
    TurnResult result = new TurnResult(ctx.conversationId, ctx.turnId, status, path, reply, error,
        ctx.calls, ctx.events, ctx.conversationState().reduced());
    for (AgentListener l : listeners) {
      l.onTurnEnd(result, ctx);
    }
    return result;
  }

  /** Rebuilds the recorded result of an applied turn from the log alone. */
  private static TurnResult recorded(AgentContext ctx, ConversationState.TurnRecord rec) {
    return new TurnResult(ctx.conversationId, rec.turnId(), rec.status(), rec.path(), rec.reply(),
        rec.error(), rec.calls(), rec.events(), Map.of());
  }

  private static Map<String, Object> map(String k1, Object v1, String k2, Object v2) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(k1, v1);
    m.put(k2, v2);
    return m;
  }

  private static Map<String, Object> map(String k, Object v) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(k, v);
    return m;
  }
}

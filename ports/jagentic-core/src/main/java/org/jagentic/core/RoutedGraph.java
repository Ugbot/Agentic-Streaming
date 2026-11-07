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
  private final Map<String, List<Guardrail>> pathGuardrails;
  private final String agentId;
  private final ToolPolicy toolPolicy;
  private final Approvers approvers;
  private final Limits limits;

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
    this.pathGuardrails = Map.of();
    this.agentId = "";
    this.toolPolicy = ToolPolicy.DENY_ALL;
    this.approvers = Approvers.NONE;
    this.limits = Limits.NONE;
  }

  private RoutedGraph(RoutedGraph base, List<TimerSpec> timers,
                      Map<String, List<Guardrail>> pathGuardrails, String agentId,
                      ToolPolicy toolPolicy, Approvers approvers, Limits limits) {
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
    this.pathGuardrails = Map.copyOf(pathGuardrails);
    this.agentId = agentId;
    this.toolPolicy = toolPolicy;
    this.approvers = approvers;
    this.limits = limits;
  }

  /**
   * The same graph with the workflow's {@code timers} (spec section 8): scheduled on a conversation's
   * first turn and fired, before {@code turn_received}, on the first later turn delivered at or past
   * their deadline, reading {@link AgentContext#clock} for processing time and the folded watermark
   * for event time.
   */
  public RoutedGraph withTimers(List<TimerSpec> timers) {
    return new RoutedGraph(this, timers, pathGuardrails, agentId, toolPolicy, approvers, limits);
  }

  /** The same graph under this policy. Without one every tool call is refused. */
  public RoutedGraph withToolPolicy(ToolPolicy policy) {
    return new RoutedGraph(this, timers, pathGuardrails, agentId, policy, approvers, limits);
  }

  /** The same graph under these approvers. Without them nobody can resume a turn. */
  public RoutedGraph withApprovers(Approvers who) {
    return new RoutedGraph(this, timers, pathGuardrails, agentId, toolPolicy, who, limits);
  }

  /** The same graph under these limits. */
  public RoutedGraph withLimits(Limits l) {
    return new RoutedGraph(this, timers, pathGuardrails, agentId, toolPolicy, approvers, l);
  }

  /** The same graph with rails that apply to one path alone, after routing. */
  public RoutedGraph withPathGuardrails(Map<String, List<Guardrail>> rails) {
    return new RoutedGraph(this, timers, rails, agentId, toolPolicy, approvers, limits);
  }

  /** The same graph under the workflow's {@code agent.id}, which scopes its journal entries. */
  public RoutedGraph withAgentId(String id) {
    return new RoutedGraph(this, timers, pathGuardrails, id == null ? "" : id, toolPolicy, approvers, limits);
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

  /**
   * The turn boundary. It never throws for anything a turn does: whatever escapes the turn's own
   * handling becomes a terminal {@code turn_failed} of class {@code fatal} on the log. Only {@link
   * LogUnavailable} passes through, since a turn cannot record its end in a log it cannot reach.
   */
  public TurnResult handle(Event event, AgentContext ctx) {
    ctx.listeners = listeners; // so callTool can fire tool-call hooks
    ctx.contextWindow = contextWindow;
    ctx.journalScope = agentId;
    ctx.policy = toolPolicy;
    ctx.limits = limits;
    ctx.principal = event.principal();
    if (event.turnId() == null || event.turnId().isBlank()) {
      return refused(event, TurnError.ErrorClass.VALIDATION,
          "a turn needs a non-empty turn_id; it is the idempotency key", ctx.conversationState());
    }
    try {
      return process(event, ctx);
    } catch (LogUnavailable e) {
      throw e;
    } catch (RuntimeException e) {
      return failAtBoundary(ctx, e);
    }
  }

  private TurnResult failAtBoundary(AgentContext ctx, RuntimeException e) {
    for (AgentListener l : listeners) {
      try {
        l.onError("turn", e, ctx);
      } catch (RuntimeException ignored) {
        // a listener observes the failure; it cannot change the outcome
      }
    }
    String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    TurnError error = new TurnError(TurnError.ErrorClass.FATAL, message);
    if (!endedOnTheLog(ctx)) {
      Map<String, Object> p = map("status", TurnStatus.FAILED.wire(), "reason", message);
      p.put("error_class", error.errorClass().wire());
      ctx.record(EventType.TURN_FAILED, p);
    }
    return new TurnResult(ctx.conversationId, ctx.turnId, TurnStatus.FAILED, routedPath(ctx), null,
        error, ctx.calls, ctx.segment(), stateOrEmpty(ctx));
  }

  private static boolean endedOnTheLog(AgentContext ctx) {
    if (ctx.events.isEmpty()) {
      return false;
    }
    String last = ctx.events.get(ctx.events.size() - 1).type();
    return EventType.TURN_FAILED.wire().equals(last) || EventType.TURN_COMPLETED.wire().equals(last);
  }

  private static String routedPath(AgentContext ctx) {
    for (LogEvent e : ctx.events) {
      if (EventType.ROUTED.wire().equals(e.type())) {
        return String.valueOf(e.payload().get("path"));
      }
    }
    return null;
  }

  /** The reduced state, or empty when the log cannot be folded: a corrupt log still gets a result. */
  private static Map<String, Object> stateOrEmpty(AgentContext ctx) {
    try {
      return ctx.conversationState().reduced();
    } catch (LogUnavailable e) {
      throw e;
    } catch (RuntimeException e) {
      return Map.of();
    }
  }

  /** The resume state machine of spec section 10, one branch per cell. */
  private TurnResult process(Event event, AgentContext ctx) {
    ConversationState before = ctx.conversationState();
    String turnId = event.turnId();
    ConversationState.TurnRecord prior = before.turn(turnId);
    ConversationState.Suspended parked = before.suspended().get(turnId);
    boolean idempotent = policies.idempotency() == Policies.Idempotency.TURN_ID;
    boolean applied = prior != null && prior.status() != null;
    boolean running = prior != null && prior.running();
    boolean retryable = idempotent && applied && prior.retryable();

    String tenant = tenantOf(ctx);
    if (!event.isResume() && tenant != null
        && (event.principal() == null || !tenant.equals(event.principal().tenant()))) {
      return refused(event, TurnError.ErrorClass.POLICY,
          "conversation " + event.conversationId() + " belongs to another tenant", before);
    }
    if (event.isResume() && prior != null && !ctx.runtimeSignal) {
      String problem = approvers.problem(event.principal(), tenant);
      if (problem != null) {
        return refuseSignal(event, ctx, problem);
      }
    }
    if (event.isResume()) {
      if (parked != null) {
        return resume(event, ctx, parked, prior);
      }
      if (running && prior.resumed()) {
        return replay(event, ctx, prior, true);
      }
      if (retryable && reachedNonIdempotent(prior)) {
        ctx.replayOverAndResume(prior.events());
        Event turn = original(event, prior, ctx);
        ctx.metadata = turn.metadata();
        ctx.record(EventType.TURN_RESUMED, resumedPayload(event));
        ctx.keepIndicesOfTimers(prior.events());
        return run(turn, ctx, TimerState.fold(List.of()), false, true, false);
      }
      if (retryable) {
        return rerunResumed(event, ctx, prior);
      }
      if (applied) {
        return recorded(ctx, prior).asDuplicate(before.reduced());
      }
      return refused(event, TurnError.ErrorClass.VALIDATION, "no suspended turn " + turnId + " to resume", before);
    }
    if (running) {
      return replay(event, ctx, prior, prior.resumed());
    }
    if (retryable && reachedNonIdempotent(prior)) {
      return replay(event, ctx, prior, prior.resumed());
    }
    if (retryable) {
      ctx.continueIndicesAfter(prior.events());
      return run(original(event, prior, ctx), ctx, before.timers(), before.nextSequence() == 0, false, true);
    }
    if (applied && idempotent) {
      return recorded(ctx, prior).asDuplicate(before.reduced());
    }
    return run(event, ctx, before.timers(), before.nextSequence() == 0, false, false);
  }

  /** Such a run is replayed, not run again: that tool is never called twice (spec section 4). */
  private static boolean reachedNonIdempotent(ConversationState.TurnRecord prior) {
    for (LogEvent e : prior.events()) {
      if (e.is(EventType.TOOL_INTENT) && ToolEffect.NON_IDEMPOTENT.equals(e.payload().get("effect"))) {
        return true;
      }
    }
    return false;
  }

  /** Status {@code failed}, nothing appended: the turn id stays unspent. */
  private static TurnResult refused(Event event, TurnError.ErrorClass cls, String reason,
                                    ConversationState state) {
    return new TurnResult(event.conversationId(), event.turnId() == null ? "" : event.turnId(),
        TurnStatus.FAILED, null, null, new TurnError(cls, reason),
        List.of(), List.of(), state == null ? Map.of() : state.reduced());
  }

  /** The tenant a conversation belongs to: the one that started it (spec section 11). */
  private static String tenantOf(AgentContext ctx) {
    for (LogEvent e : ctx.log.events(ctx.conversationId)) {
      if (e.is(EventType.TURN_RECEIVED)) {
        Principal first = Principal.fromMap(e.payload().get("principal"));
        return first == null ? null : first.tenant();
      }
    }
    return null;
  }

  /** Status {@code failed}, class {@code policy}. The turn is untouched, and the attempt is on the log. */
  private static TurnResult refuseSignal(Event signal, AgentContext ctx, String problem) {
    ctx.metadata = signal.metadata();
    Map<String, Object> p = map("turn_id", signal.turnId(), "reason", problem);
    p.put("signal_sender", signal.userId() == null ? "anonymous" : signal.userId());
    if (signal.principal() != null) {
      p.put("signal_principal", signal.principal().toMap());
    }
    ctx.record(EventType.SIGNAL_REFUSED, p);
    return new TurnResult(ctx.conversationId, ctx.turnId, TurnStatus.FAILED, null, null,
        new TurnError(TurnError.ErrorClass.POLICY, problem), List.of(), ctx.segment(), stateOrEmpty(ctx));
  }

  /** A turn as first received, rebuilt from the log: a re-run never trusts the delivery. */
  private static Event original(Event delivered, ConversationState.TurnRecord prior, AgentContext ctx) {
    String user = prior.userId() != null ? prior.userId() : delivered.userId();
    ctx.userId = user;
    ctx.principal = receivedBy(prior);
    return new Event(delivered.conversationId(), delivered.turnId(), user, prior.text(),
        firstMetadata(prior, delivered), null, ctx.principal);
  }

  private static Principal receivedBy(ConversationState.TurnRecord prior) {
    for (LogEvent e : prior.events()) {
      if (e.is(EventType.TURN_RECEIVED)) {
        return Principal.fromMap(e.payload().get("principal"));
      }
    }
    return null;
  }

  private static Map<String, String> firstMetadata(ConversationState.TurnRecord prior, Event delivered) {
    return prior.events().isEmpty() ? delivered.metadata() : prior.events().get(0).metadata();
  }

  private TurnResult replay(Event delivered, AgentContext ctx, ConversationState.TurnRecord prior,
                            boolean resuming) {
    ctx.replayOver(prior.events());
    Event turn = original(delivered, prior, ctx);
    List<LogEvent> others = new ArrayList<>();
    for (LogEvent e : ctx.log.events(ctx.conversationId)) {
      if (!delivered.turnId().equals(e.turnId())) {
        others.add(e);
      }
    }
    if (resuming) {
      ctx.keepIndicesOfTimers(prior.events());
    }
    return run(turn, ctx, TimerState.fold(others), others.isEmpty(), resuming, false);
  }

  private TurnResult resume(Event signal, AgentContext ctx, ConversationState.Suspended parked,
                            ConversationState.TurnRecord prior) {
    if (parked.onUnknownOutcome()) {
      String problem = resolve(signal, ctx, parked);
      if (problem != null) {
        return refused(signal, TurnError.ErrorClass.VALIDATION, problem, ctx.conversationState());
      }
    }
    Map<String, Object> resumed = resumedPayload(signal);
    if (parked.onToolApproval()) {
      if (!(signal.signal().get("approved") instanceof Boolean approved)) {
        return refused(signal, TurnError.ErrorClass.VALIDATION,
            "signal needs approved true or false, got " + signal.signal().get("approved"), ctx.conversationState());
      }
      Map<String, Object> approval = map("tool", parked.tool(), "index", parked.index());
      approval.put("approved", approved);
      resumed.put("approval", approval);
    }
    ctx.replayOverAndResume(prior.events());
    Event turn = original(signal, prior, ctx);
    ctx.metadata = turn.metadata();
    ctx.record(EventType.TURN_RESUMED, resumed);
    ctx.keepIndicesOfTimers(prior.events());
    return run(turn, ctx, TimerState.fold(List.of()), false, true, false);
  }

  private static Map<String, Object> resumedPayload(Event signal) {
    Map<String, Object> p = map("turn_id", signal.turnId(), "signal", signal.signal());
    p.put("signal_sender", signal.userId() == null ? "anonymous" : signal.userId());
    if (signal.principal() != null) {
      p.put("signal_principal", signal.principal().toMap());
    }
    return p;
  }

  /** A person says what happened to a call. Returns the reason a signal is refused, if it is. */
  private static String resolve(Event signal, AgentContext ctx, ConversationState.Suspended parked) {
    Object resolution = signal.signal().get("resolution");
    EffectJournal.Key key = new EffectJournal.Key(ctx.conversationId, ctx.turnId, EffectJournal.TOOL,
        parked.index());
    EffectJournal.Entry entry = ctx.journal.get(key).orElse(null);
    if (entry == null) {
      return "the journal holds no call " + parked.index() + " for this turn";
    }
    boolean answers = "applied".equals(resolution) || "not_applied".equals(resolution)
        || "failed".equals(resolution);
    if (answers && entry.state() != EffectJournal.State.UNKNOWN) {
      return null; // a redelivered signal: this entry has been answered
    }
    if ("applied".equals(resolution)) {
      ctx.journal.put(entry.succeeded(signal.signal().get("result")));
    } else if ("not_applied".equals(resolution)) {
      ctx.journal.put(entry.release());
    } else if ("failed".equals(resolution)) {
      String error = parked.tool() + " resolved as failed";
      ctx.journal.put(entry.withAttempt(entry.attempts().size() + 1, error).failed(error));
    } else {
      return "signal needs a resolution of applied, not_applied or failed, got " + resolution;
    }
    return null;
  }

  /** A resumed turn that failed transiently runs again on its signal, under the next free indices. */
  private TurnResult rerunResumed(Event signal, AgentContext ctx, ConversationState.TurnRecord prior) {
    ctx.continueIndicesAfter(prior.events());
    Event turn = original(signal, prior, ctx);
    ctx.metadata = turn.metadata();
    for (AgentListener l : listeners) {
      l.onTurnStart(turn, ctx);
    }
    ctx.record(EventType.TURN_RESUMED, resumedPayload(signal));
    ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "path:" + prior.path());
    return afterRouting(turn, ctx, prior.path());
  }

  /**
   * One run of a turn, from the top. A run over a turn already on the log reuses what it finds
   * ({@link AgentContext#replayOver}); {@code resuming} carries it past its suspension.
   *
   * @param timers the timer state before this turn's first event
   * @param firstTurn no other turn is on the log
   * @param rerun a transient failure runs the turn again, which has already been received
   */
  private TurnResult run(Event event, AgentContext ctx, TimerState timerState, boolean firstTurn,
                         boolean resuming, boolean rerun) {
    ctx.metadata = event.metadata();
    for (AgentListener l : listeners) {
      l.onTurnStart(event, ctx);
    }
    Long eventTime = WorkflowTimers.eventTimeOf(event);
    Long watermark = timerState.watermarkAfter(eventTime);
    Long processingNow = timers.isEmpty() ? null : WorkflowTimers.processingNow(timers, ctx);
    if (!firstTurn && !resuming && processingNow != null) {
      try {
        WorkflowTimers.fireDue(timers, timerState, ctx, processingNow, watermark);
      } catch (ToolFailure e) {
        return fail(ctx, null, TurnError.ErrorClass.TOOL, e);
      } catch (ToolRegistry.UnknownTool e) {
        return fail(ctx, null, TurnError.ErrorClass.VALIDATION, e);
      } catch (ToolRefused e) {
        return fail(ctx, null, TurnError.ErrorClass.POLICY, e);
      }
    }
    Map<String, Object> received = org.jagentic.core.cep.EventTime.annotate(
        map("turn_id", event.turnId(), "text", event.text()), event);
    received.put("user_id", event.userId() == null ? "anonymous" : event.userId());
    if (event.principal() != null) {
      received.put("principal", event.principal().toMap());
    }
    if (processingNow != null) {
      received.put(LogicalClock.PROCESSING_TIME_KEY, processingNow);
    }
    if (!rerun) {
      ctx.record(EventType.TURN_RECEIVED, received);
    }
    if (firstTurn && processingNow != null) {
      WorkflowTimers.schedule(timers, ctx, processingNow, watermark);
    }
    try {
      if (!rerun && !resuming || limits.stopped()) {
        ctx.enforce(limits.admitTurn(ctx.who(), ctx.conversationId + "\u0000" + event.turnId(), ctx.now()));
      }
    } catch (ToolRefused e) {
      return fail(ctx, null, TurnError.ErrorClass.POLICY, e);
    }

    TurnResult blocked = screenInput(guardrails, event, ctx, null);
    if (blocked != null) {
      return blocked;
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
    ctx.path = path;
    ctx.record(EventType.ROUTED, map("path", path));
    for (AgentListener l : listeners) {
      l.onRouted(path, ctx);
    }
    if (!cep.isEmpty()) {
      try {
        org.jagentic.core.cep.TurnPatterns.evaluate(cep, ctx);
      } catch (ToolFailure e) {
        return fail(ctx, path, TurnError.ErrorClass.TOOL, e);
      } catch (ToolRefused e) {
        return fail(ctx, path, TurnError.ErrorClass.POLICY, e);
      } catch (IllegalArgumentException e) {
        return fail(ctx, path, TurnError.ErrorClass.VALIDATION, e);
      }
    }
    blocked = screenInput(pathGuardrails.getOrDefault(path, List.of()), event, ctx, path);
    if (blocked != null) {
      return blocked;
    }

    String until = suspendUntil.get(path);
    if (until != null && !resuming) {
      ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "suspended");
      Map<String, Object> p = map("turn_id", event.turnId(), "path", path);
      p.put("text", event.text());
      p.put("user_id", event.userId() == null ? "anonymous" : event.userId());
      p.put("until", until);
      ctx.record(EventType.TURN_SUSPENDED, p);
      return finish(ctx, TurnStatus.SUSPENDED, path, null, null, null);
    }
    return afterRouting(event, ctx, path);
  }

  private TurnResult screenInput(List<Guardrail> rails, Event event, AgentContext ctx, String path) {
    for (Guardrail g : rails) {
      String reason = g.checkInput(event.text());
      if (reason != null) {
        ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "blocked");
        String reply = "[blocked] " + reason;
        ctx.record(EventType.GUARDRAIL_REJECTED, map("reason", reason, "reply", reply));
        for (AgentListener l : listeners) {
          l.onGuardrailBlock(reason, ctx);
        }
        return finish(ctx, TurnStatus.REJECTED, path, reply,
            new TurnError(TurnError.ErrorClass.GUARDRAIL, reason), null);
      }
    }
    return null;
  }

  /** The saga if the workflow declares one, otherwise the brain. Both can stop on an unknown outcome. */
  private TurnResult afterRouting(Event event, AgentContext ctx, String path) {
    ctx.path = path;
    try {
      return saga != null ? runSaga(event, ctx, path) : runBrain(event, ctx, path);
    } catch (TurnSuspendedOnUnknown e) {
      ctx.store.putAttribute(ctx.conversationId, PHASE_ATTR, "suspended");
      Map<String, Object> p = map("turn_id", event.turnId(), "path", path);
      p.put("text", event.text());
      p.put("user_id", event.userId() == null ? "anonymous" : event.userId());
      p.put("until", e.until);
      p.put("tool", e.tool);
      p.put("index", e.index);
      ctx.record(EventType.TURN_SUSPENDED, p);
      return finish(ctx, TurnStatus.SUSPENDED, path, null, null, null);
    }
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
      } catch (ToolRegistry.UnknownTool | ToolArgumentsRejected e) {
        return fail(ctx, path, TurnError.ErrorClass.VALIDATION, e);
      } catch (ToolRefused e) {
        return fail(ctx, path, TurnError.ErrorClass.POLICY, e);
      } catch (TransientFailure e) {
        return fail(ctx, path, TurnError.ErrorClass.TRANSIENT, e);
      } catch (LogUnavailable | TurnSuspendedOnUnknown e) {
        throw e;
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
        List<Guardrail> rails = new ArrayList<>(guardrails);
        rails.addAll(pathGuardrails.getOrDefault(path, List.of()));
        for (Guardrail g : rails) {
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
    if (event.userId() != null) {
      ctx.store.associateUser(event.conversationId(), event.userId());
    }
    List<CompletedStep> done = new ArrayList<>();
    for (SagaPlan.Step step : saga.steps()) {
      Object result;
      try {
        result = ctx.callAs(ToolOrigin.SAGA, step.tool(), step.args());
      } catch (ToolFailure | ToolRegistry.UnknownTool | ToolArgumentsRejected | ToolRefused e) {
        ctx.record(EventType.COMPENSATION_STARTED, map("failed_step", step.name()));
        List<String> failedUndos = compensate(done, ctx);
        if (!failedUndos.isEmpty()) {
          return fail(ctx, path, TurnError.ErrorClass.FATAL, new IllegalStateException(
              "compensation failed for " + failedUndos + " after: " + e.getMessage()));
        }
        ctx.record(EventType.COMPENSATION_COMPLETED, map("steps", done.size()));
        return fail(ctx, path, e instanceof ToolFailure ? TurnError.ErrorClass.TOOL
            : e instanceof ToolRefused ? TurnError.ErrorClass.POLICY : TurnError.ErrorClass.VALIDATION, e);
      }
      done.add(new CompletedStep(step, result));
    }
    return complete(event, ctx, path, "[" + path + "] saga completed");
  }

  /**
   * Runs every undo in reverse order. One that fails is recorded and passed over, so the rest still
   * run; the ids of those that failed come back for the turn's terminal error.
   */
  private static List<String> compensate(List<CompletedStep> done, AgentContext ctx) {
    List<String> failed = new ArrayList<>();
    for (int i = done.size() - 1; i >= 0; i--) {
      CompletedStep completed = done.get(i);
      String undo = completed.step().compensateWith();
      if (undo == null) {
        continue;
      }
      try {
        ctx.compensate(undo, completed.step().args(), completed.result());
      } catch (LogUnavailable | TurnSuspendedOnUnknown e) {
        throw e;
      } catch (RuntimeException e) {
        failed.add(undo);
        Map<String, Object> p = map("tool", undo, "step", completed.step().name());
        p.put("index", e instanceof ToolFailure f ? f.index() : null);
        p.put("error", e.getMessage());
        ctx.record(EventType.COMPENSATION_FAILED, p);
      }
    }
    return failed;
  }

  /** A saga step that finished, with what it returned, which is what its undo is given. */
  private record CompletedStep(SagaPlan.Step step, Object result) {}

  private TurnResult fail(AgentContext ctx, String path, TurnError.ErrorClass cls, RuntimeException e) {
    String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    return finish(ctx, TurnStatus.FAILED, path, null, new TurnError(cls, message), EventType.TURN_FAILED);
  }

  private TurnResult complete(Event event, AgentContext ctx, String path, String reply) {
    EffectJournal.Key next = ctx.nextToolKey();
    if (ctx.journal.get(next).isPresent()) {
      throw new EffectJournal.Divergence(next); // a recorded effect must not vanish behind a success
    }
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
        ctx.calls, ctx.segment(), stateOrEmpty(ctx));
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

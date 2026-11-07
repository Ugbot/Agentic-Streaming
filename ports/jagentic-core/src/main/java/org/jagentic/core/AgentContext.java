package org.jagentic.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

/**
 * Per-turn handle an agent's brain uses: the conversation's stores, the tool registry, the
 * retriever, and the recorder that appends this turn's events to the conversation log. Tool calls
 * go through {@link #callTool(String, Map)}, which applies {@code policies.retry} and records one
 * attempt-numbered {@code tool_called}/{@code tool_failed} event per attempt.
 */
public final class AgentContext {
  public final String conversationId;
  public final String turnId;
  /** Whom the turn runs as. A resume sets this back to the user of {@code turn_received}. */
  public String userId;
  public final ConversationStore store;
  public final KeyedStateStore state;
  public final ToolRegistry tools;
  public final Retrieval.TwoTierRetriever retriever; // may be null
  public final ConversationLog log;
  public final Policies policies;

  /** Structured calls made during this turn, one entry per attempt, in execution order. */
  public final List<ToolCall> calls = new ArrayList<>();
  /** The events appended during this turn, in sequence order. */
  public final List<LogEvent> events = new ArrayList<>();
  /** @deprecated tool ids in call order (one per call, not per attempt); use {@link #calls}. */
  @Deprecated
  public final List<String> toolCalls = new ArrayList<>();
  public List<AgentListener> listeners = List.of(); // set by RoutedGraph; tool-call hooks fire here
  /** The workflow's {@code context} window; set by RoutedGraph, bounds the folded transcript. */
  public ContextWindow contextWindow = ContextWindow.NONE;

  /** How retry back-off waits; the default sleeps the calling thread. Tests may inject a recorder. */
  public LongConsumer sleeper = AgentContext::sleep;
  /** Randomness for {@code jitter: true} policies only; unused (and irrelevant) when jitter is off. */
  public RandomGenerator random = null;
  /** The runtime's processing-time clock for workflow timers (spec section 8); null when the runtime has none. */
  public LogicalClock clock = null;
  /** The effect journal every tool and model call goes through; none by default. */
  public EffectJournal journal = EffectJournal.NONE;
  /** What distinguishes this workflow's calls from another's on a shared store: its agent id. */
  public String journalScope = "";
  /** A test's view of the turn. */
  public TurnProbe probe = TurnProbe.NONE;
  /** The metadata of the turn being handled, stamped on every event. */
  public Map<String, String> metadata = Map.of();
  /** The model's label on {@code llm_called}, where the workflow names one. */
  public String model = null;
  /** Who the turn runs as, when anyone vouched for it. A resume sets this back to {@code turn_received}'s. */
  public Principal principal = null;
  /** The runtime itself resumes this turn: its own timer, not a signal from outside. */
  public boolean runtimeSignal = false;
  /** The one check every tool call passes; set by RoutedGraph. */
  public ToolPolicy policy = ToolPolicy.DENY_ALL;
  /** What must not be recorded; set by RoutedGraph. */
  public Redactor redactor = Redactor.DEFAULT;
  /** This workflow's per-principal limits; set by RoutedGraph. */
  public Limits limits = Limits.NONE;
  /** The path this turn was routed to, once it has been. */
  public String path = null;

  private int nextToolIndex = 0;
  private int nextLlmIndex = 0;
  /** What an earlier run of this turn left on the log, by {@link #replayKey}. */
  private final Map<String, java.util.ArrayDeque<LogEvent>> kept = new LinkedHashMap<>();

  /**
   * Pre-spec constructor: a fresh random turn id, a turn-scoped in-memory log and default policies.
   * Callers that need idempotency or durable replay pass a shared {@link ConversationLog}.
   */
  public AgentContext(String conversationId, String userId, ConversationStore store,
                      KeyedStateStore state, ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    this(conversationId, null, userId, store, state, tools, retriever, new ConversationLog.InMemory(),
        Policies.DEFAULTS);
  }

  public AgentContext(String conversationId, String turnId, String userId, ConversationStore store,
                      KeyedStateStore state, ToolRegistry tools, Retrieval.TwoTierRetriever retriever,
                      ConversationLog log, Policies policies) {
    this.conversationId = conversationId;
    this.turnId = turnId == null ? new Event(conversationId, userId, "").turnId() : turnId;
    this.userId = userId;
    this.store = store;
    this.state = state;
    this.tools = tools;
    this.retriever = retriever;
    this.log = log == null ? new ConversationLog.InMemory() : log;
    this.policies = policies == null ? Policies.DEFAULTS : policies;
  }

  /** The {@code append} verb for this turn: records the event and remembers it as this turn's. */
  public LogEvent record(EventType type, Map<String, Object> raw) {
    Map<String, Object> payload = redactor.apply(raw);
    java.util.ArrayDeque<LogEvent> earlier = kept.get(replayKey(type.wire(), payload));
    if (earlier != null && !earlier.isEmpty()) {
      LogEvent e = earlier.poll();
      events.add(e);
      return e;
    }
    probe.beforeAppend(type.wire());
    long now = clock != null ? clock.nowMs() : System.currentTimeMillis();
    LogEvent e = log.append(conversationId, turnId, type, payload, now, metadata);
    events.add(e);
    probe.afterAppend(type.wire());
    return e;
  }

  /**
   * Starts a run of a turn that is already on the log (spec section 10). Appending is at-most-once
   * per {@code (turn_id, type, index, attempt)}, so what the log kept is reused and a log that lost
   * the turn gets every event again. Each {@code turn_resumed} is its own, so none is matched.
   */
  public void replayOver(List<LogEvent> earlier) {
    replayOver(earlier, -1);
  }

  /** As above for a run a new {@code turn_resumed} opens: every earlier wait has been answered. */
  void replayOverAndResume(List<LogEvent> earlier) {
    replayOver(earlier, Long.MAX_VALUE);
  }

  private void replayOver(List<LogEvent> earlier, long resumedAt) {
    long lastResume = resumedAt;
    for (LogEvent e : earlier) {
      if (e.is(EventType.TURN_RESUMED)) {
        lastResume = Math.max(lastResume, e.sequence());
      }
    }
    for (LogEvent e : earlier) {
      boolean answered = (e.is(EventType.TURN_SUSPENDED) || e.is(EventType.UNKNOWN_OUTCOME))
          && e.sequence() < lastResume;
      if (e.is(EventType.TURN_RESUMED)) {
        events.add(e);
      } else if (!answered) {
        kept.computeIfAbsent(replayKey(e.type(), e.payload()), k -> new java.util.ArrayDeque<>()).add(e);
      }
    }
  }

  private static String replayKey(String type, Map<String, Object> payload) {
    return type + "/" + payload.get("index") + "/" + payload.get("attempt");
  }

  /** The events a result reports: the turn's, from its last {@code turn_resumed}. */
  public List<LogEvent> segment() {
    List<LogEvent> ordered = new ArrayList<>(events);
    ordered.sort(java.util.Comparator.comparingLong(LogEvent::sequence));
    for (int i = ordered.size() - 1; i >= 0; i--) {
      if (ordered.get(i).is(EventType.TURN_RESUMED)) {
        return List.copyOf(ordered.subList(i, ordered.size()));
      }
    }
    return List.copyOf(ordered);
  }

  /** The folded state of the whole conversation, including this turn's events so far. */
  public ConversationState conversationState() {
    return log.state(conversationId, contextWindow);
  }

  /** Reserves the next {@code call_index} for this turn. */
  public int nextToolIndex() {
    return nextToolIndex++;
  }

  /** The key of the call this turn would make next. */
  EffectJournal.Key nextToolKey() {
    return new EffectJournal.Key(conversationId, turnId, EffectJournal.TOOL, nextToolIndex);
  }

  /** A re-run turn keeps counting: {@code (turn_id, index)} identifies one call across runs. */
  public void continueToolIndexAfter(List<ToolCall> earlier) {
    for (ToolCall c : earlier) {
      nextToolIndex = Math.max(nextToolIndex, c.index() + 1);
    }
  }

  /** As above from the log, which also shows a call that had an intent and no outcome. */
  public void continueIndicesAfter(List<LogEvent> earlier) {
    for (LogEvent e : earlier) {
      if (e.payload().get("index") instanceof Number n) {
        if (e.is(EventType.LLM_CALLED)) {
          nextLlmIndex = Math.max(nextLlmIndex, n.intValue() + 1);
        } else {
          nextToolIndex = Math.max(nextToolIndex, n.intValue() + 1);
        }
      }
    }
  }

  /** A resume does not fire timers again, so the calls they made keep their indices. */
  void keepIndicesOfTimers(List<LogEvent> earlier) {
    for (LogEvent e : earlier) {
      if (e.is(EventType.TURN_RECEIVED)) {
        return;
      }
      if (e.payload().get("index") instanceof Number n) {
        nextToolIndex = Math.max(nextToolIndex, n.intValue() + 1);
      }
    }
  }

  /**
   * One model call through the journal, by its index in the turn: a recorded answer is reused and
   * the model is not asked again, so a re-run takes the same path. {@code llm_called} follows every
   * answer. A crash during the call leaves nothing recorded, and the model is asked again.
   *
   * @param request what identifies the call across runs: the brain's instructions. The transcript is
   *     not part of it, since a log that kept part of the turn shows more of it than the first run saw
   * @throws EffectJournal.Divergence when the recorded answer was to a different request
   */
  @SuppressWarnings("unchecked")
  public org.jagentic.core.llm.ChatResult chat(String request, Supplier<org.jagentic.core.llm.ChatResult> call) {
    int index = nextLlmIndex++;
    EffectJournal.Key key = new EffectJournal.Key(conversationId, turnId, EffectJournal.LLM, index);
    String intent = EffectJournal.modelIntentOf(journalScope, redactor.apply(request));
    EffectJournal.Entry recorded = journal.get(key).orElse(null);
    if (recorded != null && !intent.equals(recorded.intent())) {
      throw new EffectJournal.Divergence(key, recorded.intent(), intent);
    }
    Map<String, Object> answer = recorded == null ? null : (Map<String, Object>) recorded.result();
    if (answer == null) {
      enforce(limits.admitModel(who(), now()));
      org.jagentic.core.llm.ChatResult fresh = callModel(call);
      answer = new LinkedHashMap<>();
      if (fresh.isToolCall()) {
        answer.put("tool", fresh.tool());
        answer.put("args", fresh.args());
      } else {
        answer.put("text", fresh.text());
      }
      journal.put(EffectJournal.Entry.pending(key, intent, ToolEffect.READ).succeeded(redactor.apply(answer)));
      limits.chargeModel(who(), now(), request.length() + String.valueOf(answer).length());
    }
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("index", index);
    if (model != null) {
      p.put("model", model);
    }
    p.putAll(answer);
    record(EventType.LLM_CALLED, p);
    if (answer.get("tool") != null) {
      Object args = answer.get("args");
      return org.jagentic.core.llm.ChatResult.toolCall(String.valueOf(answer.get("tool")),
          args instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of());
    }
    return org.jagentic.core.llm.ChatResult.text(answer.get("text") == null ? null : String.valueOf(answer.get("text")));
  }

  /**
   * One model call under {@code policies.retry}: a {@link TransientFailure} is retried, anything
   * else propagates. Only the call is retried, so tools the model has already called this turn are
   * not called again.
   */
  public <T> T callModel(Supplier<T> call) {
    Policies.RetryPolicy retry = policies.retry();
    for (int attempt = 1; ; attempt++) {
      try {
        return call.get();
      } catch (TransientFailure e) {
        for (AgentListener l : listeners) {
          l.onError("model", e, this);
        }
        if (attempt >= retry.attempts()) {
          throw e;
        }
        long delay = retry.delayBefore(attempt + 1, random);
        if (delay > 0) {
          sleeper.accept(delay);
        }
      }
    }
  }

  /** {@code invoke} with {@code tool_called}/{@code delegated} recorded on success. */
  public Object callTool(String toolId, Map<String, Object> params) {
    return invoke(toolId, params, null);
  }

  /** As above for what is not a brain: a timer, a CEP pattern or a saga step. */
  public Object callAs(ToolOrigin origin, String toolId, Map<String, Object> params) {
    return invoke(origin, toolId, params, null, policies.onToolError() == Policies.OnToolError.CONTINUE);
  }

  /**
   * Invokes a registered tool through the effect journal, under {@code policies.retry}. What is
   * already recorded for this {@code (turn_id, index)} is reused and nothing is called. Every attempt
   * appends its own event with an incrementing {@code attempt}; the recorded {@link ToolCall}s mirror
   * them. After the budget is spent the call either throws {@link ToolFailure} ({@code on_tool_error:
   * fail}) or returns {@code null} ({@code on_tool_error: continue}).
   *
   * @param successEvent the event type recorded on success, or {@code null} for the default
   *     ({@code delegated} for peer tools, {@code tool_called} otherwise)
   */
  public Object invoke(String toolId, Map<String, Object> params, EventType successEvent) {
    return invoke(ToolOrigin.BRAIN, toolId, params, successEvent,
        policies.onToolError() == Policies.OnToolError.CONTINUE);
  }

  /**
   * One saga undo, given what its step did. It always throws when its budget is spent, whatever
   * {@code on_tool_error} says: a failed compensation must never read as a compensated step.
   */
  public Object compensate(String toolId, Map<String, Object> forwardArgs, Object forwardResult) {
    Map<String, Object> forward = new LinkedHashMap<>();
    forward.put("forward_args", forwardArgs == null ? Map.of() : forwardArgs);
    forward.put("forward_result", forwardResult);
    return invoke(ToolOrigin.COMPENSATION, toolId, forward, EventType.COMPENSATION_STEP, false);
  }

  /** The call about to get the next index was allowed when it began, on the log or in the journal. */
  private boolean alreadyBegun() {
    java.util.ArrayDeque<LogEvent> begun = kept.get(EventType.TOOL_INTENT.wire() + "/" + nextToolIndex + "/null");
    return begun != null && !begun.isEmpty() || journal.get(nextToolKey()).isPresent();
  }

  /** Refuses what the policy refuses: on the log first, then the turn ends. */
  private void authorise(ToolOrigin origin, String toolId, ToolRegistry.Tool tool, Map<String, Object> args) {
    boolean outside = policy.outsideThisProcess();
    if (!outside || !alreadyBegun()) {
      decide(origin, toolId, tool, args);
    }
    enforce(limits.admitTool(who(), now(), toolId, limits.hasQuota(toolId) && alreadyBegun()));
  }

  private void decide(ToolOrigin origin, String toolId, ToolRegistry.Tool tool, Map<String, Object> args) {
    ToolPolicy.Decision decision = policy.decide(new ToolPolicy.Request(toolId, origin, path,
        tool == null ? null : tool.effect(), args, principal));
    if (!decision.allowed()) {
      refuse(origin, toolId, decision.reason());
    }
    if (decision.needsPerson()) {
      gate(origin, toolId);
    }
  }

  /** Who the limits count this turn against. */
  String who() {
    return Limits.who(principal, userId);
  }

  long now() {
    return clock != null ? clock.nowMs() : System.currentTimeMillis();
  }

  /** A limit that was exceeded is on the log, and the turn ends. */
  void enforce(Limits.Violation violation) {
    if (violation == null) {
      return;
    }
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("limit", violation.limit());
    p.put("who", who());
    p.putAll(violation.detail());
    record(EventType.LIMIT_EXCEEDED, p);
    throw new ToolRefused(violation.limit(), violation.limit() + " exceeded for " + who());
  }

  private void refuse(ToolOrigin origin, String toolId, String reason) {
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("tool", toolId);
    p.put("origin", origin.wire());
    p.put("path", path);
    p.put("reason", reason);
    if (principal != null) {
      p.put("principal", principal.toMap());
    }
    record(EventType.TOOL_REFUSED, p);
    throw new ToolRefused(toolId, reason);
  }

  /** A tool that needs a person waits for one. A timer or a pattern has nobody to ask. */
  private void gate(ToolOrigin origin, String toolId) {
    if (origin.unattended()) {
      refuse(origin, toolId, "tool " + toolId + " needs a person and " + origin.wire() + " has nobody to ask");
    }
    Boolean answer = approvalOf(toolId, nextToolIndex);
    if (answer == null) {
      throw new TurnSuspendedOnUnknown(toolId, nextToolIndex, ConversationState.TOOL_APPROVAL);
    }
    if (!answer) {
      refuse(origin, toolId, "an approver refused tool " + toolId);
    }
  }

  /** What the log says an approver answered about this call, the last answer standing. */
  private Boolean approvalOf(String toolId, int index) {
    Boolean answer = null;
    for (LogEvent e : log.events(conversationId)) {
      if (e.is(EventType.TURN_RESUMED) && turnId.equals(e.turnId())
          && e.payload().get("approval") instanceof Map<?, ?> a
          && toolId.equals(a.get("tool")) && a.get("index") instanceof Number n && n.intValue() == index) {
        answer = Boolean.TRUE.equals(a.get("approved"));
      }
    }
    return answer;
  }

  private Object invoke(ToolOrigin origin, String toolId, Map<String, Object> params,
                        EventType successEvent, boolean swallowFailure) {
    ToolRegistry.Tool tool = tools.get(toolId);
    Map<String, Object> args = params == null ? Map.of()
        : Collections.unmodifiableMap(new LinkedHashMap<>(params));
    authorise(origin, toolId, tool, args);
    if (tool == null) {
      throw new ToolRegistry.UnknownTool(toolId);
    }
    if (tool.inputSchema() != null) {
      List<String> problems = org.jagentic.core.structured.Structured.validate(args, tool.inputSchema());
      if (!problems.isEmpty()) {
        throw new ToolArgumentsRejected(toolId, problems);
      }
    }
    int index = nextToolIndex();
    toolCalls.add(toolId);
    for (AgentListener l : listeners) {
      l.onToolCallStart(toolId, this);
    }
    String key = EffectJournal.idempotencyKey(conversationId, turnId, index);
    ToolRegistry.CallIdentity identity = new ToolRegistry.CallIdentity(conversationId, turnId, index, key);
    Policies.RetryPolicy retry = policies.retry();
    EffectJournal.Key journalKey = new EffectJournal.Key(conversationId, turnId, EffectJournal.TOOL, index);
    Map<String, Object> recorded = redactor.apply(args);
    String intent = EffectJournal.intentOf(journalScope, toolId, recorded);
    java.util.ArrayDeque<LogEvent> begun = kept.get(EventType.TOOL_INTENT.wire() + "/" + index + "/null");
    if (begun != null && !begun.isEmpty()) {
      // The log says this call was begun, so a journal that lost it must not read as a first call.
      journal.putIfAbsent(EffectJournal.Entry.pending(journalKey, intent, tool.effect()));
    }
    EffectJournal.Entry entry = journal.recordOrGet(
        journalKey, intent, tool.effect(), new EffectJournal.Call() {
          @Override
          public void onIntent(EffectJournal.Entry recorded) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("tool", toolId);
            p.put("index", index);
            p.put("args", args);
            p.put("effect", tool.effect());
            p.put("idempotency_key", key);
            if (principal != null) {
              p.put("principal", principal.toMap());
            }
            record(EventType.TOOL_INTENT, p);
          }

          @Override
          public void onFailedAttempt(EffectJournal.Attempt failed) {
            calls.add(ToolCall.failed(toolId, index, recorded, failed.error(), failed.attempt()));
            record(EventType.TOOL_FAILED, payload(toolId, index, failed.attempt(), args, null, failed.error()));
          }

          @Override
          public int budget() {
            return retry.attempts();
          }

          @Override
          public Object attempt(int attempt) {
            long delay = attempt > 1 ? retry.delayBefore(attempt, random) : 0;
            if (delay > 0) {
              sleeper.accept(delay);
            }
            probe.beforeCall(toolId);
            try {
              return redactor.apply(tool.execute(args, identity));
            } catch (RuntimeException e) {
              for (AgentListener l : listeners) {
                l.onError("tool:" + toolId, e, AgentContext.this);
              }
              throw e;
            }
          }

          @Override
          public String recorded(String error) {
            return redactor.apply(error);
          }

          @Override
          public void afterAttempt() {
            probe.duringCall(toolId);
          }

          @Override
          public void afterRecord() {
            probe.afterCall(toolId);
          }
        });
    switch (entry.state()) {
      case SUCCEEDED -> {
        int attempt = entry.attempts().size() + 1;
        calls.add(ToolCall.succeeded(toolId, index, recorded, entry.result(), attempt));
        EventType type = successEvent != null ? successEvent
            : tool.peer() ? EventType.DELEGATED : EventType.TOOL_CALLED;
        record(type, payload(toolId, index, attempt, args, entry.result(), null));
        for (AgentListener l : listeners) {
          l.onToolCallEnd(toolId, entry.result(), this);
        }
        return entry.result();
      }
      case UNKNOWN -> {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("tool", toolId);
        p.put("index", index);
        p.put("args", args);
        p.put("idempotency_key", key);
        p.put("reason", "no recorded result");
        record(EventType.UNKNOWN_OUTCOME, p);
        throw new TurnSuspendedOnUnknown(toolId, index);
      }
      default -> {
        if (swallowFailure) {
          return null;
        }
        throw new ToolFailure(toolId, index, Math.max(1, entry.attempts().size()),
            String.valueOf(entry.error()), null);
      }
    }
  }

  /** The {@code retrieve} verb: top-{@code k} passages, recorded as a {@code retrieved} event. */
  public List<Retrieval.Scored> retrieve(float[] query, int k) {
    if (retriever == null) {
      return List.of();
    }
    List<Retrieval.Scored> hits = retriever.retrieve(query, k);
    List<String> ids = new ArrayList<>(hits.size());
    for (Retrieval.Scored s : hits) {
      ids.add(s.id());
    }
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("ids", ids);
    record(EventType.RETRIEVED, p);
    return hits;
  }

  private static Map<String, Object> payload(String tool, int index, int attempt,
                                             Map<String, Object> args, Object result, String error) {
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("tool", tool);
    p.put("index", index);
    p.put("attempt", attempt);
    p.put("args", args);
    if (error != null) {
      p.put("error", error);
    } else {
      p.put("result", result);
    }
    return p;
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}

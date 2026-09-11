package org.jagentic.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
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
  public final String userId;
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

  /** How retry back-off waits; the default sleeps the calling thread. Tests may inject a recorder. */
  public LongConsumer sleeper = AgentContext::sleep;
  /** Randomness for {@code jitter: true} policies only; unused (and irrelevant) when jitter is off. */
  public RandomGenerator random = null;

  private int nextToolIndex = 0;

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
  public LogEvent record(EventType type, Map<String, Object> payload) {
    LogEvent e = log.append(conversationId, turnId, type, payload);
    events.add(e);
    return e;
  }

  /** The folded state of the whole conversation, including this turn's events so far. */
  public ConversationState conversationState() {
    return log.state(conversationId);
  }

  /** Reserves the next {@code call_index} for this turn. */
  public int nextToolIndex() {
    return nextToolIndex++;
  }

  /** {@code invoke} with {@code tool_called}/{@code delegated} recorded on success. */
  public Object callTool(String toolId, Map<String, Object> params) {
    return invoke(toolId, params, null);
  }

  /**
   * Invokes a registered tool under {@code policies.retry}. Every attempt appends its own event
   * with an incrementing {@code attempt}; the recorded {@link ToolCall}s mirror them. After the
   * budget is spent the call either throws {@link ToolFailure} ({@code on_tool_error: fail}) or
   * returns {@code null} ({@code on_tool_error: continue}).
   *
   * @param successEvent the event type recorded on success, or {@code null} for the default
   *     ({@code delegated} for peer tools, {@code tool_called} otherwise)
   */
  public Object invoke(String toolId, Map<String, Object> params, EventType successEvent) {
    ToolRegistry.Tool tool = tools.get(toolId);
    if (tool == null) {
      throw new ToolRegistry.UnknownTool(toolId);
    }
    Map<String, Object> args = params == null ? Map.of()
        : Collections.unmodifiableMap(new LinkedHashMap<>(params));
    int index = nextToolIndex();
    toolCalls.add(toolId);
    Policies.RetryPolicy retry = policies.retry();
    int budget = retry.attempts();
    for (AgentListener l : listeners) {
      l.onToolCallStart(toolId, this);
    }
    for (int attempt = 1; attempt <= budget; attempt++) {
      Object result;
      try {
        result = tool.execute(args);
      } catch (RuntimeException e) {
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        calls.add(ToolCall.failed(toolId, index, args, message, attempt));
        record(EventType.TOOL_FAILED, payload(toolId, index, attempt, args, null, message));
        for (AgentListener l : listeners) {
          l.onError("tool:" + toolId, e, this);
        }
        if (attempt == budget) {
          if (policies.onToolError() == Policies.OnToolError.CONTINUE) {
            return null;
          }
          throw new ToolFailure(toolId, index, attempt, message, e);
        }
        long delay = retry.delayBefore(attempt + 1, random);
        if (delay > 0) {
          sleeper.accept(delay);
        }
        continue;
      }
      calls.add(ToolCall.succeeded(toolId, index, args, result, attempt));
      EventType type = successEvent != null ? successEvent
          : tool.peer() ? EventType.DELEGATED : EventType.TOOL_CALLED;
      record(type, payload(toolId, index, attempt, args, result, null));
      for (AgentListener l : listeners) {
        l.onToolCallEnd(toolId, result, this);
      }
      return result;
    }
    throw new IllegalStateException("retry loop exited without a result for " + toolId);
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

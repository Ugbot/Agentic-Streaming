package org.jagentic.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The outcome of one turn, in the shape of {@code spec/v1/result.schema.json} ({@link #toMap()}).
 *
 * <p>Compatibility: the pre-spec fields {@code reply}, {@code path}, {@code ok} and the
 * {@code List<String> toolCalls} view are kept and still mutable, but they are derived from the
 * spec fields at construction time and are deprecated. {@code ok} means {@code status ==
 * COMPLETED}; a caller that assigns {@code ok} or {@code reply} directly changes only the legacy
 * view, never {@link #status} or the recorded log. New code should read {@link #status}, {@link
 * #calls}, {@link #events} and {@link #error}.</p>
 */
public final class TurnResult implements Serializable {
  private static final long serialVersionUID = 1L;

  public final String conversationId;
  public final String turnId;
  public final TurnStatus status;
  public final TurnError error;
  /** Structured tool calls in execution order (one entry per attempt). */
  public final List<ToolCall> calls;
  /** The events this turn appended (empty for a {@code duplicate}). */
  public final List<LogEvent> events;
  /** The reduced conversation state after this turn. */
  public final Map<String, Object> state;

  /** @deprecated read {@link #status}/{@link #reply()}; kept mutable for pre-spec callers. */
  @Deprecated
  public String reply;
  /** @deprecated read {@link #path()}; kept mutable for pre-spec callers. */
  @Deprecated
  public String path;
  /** @deprecated {@code status == COMPLETED}; kept mutable for pre-spec callers. */
  @Deprecated
  public boolean ok;
  /** @deprecated tool ids only; use {@link #calls} for arguments, results and attempts. */
  @Deprecated
  public final List<String> toolCalls;

  /**
   * @deprecated pre-spec constructor: records a {@code completed} turn with a fresh turn id, no
   *     events and no state. Use {@link #TurnResult(String, String, TurnStatus, String, String,
   *     TurnError, List, List, Map)}.
   */
  @Deprecated
  public TurnResult(String conversationId, String reply, List<String> toolCalls) {
    this.conversationId = conversationId;
    this.turnId = UUID.randomUUID().toString();
    this.status = TurnStatus.COMPLETED;
    this.error = null;
    this.calls = List.of();
    this.events = List.of();
    this.state = Map.of();
    this.reply = reply;
    this.path = null;
    this.ok = true;
    this.toolCalls = toolCalls == null ? new ArrayList<>() : new ArrayList<>(toolCalls);
  }

  public TurnResult(String conversationId, String turnId, TurnStatus status, String path,
                    String reply, TurnError error, List<ToolCall> calls, List<LogEvent> events,
                    Map<String, Object> state) {
    if (status == null) {
      throw new IllegalArgumentException("TurnResult requires a status");
    }
    this.conversationId = conversationId;
    this.turnId = turnId;
    this.status = status;
    this.path = path;
    this.reply = reply;
    this.error = error;
    this.calls = calls == null ? List.of() : List.copyOf(calls);
    this.events = events == null ? List.of() : List.copyOf(events);
    this.state = state == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(state));
    this.ok = status == TurnStatus.COMPLETED;
    Map<Integer, String> byIndex = new LinkedHashMap<>();
    for (ToolCall c : this.calls) {
      byIndex.putIfAbsent(c.index(), c.tool());
    }
    this.toolCalls = new ArrayList<>(byIndex.values());
  }

  public String reply() {
    return reply;
  }

  public String path() {
    return path;
  }

  /** True when the turn waits for a person to say what happened to a call (spec section 10). */
  public boolean awaitsAPerson() {
    if (status != TurnStatus.SUSPENDED || events.isEmpty()) {
      return false;
    }
    LogEvent last = events.get(events.size() - 1);
    return last.is(EventType.TURN_SUSPENDED)
        && (ConversationState.UNKNOWN_OUTCOME.equals(last.payload().get("until"))
            || ConversationState.TOOL_APPROVAL.equals(last.payload().get("until")));
  }

  /** The same outcome re-reported for a redelivered {@code turn_id}: status {@code duplicate}, no events. */
  public TurnResult asDuplicate(Map<String, Object> currentState) {
    return new TurnResult(conversationId, turnId, TurnStatus.DUPLICATE, path, reply, error, calls,
        List.of(), currentState);
  }

  /** The normalized result document of {@code spec/v1/result.schema.json}. */
  public Map<String, Object> toMap() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("conversation_id", conversationId);
    m.put("turn_id", turnId);
    m.put("status", status.wire());
    m.put("path", path);
    m.put("reply", reply);
    m.put("state", state);
    List<Map<String, Object>> tc = new ArrayList<>(calls.size());
    for (ToolCall c : calls) {
      tc.add(c.toMap());
    }
    m.put("tool_calls", tc);
    List<Map<String, Object>> ev = new ArrayList<>(events.size());
    for (LogEvent e : events) {
      ev.add(e.toMap());
    }
    m.put("events", ev);
    m.put("error", error == null ? null : error.toMap());
    return m;
  }

  @Override
  public String toString() {
    return "TurnResult" + toMap();
  }

  /** A result back from its normalized document. */
  public static TurnResult fromMap(Map<String, Object> m) {
    String cid = (String) m.get("conversation_id");
    String turnId = (String) m.get("turn_id");
    TurnStatus status = TurnStatus.parse((String) m.get("status"));
    String path = (String) m.get("path");
    String reply = (String) m.get("reply");
    Map<String, Object> state =
        m.get("state") instanceof Map<?, ?> s
            ? new LinkedHashMap<>((Map<String, Object>) s)
            : Map.of();
    List<ToolCall> calls = new ArrayList<>();
    if (m.get("tool_calls") instanceof List<?> tc) {
      for (Object o : tc) {
        calls.add(toolCall((Map<String, Object>) o));
      }
    }
    List<LogEvent> events = new ArrayList<>();
    if (m.get("events") instanceof List<?> ev) {
      for (Object o : ev) {
        Map<String, Object> e = (Map<String, Object>) o;
        Map<String, Object> payload =
            e.get("payload") instanceof Map<?, ?> p ? (Map<String, Object>) p : Map.of();
        events.add(
            new LogEvent(
                cid,
                ((Number) e.get("sequence")).longValue(),
                turnId,
                (String) e.get("type"),
                payload));
      }
    }
    TurnError error = null;
    if (m.get("error") instanceof Map<?, ?> em) {
      Map<String, Object> e = (Map<String, Object>) em;
      error =
          new TurnError(
              TurnError.ErrorClass.parse((String) e.get("class")), (String) e.get("message"));
    }
    return new TurnResult(cid, turnId, status, path, reply, error, calls, events, state);
  }

  @SuppressWarnings("unchecked")
  private static ToolCall toolCall(Map<String, Object> c) {
    Map<String, Object> args =
        c.get("args") instanceof Map<?, ?> a ? (Map<String, Object>) a : Map.of();
    int index = ((Number) c.get("index")).intValue();
    int attempt = ((Number) c.get("attempt")).intValue();
    String tool = (String) c.get("tool");
    if (c.get("error") != null) {
      return ToolCall.failed(tool, index, args, String.valueOf(c.get("error")), attempt);
    }
    return ToolCall.succeeded(tool, index, args, c.get("result"), attempt);
  }
}

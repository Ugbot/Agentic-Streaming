package org.jagentic.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The pure fold ({@code reduce}) of a conversation log. Everything the runtime needs to know about a
 * conversation, its reduced state (as in {@code result.state}), the transcript, which turns have
 * been applied and with what result, and which turns are suspended, is derived here and nowhere
 * else. The fold is total: unknown event types are ignored, never rejected.
 */
public final class ConversationState implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The recorded outcome of one applied turn, rebuilt from its events. */
  public record TurnRecord(String turnId, String text, TurnStatus status, String path, String reply,
                           TurnError error, List<ToolCall> calls, List<LogEvent> events)
      implements Serializable {
    public boolean terminal() {
      return status != null && status != TurnStatus.SUSPENDED;
    }
  }

  /** A turn parked by {@code turn_suspended} and not yet resumed. */
  public record Suspended(String turnId, String path, String text, String until)
      implements Serializable {}

  private final long turnCount;
  private final long transcriptLength;
  private final List<String> lastRetrievedIds;
  private final List<ChatMessage> transcript;
  private final Map<String, TurnRecord> turns;
  private final Map<String, Suspended> suspended;
  private final long nextSequence;

  private ConversationState(long turnCount, long transcriptLength, List<String> lastRetrievedIds,
                            List<ChatMessage> transcript, Map<String, TurnRecord> turns,
                            Map<String, Suspended> suspended, long nextSequence) {
    this.turnCount = turnCount;
    this.transcriptLength = transcriptLength;
    this.lastRetrievedIds = lastRetrievedIds;
    this.transcript = transcript;
    this.turns = turns;
    this.suspended = suspended;
    this.nextSequence = nextSequence;
  }

  public static ConversationState empty() {
    return fold(List.of());
  }

  public static ConversationState fold(List<LogEvent> log) {
    long turnCount = 0;
    long transcriptLength = 0;
    List<String> lastRetrieved = null;
    List<ChatMessage> transcript = new ArrayList<>();
    Map<String, Builder> builders = new LinkedHashMap<>();
    Map<String, Suspended> suspended = new LinkedHashMap<>();
    long expected = 0;
    for (LogEvent e : log) {
      if (e.sequence() != expected) {
        throw new IllegalStateException("conversation " + e.conversationId()
            + " log is not dense: expected sequence " + expected + " but found " + e.sequence());
      }
      expected++;
      Builder b = e.turnId() == null ? null
          : builders.computeIfAbsent(e.turnId(), Builder::new);
      if (b != null) {
        b.events.add(e);
      }
      EventType type = e.eventType().orElse(null);
      if (type == null) {
        continue;
      }
      Map<String, Object> p = e.payload();
      switch (type) {
        case TURN_RECEIVED -> {
          turnCount++;
          if (b != null) {
            b.text = str(p.get("text"));
          }
        }
        case MEMORY_WRITTEN -> {
          Object msgs = p.get("messages");
          if (msgs instanceof List<?> list) {
            transcriptLength += list.size();
            for (Object o : list) {
              if (o instanceof Map<?, ?> m) {
                transcript.add(new ChatMessage(str(m.get("role")), str(m.get("text")), null, null));
              }
            }
          }
        }
        case RETRIEVED -> {
          Object ids = p.get("ids");
          if (ids instanceof List<?> list) {
            List<String> copy = new ArrayList<>(list.size());
            for (Object o : list) {
              copy.add(String.valueOf(o));
            }
            lastRetrieved = Collections.unmodifiableList(copy);
          }
        }
        case ROUTED -> {
          if (b != null) {
            b.path = str(p.get("path"));
          }
        }
        case GUARDRAIL_REJECTED -> {
          if (b != null) {
            b.status = TurnStatus.REJECTED;
            b.reply = str(p.get("reply"));
            b.error = new TurnError(TurnError.ErrorClass.GUARDRAIL, str(p.get("reason")));
          }
        }
        case REPLY_DRAFTED -> {
          if (b != null) {
            b.reply = str(p.get("reply"));
          }
        }
        case VERIFICATION_FAILED -> {
          if (b != null) {
            b.error = new TurnError(TurnError.ErrorClass.VERIFICATION, "verifier rejected the reply");
          }
        }
        case TOOL_CALLED, DELEGATED, COMPENSATION_STEP -> {
          if (b != null) {
            b.calls.add(ToolCall.succeeded(str(p.get("tool")), intOf(p.get("index")), args(p),
                p.get("result"), Math.max(1, intOf(p.get("attempt")))));
          }
        }
        case TOOL_FAILED -> {
          if (b != null) {
            b.calls.add(ToolCall.failed(str(p.get("tool")), intOf(p.get("index")), args(p),
                str(p.get("error")), Math.max(1, intOf(p.get("attempt")))));
          }
        }
        case TURN_COMPLETED -> {
          if (b != null) {
            b.status = TurnStatus.COMPLETED;
            b.reply = str(p.get("reply"));
            b.error = null;
          }
        }
        case TURN_FAILED -> {
          if (b != null) {
            String declared = str(p.get("status"));
            b.status = declared == null ? TurnStatus.FAILED : TurnStatus.parse(declared);
            if (p.containsKey("error_class")) {
              b.error = new TurnError(TurnError.ErrorClass.parse(str(p.get("error_class"))),
                  str(p.get("reason")));
            } else if (b.error == null) {
              b.error = new TurnError(TurnError.ErrorClass.TOOL, str(p.get("reason")));
            }
          }
        }
        case TURN_SUSPENDED -> {
          String tid = str(p.get("turn_id"));
          suspended.put(tid, new Suspended(tid, str(p.get("path")), str(p.get("text")),
              str(p.get("until"))));
          if (b != null) {
            b.status = TurnStatus.SUSPENDED;
          }
        }
        case TURN_RESUMED -> {
          suspended.remove(str(p.get("turn_id")));
          if (b != null) {
            b.status = null;
          }
        }
        default -> {
          // brain_started, timer_*, compensation_started/completed carry no state.
        }
      }
    }
    Map<String, TurnRecord> turns = new LinkedHashMap<>();
    for (Builder b : builders.values()) {
      turns.put(b.turnId, b.build());
    }
    return new ConversationState(turnCount, transcriptLength, lastRetrieved,
        Collections.unmodifiableList(transcript), Collections.unmodifiableMap(turns),
        Collections.unmodifiableMap(suspended), expected);
  }

  /** The {@code state} object of a normalized result (spec reference {@code reduce_state}). */
  public Map<String, Object> reduced() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("turn_count", turnCount);
    m.put("transcript_length", transcriptLength);
    if (lastRetrievedIds != null) {
      m.put("last_retrieved_ids", lastRetrievedIds);
    }
    return m;
  }

  public long turnCount() {
    return turnCount;
  }

  public long transcriptLength() {
    return transcriptLength;
  }

  public List<String> lastRetrievedIds() {
    return lastRetrievedIds == null ? List.of() : lastRetrievedIds;
  }

  public List<ChatMessage> transcript() {
    return transcript;
  }

  /** Applied turns keyed by {@code turn_id}, in first-seen order. */
  public Map<String, TurnRecord> turns() {
    return turns;
  }

  public TurnRecord turn(String turnId) {
    return turns.get(turnId);
  }

  public Map<String, Suspended> suspended() {
    return suspended;
  }

  /** The sequence the next appended event will receive. */
  public long nextSequence() {
    return nextSequence;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ConversationState s && turnCount == s.turnCount
        && transcriptLength == s.transcriptLength
        && Objects.equals(lastRetrievedIds, s.lastRetrievedIds)
        && transcript.equals(s.transcript) && turns.equals(s.turns)
        && suspended.equals(s.suspended) && nextSequence == s.nextSequence;
  }

  @Override
  public int hashCode() {
    return Objects.hash(turnCount, transcriptLength, lastRetrievedIds, transcript, turns, suspended,
        nextSequence);
  }

  private static String str(Object o) {
    return o == null ? null : String.valueOf(o);
  }

  private static int intOf(Object o) {
    return o instanceof Number n ? n.intValue() : 0;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> args(Map<String, Object> p) {
    Object a = p.get("args");
    return a instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
  }

  private static final class Builder {
    final String turnId;
    String text;
    TurnStatus status;
    String path;
    String reply;
    TurnError error;
    final List<ToolCall> calls = new ArrayList<>();
    final List<LogEvent> events = new ArrayList<>();

    Builder(String turnId) {
      this.turnId = turnId;
    }

    TurnRecord build() {
      return new TurnRecord(turnId, text, status, path, reply, error,
          Collections.unmodifiableList(new ArrayList<>(calls)),
          Collections.unmodifiableList(new ArrayList<>(events)));
    }
  }
}

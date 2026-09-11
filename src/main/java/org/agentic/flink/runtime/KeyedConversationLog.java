package org.agentic.flink.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.jagentic.core.ConversationLog;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;

/**
 * The spec's per-conversation event log stored in Flink keyed state.
 *
 * <p>Every instance is scoped to the key that is current when the operator hands it to the graph
 * (Flink scopes {@link ListState}/{@link ValueState} to the current key), so a single
 * {@link org.jagentic.core.RoutedGraph#handle} call only ever sees and appends to one
 * conversation's history. Sequences are dense and assigned from a checkpointed counter, so the
 * fold ({@link org.jagentic.core.ConversationState#fold}) over the restored log is identical to
 * the fold before the failure.
 */
final class KeyedConversationLog implements ConversationLog {
  private final String conversationId;
  private final ListState<LogEvent> events;
  private final ValueState<Long> nextSequence;

  KeyedConversationLog(String conversationId, ListState<LogEvent> events, ValueState<Long> nextSequence) {
    this.conversationId = Objects.requireNonNull(conversationId, "conversationId");
    this.events = Objects.requireNonNull(events, "events");
    this.nextSequence = Objects.requireNonNull(nextSequence, "nextSequence");
  }

  @Override
  public LogEvent append(String cid, String turnId, EventType type, Map<String, Object> payload) {
    requireCurrentKey(cid);
    try {
      Long seq = nextSequence.value();
      long next = seq == null ? 0L : seq;
      LogEvent e = new LogEvent(cid, next, turnId, type.wire(), payload);
      events.add(e);
      nextSequence.update(next + 1);
      return e;
    } catch (Exception ex) {
      throw new IllegalStateException("failed to append to keyed conversation log for " + cid, ex);
    }
  }

  @Override
  public List<LogEvent> events(String cid) {
    requireCurrentKey(cid);
    try {
      List<LogEvent> out = new ArrayList<>();
      Iterable<LogEvent> it = events.get();
      if (it != null) {
        for (LogEvent e : it) {
          out.add(e);
        }
      }
      return out;
    } catch (Exception ex) {
      throw new IllegalStateException("failed to read keyed conversation log for " + cid, ex);
    }
  }

  /** Keyed state only ever exposes the current key's conversation. */
  @Override
  public Set<String> conversations() {
    return Set.of(conversationId);
  }

  private void requireCurrentKey(String cid) {
    if (!conversationId.equals(cid)) {
      throw new IllegalStateException("keyed conversation log for " + conversationId
          + " asked to operate on conversation " + cid + "; Flink keying guarantees single-writer per key");
    }
  }
}

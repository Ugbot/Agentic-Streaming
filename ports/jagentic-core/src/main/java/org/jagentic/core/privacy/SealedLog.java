package org.jagentic.core.privacy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.EventType;
import org.jagentic.core.Limits;
import org.jagentic.core.LogEvent;
import org.jagentic.core.Principal;

/** A log whose store holds only sealed payloads. Whoever reads through this sees them open. */
public final class SealedLog implements ConversationLog {

  private final ConversationLog inner;
  private final Sealer sealer;

  public SealedLog(ConversationLog inner, KeyVault vault) {
    this.inner = inner;
    this.sealer = new Sealer(vault);
  }

  @Override
  public LogEvent append(String conversationId, String turnId, EventType type, Map<String, Object> payload,
                         long timestamp, Map<String, String> metadata) {
    String owner = type == EventType.TURN_RECEIVED ? ownerOf(payload) : null;
    LogEvent stored = inner.append(conversationId, turnId, type, sealer.seal(conversationId, owner, payload),
        timestamp, metadata);
    return open(stored);
  }

  private static String ownerOf(Map<String, Object> received) {
    Object user = received.get("user_id");
    return Limits.who(Principal.fromMap(received.get("principal")), user == null ? null : String.valueOf(user));
  }

  private LogEvent open(LogEvent e) {
    return new LogEvent(e.conversationId(), e.sequence(), e.turnId(), e.type(),
        sealer.open(e.conversationId(), e.payload()), e.timestamp(), e.metadata());
  }

  @Override
  public List<LogEvent> events(String conversationId) {
    return inner.events(conversationId).stream().map(this::open).toList();
  }

  @Override
  public Set<String> conversations() {
    return inner.conversations();
  }
}

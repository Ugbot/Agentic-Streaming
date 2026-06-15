package org.jagentic.pekko.entity;

import java.util.ArrayList;
import java.util.List;

import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.pekko.serialization.CborSerializable;

/** The exact, ordered set of store mutations one turn produced — the payload of the single
 * {@code TurnCommitted} event persisted per turn. Replaying these onto the committed view
 * reconstructs conversation state on recovery <b>without re-running the (LLM-calling) pipeline</b>.
 *
 * <p>Ops are kept in execution order so replay is faithful (associate → append user →
 * set attributes → append assistant → …).</p>
 */
public record TurnMutations(List<Op> ops) implements CborSerializable {

  public sealed interface Op extends CborSerializable permits Append, AttrPut, Associate, KeyedPut {}

  public record Append(ChatMessage message) implements Op {}

  public record AttrPut(String key, String value) implements Op {}

  public record Associate(String userId) implements Op {}

  /** Keyed scalar state. Stored as String — the core only keeps scalars here; richer values
   * on the event-sourced profiles are a documented limitation. */
  public record KeyedPut(String name, String value) implements Op {}

  /** Replay the recorded mutations onto the durable views (used by the event handler). */
  public void applyTo(String conversationId, ConversationStore store, KeyedStateStore keyed) {
    for (Op op : ops) {
      if (op instanceof Append a) {
        store.append(conversationId, a.message());
      } else if (op instanceof AttrPut p) {
        store.putAttribute(conversationId, p.key(), p.value());
      } else if (op instanceof Associate as) {
        store.associateUser(conversationId, as.userId());
      } else if (op instanceof KeyedPut k) {
        keyed.put(conversationId, k.name(), k.value());
      }
    }
  }

  /** Mutable accumulator the recording stores write into during a turn. */
  public static final class Recorder {
    private final List<Op> ops = new ArrayList<>();

    public void append(ChatMessage m) {
      ops.add(new Append(m));
    }

    public void attr(String key, String value) {
      ops.add(new AttrPut(key, value));
    }

    public void associate(String userId) {
      ops.add(new Associate(userId));
    }

    public void keyed(String name, String value) {
      ops.add(new KeyedPut(name, value));
    }

    public TurnMutations build() {
      return new TurnMutations(List.copyOf(ops));
    }
  }
}

package org.jagentic.ports.pulsar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;

/**
 * A {@link ConversationStore} backed by Pulsar Functions' built-in <b>state store</b>
 * (the {@link StateBytes} seam onto {@code Context.getState/putState}). This is the
 * port's realization of <b>C1 (durable keyed state)</b>: Pulsar persists function
 * state in BookKeeper, replicated and recovered on instance restart/rebalance — the
 * analogue of Flink's checkpointed keyed {@code ValueState}, supplied natively by the
 * runtime rather than an external database.
 *
 * <p>Each conversation's whole envelope (bounded transcript + scalar attributes +
 * owner) is serialized under the key {@code conv/<conversationId>}; a reverse index
 * for {@link #conversationsForUser} lives under {@code user/<userId>}. Because a
 * Pulsar Function consuming with a {@code Key_Shared} subscription routes one message
 * key to one instance in order, all writes for a given conversation are
 * single-writer (C2), so read-modify-write of the envelope is safe.
 */
public final class PulsarStateConversationStore implements ConversationStore {

  /** Serializable envelope persisted as one state value per conversation. */
  private static final class Envelope implements Serializable {
    private static final long serialVersionUID = 1L;
    final ArrayList<String[]> messages = new ArrayList<>(); // [role, content, toolName, toolCallId]
    final LinkedHashMap<String, String> attrs = new LinkedHashMap<>();
    String owner;
  }

  private final StateBytes state;
  private final int maxMessages;

  public PulsarStateConversationStore(StateBytes state) {
    this(state, 200);
  }

  public PulsarStateConversationStore(StateBytes state, int maxMessages) {
    this.state = state;
    this.maxMessages = Math.max(1, maxMessages);
  }

  private static String convKey(String cid) {
    return "conv/" + cid;
  }

  private static String userKey(String userId) {
    return "user/" + userId;
  }

  private Envelope load(String cid) {
    byte[] raw = state.get(convKey(cid));
    if (raw == null) {
      return new Envelope();
    }
    return deserialize(raw, Envelope.class);
  }

  private void save(String cid, Envelope env) {
    state.put(convKey(cid), serialize(env));
  }

  @Override
  public void append(String cid, ChatMessage m) {
    Envelope env = load(cid);
    env.messages.add(new String[] {m.role(), m.content(), m.toolName(), m.toolCallId()});
    while (env.messages.size() > maxMessages) {
      env.messages.remove(0);
    }
    save(cid, env);
  }

  @Override
  public List<ChatMessage> history(String cid) {
    Envelope env = load(cid);
    List<ChatMessage> out = new ArrayList<>(env.messages.size());
    for (String[] m : env.messages) {
      out.add(new ChatMessage(m[0], m[1], m[2], m[3]));
    }
    return out;
  }

  @Override
  public int messageCount(String cid) {
    return load(cid).messages.size();
  }

  @Override
  public void putAttribute(String cid, String key, String value) {
    Envelope env = load(cid);
    env.attrs.put(key, value);
    save(cid, env);
  }

  @Override
  public Optional<String> getAttribute(String cid, String key) {
    return Optional.ofNullable(load(cid).attrs.get(key));
  }

  @Override
  public Map<String, String> attributes(String cid) {
    return new LinkedHashMap<>(load(cid).attrs);
  }

  @Override
  public void associateUser(String cid, String userId) {
    Envelope env = load(cid);
    if (env.owner != null && !env.owner.equals(userId)) {
      removeFromUserIndex(env.owner, cid);
    }
    env.owner = userId;
    save(cid, env);
    addToUserIndex(userId, cid);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> conversationsForUser(String userId) {
    byte[] raw = state.get(userKey(userId));
    if (raw == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(deserialize(raw, LinkedHashSet.class));
  }

  @Override
  public void clear(String cid) {
    Envelope env = load(cid);
    if (env.owner != null) {
      removeFromUserIndex(env.owner, cid);
    }
    state.delete(convKey(cid));
  }

  @SuppressWarnings("unchecked")
  private void addToUserIndex(String userId, String cid) {
    byte[] raw = state.get(userKey(userId));
    Set<String> ids = raw == null ? new LinkedHashSet<>() : deserialize(raw, LinkedHashSet.class);
    if (ids.add(cid)) {
      state.put(userKey(userId), serialize((Serializable) ids));
    }
  }

  @SuppressWarnings("unchecked")
  private void removeFromUserIndex(String userId, String cid) {
    byte[] raw = state.get(userKey(userId));
    if (raw == null) {
      return;
    }
    Set<String> ids = deserialize(raw, LinkedHashSet.class);
    if (ids.remove(cid)) {
      state.put(userKey(userId), serialize((Serializable) ids));
    }
  }

  // --- Java-serialization helpers (dependency-free; the envelope is small/bounded) ---

  private static byte[] serialize(Serializable o) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(o);
      oos.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static <T> T deserialize(byte[] raw, Class<T> type) {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(raw))) {
      return type.cast(ois.readObject());
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Failed to deserialize state", e);
    }
  }
}

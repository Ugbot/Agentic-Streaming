package org.jagentic.ports.pulsar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Optional;

import org.jagentic.core.KeyedStateStore;

/**
 * A {@link KeyedStateStore} backed by Pulsar Functions' state API (via {@link
 * StateBytes}) — the portable analogue of Flink keyed {@code ValueState}, persisted
 * durably by the runtime (C1). Each {@code (key, name)} scalar slot is stored under
 * {@code ks/<key>/<name>}. Values must be {@link Serializable} (the banking graph
 * writes none here; the slot exists so a real brain can stash per-conversation scratch
 * state that survives restart, exactly as a Flink operator would).
 */
public final class PulsarStateKeyedStore implements KeyedStateStore {

  private final StateBytes state;

  public PulsarStateKeyedStore(StateBytes state) {
    this.state = state;
  }

  private static String slot(String key, String name) {
    return "ks/" + key + "/" + name;
  }

  @Override
  public Optional<Object> get(String key, String name) {
    byte[] raw = state.get(slot(key, name));
    return raw == null ? Optional.empty() : Optional.of(deserialize(raw));
  }

  @Override
  public void put(String key, String name, Object value) {
    if (value == null) {
      state.delete(slot(key, name));
      return;
    }
    if (!(value instanceof Serializable)) {
      throw new IllegalArgumentException(
          "PulsarStateKeyedStore requires Serializable values; got " + value.getClass());
    }
    state.put(slot(key, name), serialize((Serializable) value));
  }

  @Override
  public void clear(String key) {
    // Pulsar's state API has no prefix scan; a real port tracks this key's slot names.
    // The banking graph writes no keyed slots, so there is nothing to sweep here.
  }

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

  private static Object deserialize(byte[] raw) {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(raw))) {
      return ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Failed to deserialize keyed state", e);
    }
  }
}

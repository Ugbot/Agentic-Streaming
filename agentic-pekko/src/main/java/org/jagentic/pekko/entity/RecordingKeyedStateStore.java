package org.jagentic.pekko.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.jagentic.core.KeyedStateStore;

/** Per-turn overlay over the entity's committed {@link KeyedStateStore}: reads see committed +
 * pending; writes are buffered into the {@link TurnMutations.Recorder} (as String scalars) and
 * applied to the committed view only when the turn's event is replayed. */
final class RecordingKeyedStateStore implements KeyedStateStore {

  private final KeyedStateStore committed;
  private final TurnMutations.Recorder rec;
  private final Map<String, Object> pending = new HashMap<>();

  RecordingKeyedStateStore(KeyedStateStore committed, TurnMutations.Recorder rec) {
    this.committed = committed;
    this.rec = rec;
  }

  @Override
  public Optional<Object> get(String key, String name) {
    if (pending.containsKey(name)) {
      return Optional.ofNullable(pending.get(name));
    }
    return committed.get(key, name);
  }

  @Override
  public void put(String key, String name, Object value) {
    pending.put(name, value);
    rec.keyed(name, value == null ? null : String.valueOf(value));
  }

  @Override
  public void clear(String key) {
    committed.clear(key);
  }
}

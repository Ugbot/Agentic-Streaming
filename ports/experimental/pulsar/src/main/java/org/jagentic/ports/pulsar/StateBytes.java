package org.jagentic.ports.pulsar;

/**
 * A minimal byte-keyed durable store — the narrow seam onto Pulsar Functions' state
 * API ({@code Context.getState/putState/deleteState}). Keeping the agent-facing
 * stores ({@link PulsarStateConversationStore}, {@link PulsarStateKeyedStore})
 * decoupled from Pulsar's {@code Context} behind this interface means they are unit
 * testable with a plain in-memory map, and the same code runs against the real
 * BookKeeper-backed state in a deployed function.
 */
public interface StateBytes {
  /** @return the stored value, or {@code null} if absent. */
  byte[] get(String key);

  void put(String key, byte[] value);

  void delete(String key);
}

package org.jagentic.core.pipeline;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.Runtime;

/** ServiceLoader SPI that lets an engine module (Pekko / Kafka-Streams / Temporal / Pulsar)
 * supply a {@link Runtime} for a backend name, so {@code backend: <name>} in a pipeline.yaml
 * resolves without the core hard-coding every engine. The core ships {@code local}; a module
 * registers a provider via {@code META-INF/services/org.jagentic.core.pipeline.BackendProvider}. */
public interface BackendProvider {

  /** The {@code backend:} value this provider handles (e.g. {@code "pekko"}). */
  String name();

  /** Build a runtime for the compiled graph. {@code conversationStore} is the durable store the
   * loader resolved (engines with their own durable state — e.g. Pekko event sourcing — may
   * ignore it). */
  Runtime create(GraphBuilder.Built built, ConversationStore conversationStore);
}

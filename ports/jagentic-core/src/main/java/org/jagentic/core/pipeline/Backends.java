package org.jagentic.core.pipeline;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.Runtime;

/**
 * Backend registry (JVM): name → {@link Runtime} from a {@link GraphBuilder.Built}. The
 * core ships {@code local}; engine modules (Pulsar/Temporal/Kafka-Streams/Pekko) provide
 * their own {@code Runtime} over the same {@code Built} graph, so "choose a backend and
 * the rest falls into place" holds across the JVM ports too.
 */
public final class Backends {

  private Backends() {}

  public static Runtime create(String name, GraphBuilder.Built built) {
    String key = (name == null ? "local" : name).trim().toLowerCase();
    if ("local".equals(key)) {
      return new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
          new KeyedStateStore.InMemory(), built.tools(), built.retriever());
    }
    throw new IllegalArgumentException(
        "backend '" + key + "' is provided by its engine module (kafka-streams/pekko/temporal/pulsar); "
            + "construct that module's Runtime with GraphBuilder.Built. Core ships 'local'.");
  }
}

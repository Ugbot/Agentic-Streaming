package org.jagentic.core.pipeline;

import java.util.ServiceLoader;

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
    return create(name, built, new ConversationStore.InMemory());
  }

  public static Runtime create(String name, GraphBuilder.Built built, ConversationStore conversationStore) {
    String key = (name == null ? "local" : name).trim().toLowerCase();
    if ("local".equals(key)) {
      return new LocalRuntime(built.graph(),
          conversationStore == null ? new ConversationStore.InMemory() : conversationStore,
          new KeyedStateStore.InMemory(), built.tools(), built.retriever());
    }
    // Engine modules (pekko/kafka-streams/temporal/pulsar) register a BackendProvider on the
    // classpath via ServiceLoader, so `backend: <name>` resolves without the core knowing them.
    ConversationStore store = conversationStore == null ? new ConversationStore.InMemory() : conversationStore;
    for (BackendProvider provider : ServiceLoader.load(BackendProvider.class)) {
      if (key.equals(provider.name().trim().toLowerCase())) {
        return provider.create(built, store);
      }
    }
    throw new IllegalArgumentException(
        "backend '" + key + "' is not available: the core ships 'local', and no BackendProvider "
            + "on the classpath claims it. Add the engine module (e.g. agentic-pekko) as a dependency.");
  }
}

package org.agentic.flink.memory.conversation;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovery + default factory for {@link ConversationStore}, mirroring the framework's other SPI
 * loaders.
 *
 * <p>{@link #discover()} returns the first {@link ConversationStore} registered via {@link
 * ServiceLoader} (a {@code META-INF/services/org.agentic.flink.memory.conversation.ConversationStore}
 * entry — e.g. a Redis-backed store on the classpath for a distributed cluster); if none is
 * registered it falls back to the process-wide {@link InMemoryConversationStore#shared() in-JVM
 * store}, which is the correct default for the embedded single-JVM deployment.
 */
public final class ConversationStores {

  private static final Logger LOG = LoggerFactory.getLogger(ConversationStores.class);

  private ConversationStores() {}

  /** The default store: the first ServiceLoader-registered impl, else the shared in-JVM store. */
  public static ConversationStore discover() {
    ServiceLoader<ConversationStore> loader = ServiceLoader.load(ConversationStore.class);
    Iterator<ConversationStore> it = loader.iterator();
    while (it.hasNext()) {
      try {
        ConversationStore store = it.next();
        // Don't let an explicit in-memory registration shadow the shared singleton.
        if (store instanceof InMemoryConversationStore) {
          continue;
        }
        LOG.info("Using ConversationStore from ServiceLoader: {}", store.getClass().getName());
        return store;
      } catch (Throwable t) {
        LOG.warn("Skipping a ConversationStore provider that failed to load: {}", t.toString());
      }
    }
    return InMemoryConversationStore.shared();
  }
}

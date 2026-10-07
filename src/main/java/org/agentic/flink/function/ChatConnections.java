package org.agentic.flink.function;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import org.agentic.flink.llm.ChatConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ServiceLoader discovery of the default {@link ChatConnection} shared by the process functions.
 * Providers that fail to load (a missing optional dependency surfaces as {@link
 * ServiceConfigurationError} or a {@link LinkageError} such as {@code NoClassDefFoundError}) are
 * logged and skipped so a later provider is still found.
 */
final class ChatConnections {
  private static final Logger LOG = LoggerFactory.getLogger(ChatConnections.class);

  private ChatConnections() {}

  /** The first loadable registered provider, or null when none is registered or loadable. */
  static ChatConnection discover() {
    return firstLoadable(ServiceLoader.load(ChatConnection.class).iterator());
  }

  /** The first loadable registered provider; throws naming {@code caller} when there is none. */
  static ChatConnection require(String caller) {
    ChatConnection connection = discover();
    if (connection == null) {
      throw new IllegalStateException(
          caller + " requires a ChatConnection registered via ServiceLoader");
    }
    return connection;
  }

  static ChatConnection firstLoadable(Iterator<ChatConnection> providers) {
    while (true) {
      try {
        if (!providers.hasNext()) {
          return null;
        }
        ChatConnection c = providers.next();
        if (c != null) {
          return c;
        }
      } catch (ServiceConfigurationError | LinkageError e) {
        LOG.warn("Skipping a ChatConnection provider that failed to load: {}", e.toString());
      }
    }
  }
}

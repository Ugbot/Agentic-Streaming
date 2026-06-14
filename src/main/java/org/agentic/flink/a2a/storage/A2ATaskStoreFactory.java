package org.agentic.flink.a2a.storage;

import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for {@link A2ATaskStore} backends, mirroring {@link
 * org.agentic.flink.storage.StorageFactory}. Built-ins recognized by short name:
 *
 * <ul>
 *   <li>{@code memory} — {@link InMemoryA2ATaskStore} (tests / embedded gateway)
 *   <li>{@code postgres} / {@code postgresql} — {@link PostgresA2ATaskStore}
 *   <li>{@code redis} — {@link RedisA2ATaskStore} (requires the optional Jedis dependency)
 * </ul>
 *
 * <p>Any other name is resolved via {@link ServiceLoader} of {@link A2ATaskStore}, matching by
 * provider name, simple class name, or FQN — so third-party backends drop in without code changes.
 */
public final class A2ATaskStoreFactory {
  private static final Logger LOG = LoggerFactory.getLogger(A2ATaskStoreFactory.class);

  private A2ATaskStoreFactory() {}

  /** Create and {@code initialize} a task store for the given backend name. */
  public static A2ATaskStore create(String backend, Map<String, String> config) throws Exception {
    if (backend == null || backend.isEmpty()) {
      throw new IllegalArgumentException("backend must be non-empty");
    }
    A2ATaskStore store = instantiate(backend);
    store.initialize(config == null ? Map.of() : config);
    LOG.info("A2ATaskStore initialized: backend={}, provider={}", backend, store.getProviderName());
    return store;
  }

  private static A2ATaskStore instantiate(String backend) {
    switch (backend.toLowerCase()) {
      case "memory":
        return new InMemoryA2ATaskStore();
      case "postgres":
      case "postgresql":
        return new PostgresA2ATaskStore();
      case "redis":
        return new RedisA2ATaskStore();
      default:
        for (ServiceLoader.Provider<A2ATaskStore> p :
            ServiceLoader.load(A2ATaskStore.class).stream().toList()) {
          try {
            A2ATaskStore candidate = p.get();
            if (matches(candidate, backend)) {
              return candidate;
            }
          } catch (Throwable t) {
            LOG.debug("Skipping A2ATaskStore provider {}: {}", p.type().getName(), t.toString());
          }
        }
        throw new IllegalArgumentException(
            "Unknown A2ATaskStore backend: "
                + backend
                + ". Built-ins: memory, postgres, redis.");
    }
  }

  private static boolean matches(A2ATaskStore store, String backend) {
    return backend.equalsIgnoreCase(store.getClass().getSimpleName())
        || backend.equalsIgnoreCase(store.getClass().getName())
        || backend.equalsIgnoreCase(store.getProviderName());
  }
}

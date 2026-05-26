package org.agentic.flink.storage;

import org.agentic.flink.storage.memory.InMemoryLongTermStore;
import org.agentic.flink.storage.memory.InMemoryShortTermStore;
import org.agentic.flink.storage.postgres.PostgresConversationStore;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for legacy {@link StorageProvider} instances.
 *
 * <p><b>Flink-state-first note:</b> Short-term memory should be obtained through {@link
 * org.agentic.flink.memory.FlinkStateShortTermMemory} rather than this factory. The
 * {@link #createShortTermStore} method is retained for backward compatibility and only supports
 * the {@code "memory"} backend, which is useful for tests and for plumbing the old
 * {@code MetricsWrapper} into examples.
 *
 * <p>Long-term store discovery uses {@link ServiceLoader} of {@link LongTermMemoryStore}, with
 * three built-ins also recognized by their short name:
 *
 * <ul>
 *   <li>{@code "memory"} — {@link InMemoryLongTermStore} (tests + local dev)
 *   <li>{@code "postgresql"} / {@code "postgres"} — {@link PostgresConversationStore} (production
 *       default)
 *   <li>{@code "redis"} — optional, discovered via ServiceLoader if Jedis is on the classpath
 * </ul>
 */
public final class StorageFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StorageFactory.class);

  private StorageFactory() {}

  /**
   * Create an {@link InMemoryShortTermStore} for backward-compatibility with code that still
   * holds a {@link ShortTermMemoryStore} reference. New code should bind a {@link
   * org.agentic.flink.memory.ShortTermMemory} from {@link
   * org.agentic.flink.memory.FlinkStateShortTermMemory#spec()} inside a {@code
   * RichFunction.open()} instead.
   *
   * @param backend Only {@code "memory"} is accepted. Other values throw.
   */
  public static ShortTermMemoryStore createShortTermStore(
      String backend, Map<String, String> config) throws Exception {
    require(backend, "backend");
    require(config, "config");

    if (!"memory".equalsIgnoreCase(backend)) {
      throw new IllegalArgumentException(
          "Only the 'memory' short-term backend is supported through StorageFactory. "
              + "For production, bind a ShortTermMemory via FlinkStateShortTermMemory.spec() "
              + "inside your RichFunction.open(). Got: "
              + backend);
    }
    ShortTermMemoryStore store = new InMemoryShortTermStore();
    store.initialize(config);
    LOG.info("Short-term store initialized: in-memory");
    return store;
  }

  /**
   * Create a long-term store by short name. Built-ins ({@code memory}, {@code postgres}) are
   * recognized first; otherwise {@link ServiceLoader} is consulted, matching by
   * {@link LongTermMemoryStore#getProviderName()} case-insensitively, by simple class name, or by
   * fully qualified class name.
   */
  public static LongTermMemoryStore createLongTermStore(
      String backend, Map<String, String> config) throws Exception {
    require(backend, "backend");
    require(config, "config");

    LongTermMemoryStore store = instantiateLongTerm(backend);
    store.initialize(config);
    LOG.info(
        "Long-term store initialized: backend={}, provider={}", backend, store.getProviderName());
    return store;
  }

  private static LongTermMemoryStore instantiateLongTerm(String backend) {
    switch (backend.toLowerCase()) {
      case "memory":
        return new InMemoryLongTermStore();
      case "postgres":
      case "postgresql":
        return new PostgresConversationStore();
      default:
        // ServiceLoader path — supports third-party backends and the optional Redis store.
        // Use Provider.get() in a try/catch so a missing optional dep (e.g. Jedis) doesn't
        // poison the iteration for unrelated backends.
        for (ServiceLoader.Provider<LongTermMemoryStore> p :
            ServiceLoader.load(LongTermMemoryStore.class).stream().toList()) {
          try {
            LongTermMemoryStore candidate = p.get();
            if (matches(candidate, backend)) {
              return candidate;
            }
          } catch (Throwable t) {
            LOG.debug(
                "Skipping LongTermMemoryStore provider {} (missing dependency?): {}",
                p.type().getName(), t.toString());
          }
        }
        throw new IllegalArgumentException(
            "Unknown long-term store backend: "
                + backend
                + ". Built-ins: memory, postgres. Discovered via ServiceLoader: "
                + discoveredNames());
    }
  }

  private static boolean matches(LongTermMemoryStore store, String backend) {
    String simple = store.getClass().getSimpleName();
    String fqn = store.getClass().getName();
    String provider = store.getProviderName();
    if (backend.equalsIgnoreCase(simple)
        || backend.equalsIgnoreCase(fqn)
        || backend.equalsIgnoreCase(provider)) {
      return true;
    }
    // Permissive alias: short backend tag matches if it's a case-insensitive substring of the
    // simple class name. Lets users say "redis" for RedisConversationStore without forcing a
    // separate alias map.
    return simple.toLowerCase().contains(backend.toLowerCase());
  }

  private static String discoveredNames() {
    TreeSet<String> names = new TreeSet<>();
    for (ServiceLoader.Provider<LongTermMemoryStore> p :
        ServiceLoader.load(LongTermMemoryStore.class).stream().toList()) {
      names.add(p.type().getSimpleName());
    }
    return names.isEmpty() ? "<none>" : String.join(", ", names);
  }

  /**
   * Vector store creation is now ServiceLoader-only. The framework ships no built-in
   * external-vector-store implementation; users plug their own (Qdrant, Pinecone, pgvector,
   * etc.) by registering a {@link VectorStore} service. For in-JVM vector memory backed by
   * Flink state, use {@link org.agentic.flink.memory.vector.FlinkStateVectorMemory}
   * instead.
   */
  public static VectorStore createVectorStore(String backend, Map<String, String> config)
      throws Exception {
    require(backend, "backend");
    require(config, "config");

    for (VectorStore candidate : ServiceLoader.load(VectorStore.class)) {
      if (backend.equalsIgnoreCase(candidate.getClass().getSimpleName())
          || backend.equalsIgnoreCase(candidate.getClass().getName())
          || backend.equalsIgnoreCase(candidate.getProviderName())) {
        candidate.initialize(config);
        return candidate;
      }
    }
    throw new IllegalArgumentException(
        "No VectorStore registered for backend: "
            + backend
            + ". For in-JVM vector memory, use FlinkStateVectorMemory.spec(...).");
  }

  /** Backwards-compatible probe used by examples and old test code. */
  public static String[] getAvailableBackends(StorageTier tier) {
    switch (tier) {
      case HOT:
        return new String[] {"memory"};
      case WARM:
        TreeSet<String> warm = new TreeSet<>();
        warm.add("memory");
        warm.add("postgres");
        warm.add("postgresql");
        for (ServiceLoader.Provider<LongTermMemoryStore> p :
            ServiceLoader.load(LongTermMemoryStore.class).stream().toList()) {
          String name = p.type().getSimpleName();
          warm.add(name);
          // Expose common short aliases derived from the class name.
          String lower = name.toLowerCase();
          if (lower.contains("redis")) warm.add("redis");
          if (lower.contains("postgres")) warm.add("postgres");
          if (lower.contains("dynamo")) warm.add("dynamodb");
          if (lower.contains("cassandra")) warm.add("cassandra");
        }
        return warm.toArray(new String[0]);
      case CHECKPOINT:
        return new String[] {"rocksdb", "hashmap"};
      case VECTOR:
        TreeSet<String> vec = new TreeSet<>();
        for (VectorStore s : ServiceLoader.load(VectorStore.class)) {
          vec.add(s.getClass().getSimpleName());
        }
        return vec.toArray(new String[0]);
      case COLD:
      default:
        return new String[] {};
    }
  }

  public static boolean isBackendAvailable(StorageTier tier, String backend) {
    for (String b : getAvailableBackends(tier)) {
      if (b.equalsIgnoreCase(backend)) {
        return true;
      }
    }
    return false;
  }

  private static void require(Object value, String name) {
    if (value == null || (value instanceof String && ((String) value).trim().isEmpty())) {
      throw new IllegalArgumentException(name + " must be non-null and non-empty");
    }
  }
}

package org.agentic.flink.storage.config;

import org.agentic.flink.channel.Channel;
import org.agentic.flink.channel.KeyedContextItem;
import org.agentic.flink.memory.ShortTermMemorySpec;
import org.agentic.flink.storage.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Configuration management for multi-tier storage architecture.
 *
 * <p>This class supports loading storage configuration from YAML files, environment variables, or
 * programmatic configuration. It provides typed access to tier-specific configurations.
 *
 * <p>Example YAML configuration:
 *
 * <pre>{@code
 * storage:
 *   hot:
 *     backend: redis
 *     config:
 *       redis.host: localhost
 *       redis.port: 6379
 *       redis.ttl.seconds: 3600
 *
 *   warm:
 *     backend: redis
 *     config:
 *       redis.host: localhost
 *       redis.port: 6379
 *       redis.database: 1
 *       redis.ttl.seconds: 86400
 *
 *   cold:
 *     backend: postgresql
 *     config:
 *       postgresql.jdbc.url: jdbc:postgresql://localhost:5432/agent_db
 *       postgresql.username: agent_user
 *       postgresql.password: ${POSTGRES_PASSWORD}
 *
 *   vector:
 *     backend: qdrant
 *     config:
 *       qdrant.host: localhost
 *       qdrant.port: 6333
 *       qdrant.collection: agent_vectors
 * }</pre>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Load from YAML file
 * StorageConfiguration config = StorageConfiguration.fromYamlFile("storage-config.yaml");
 *
 * // Create storage providers
 * ShortTermMemoryStore hotStore = config.createShortTermStore();
 * LongTermMemoryStore warmStore = config.createLongTermStore();
 *
 * // Get configuration for specific tier
 * TierConfiguration hotConfig = config.getTierConfig(StorageTier.HOT);
 * }</pre>
 *
 * <p>Status: Interface defined. YAML parsing requires Jackson dependencies. Uncomment Jackson
 * imports after adding dependencies.
 *
 * @author Agentic Flink Team
 */
public class StorageConfiguration implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(StorageConfiguration.class);

  private Map<StorageTier, TierConfiguration> tierConfigs;
  private transient ShortTermMemorySpec shortTermMemorySpec;
  private transient List<Channel<KeyedContextItem>> memoryChannels = new ArrayList<>();

  public StorageConfiguration() {
    this.tierConfigs = new HashMap<>();
    this.memoryChannels = new ArrayList<>();
  }

  public ShortTermMemorySpec getShortTermMemorySpec() {
    return shortTermMemorySpec;
  }

  public List<Channel<KeyedContextItem>> getMemoryChannels() {
    return memoryChannels == null ? new ArrayList<>() : memoryChannels;
  }

  /**
   * Load configuration from a YAML file.
   *
   * <p>Dependency required:
   *
   * <pre>{@code
   * <dependency>
   *     <groupId>com.fasterxml.jackson.dataformat</groupId>
   *     <artifactId>jackson-dataformat-yaml</artifactId>
   *     <version>2.15.2</version>
   * </dependency>
   * }</pre>
   *
   * @param filePath Path to YAML configuration file
   * @return Loaded configuration
   * @throws IOException if file reading or parsing fails
   */
  public static StorageConfiguration fromYamlFile(String filePath) throws IOException {
    LOG.info("Loading storage configuration from YAML file: {}", filePath);

    // Uncomment when Jackson YAML dependency is added:
    // ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    // File file = new File(filePath);
    // return mapper.readValue(file, StorageConfiguration.class);

    LOG.warn("YAML loading not implemented - Jackson dependency required");
    return new StorageConfiguration();
  }

  /**
   * Load configuration from classpath resource.
   *
   * @param resourcePath Classpath resource path (e.g., "config/storage.yaml")
   * @return Loaded configuration
   * @throws IOException if resource reading or parsing fails
   */
  public static StorageConfiguration fromResource(String resourcePath) throws IOException {
    LOG.info("Loading storage configuration from resource: {}", resourcePath);

    // Uncomment when Jackson YAML dependency is added:
    // ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    // InputStream is = StorageConfiguration.class.getClassLoader()
    //     .getResourceAsStream(resourcePath);
    // if (is == null) {
    //   throw new IOException("Resource not found: " + resourcePath);
    // }
    // return mapper.readValue(is, StorageConfiguration.class);

    LOG.warn("YAML loading not implemented - Jackson dependency required");
    return new StorageConfiguration();
  }

  /**
   * Create configuration programmatically.
   *
   * <p>Example:
   *
   * <pre>{@code
   * StorageConfiguration config = StorageConfiguration.builder()
   *     .withHotTier("redis", redisConfig)
   *     .withWarmTier("redis", redisWarmConfig)
   *     .withVectorTier("qdrant", qdrantConfig)
   *     .build();
   * }</pre>
   *
   * @return Builder for programmatic configuration
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Get configuration for a specific tier.
   *
   * @param tier Storage tier
   * @return Tier configuration, or null if not configured
   */
  public TierConfiguration getTierConfig(StorageTier tier) {
    return tierConfigs.get(tier);
  }

  /**
   * Set configuration for a specific tier.
   *
   * @param tier Storage tier
   * @param config Tier configuration
   */
  public void setTierConfig(StorageTier tier, TierConfiguration config) {
    tierConfigs.put(tier, config);
  }

  /**
   * Create a ShortTermMemoryStore using HOT tier configuration.
   *
   * @return Initialized short-term store
   * @throws Exception if creation or initialization fails
   */
  public ShortTermMemoryStore createShortTermStore() throws Exception {
    TierConfiguration config = tierConfigs.get(StorageTier.HOT);
    if (config == null) {
      throw new IllegalStateException("HOT tier not configured");
    }
    return StorageFactory.createShortTermStore(config.getBackend(), config.getConfig());
  }

  /**
   * Create a LongTermMemoryStore using WARM tier configuration.
   *
   * @return Initialized long-term store
   * @throws Exception if creation or initialization fails
   */
  public LongTermMemoryStore createLongTermStore() throws Exception {
    TierConfiguration config = tierConfigs.get(StorageTier.WARM);
    if (config == null) {
      throw new IllegalStateException("WARM tier not configured");
    }
    return StorageFactory.createLongTermStore(config.getBackend(), config.getConfig());
  }

  /**
   * Create a VectorStore using VECTOR tier configuration.
   *
   * @return Initialized vector store
   * @throws Exception if creation or initialization fails
   */
  public VectorStore createVectorStore() throws Exception {
    TierConfiguration config = tierConfigs.get(StorageTier.VECTOR);
    if (config == null) {
      throw new IllegalStateException("VECTOR tier not configured");
    }
    return StorageFactory.createVectorStore(config.getBackend(), config.getConfig());
  }

  /**
   * Check if a tier is configured.
   *
   * @param tier Storage tier
   * @return true if tier has configuration
   */
  public boolean isTierConfigured(StorageTier tier) {
    return tierConfigs.containsKey(tier);
  }

  /**
   * Get all configured tiers.
   *
   * @return Map of tier to configuration
   */
  public Map<StorageTier, TierConfiguration> getAllTierConfigs() {
    return new HashMap<>(tierConfigs);
  }

  /**
   * Validate configuration completeness.
   *
   * @throws IllegalStateException if configuration is invalid
   */
  public void validate() {
    LOG.info("Validating storage configuration");

    for (Map.Entry<StorageTier, TierConfiguration> entry : tierConfigs.entrySet()) {
      StorageTier tier = entry.getKey();
      TierConfiguration config = entry.getValue();

      if (config.getBackend() == null || config.getBackend().isEmpty()) {
        throw new IllegalStateException("Backend not specified for tier: " + tier);
      }

      // "spec" is a sentinel for ShortTermMemorySpec-based HOT configuration.
      if (!"spec".equalsIgnoreCase(config.getBackend())
          && !StorageFactory.isBackendAvailable(tier, config.getBackend())) {
        throw new IllegalStateException(
            "Backend '" + config.getBackend() + "' not available for tier: " + tier);
      }

      LOG.debug("Tier {} configured with backend: {}", tier, config.getBackend());
    }

    LOG.info("Storage configuration validated successfully");
  }

  /** Configuration for a single storage tier. */
  public static class TierConfiguration implements Serializable {
    private String backend;
    private Map<String, String> config;

    public TierConfiguration() {
      this.config = new HashMap<>();
    }

    public TierConfiguration(String backend, Map<String, String> config) {
      this.backend = backend;
      this.config = config != null ? config : new HashMap<>();
    }

    public String getBackend() {
      return backend;
    }

    public void setBackend(String backend) {
      this.backend = backend;
    }

    public Map<String, String> getConfig() {
      return config;
    }

    public void setConfig(Map<String, String> config) {
      this.config = config;
    }
  }

  /** Builder for programmatic configuration. */
  public static class Builder {
    private final StorageConfiguration config;

    public Builder() {
      this.config = new StorageConfiguration();
    }

    /**
     * Configure HOT tier (short-term memory).
     *
     * @param backend Backend identifier (e.g., "memory", "redis")
     * @param tierConfig Backend-specific configuration
     * @return This builder
     */
    public Builder withHotTier(String backend, Map<String, String> tierConfig) {
      config.setTierConfig(StorageTier.HOT, new TierConfiguration(backend, tierConfig));
      return this;
    }

    /**
     * Configure HOT tier with a {@link ShortTermMemorySpec} (Flink-state-backed). This is the
     * preferred path; the string-based {@link #withHotTier(String, Map)} is retained for
     * backward compatibility with the in-memory legacy store.
     */
    public Builder withHotTier(ShortTermMemorySpec spec) {
      config.shortTermMemorySpec = spec;
      // Register a sentinel tier configuration so isTierConfigured(HOT) still works.
      Map<String, String> sentinel = new HashMap<>();
      sentinel.put("spec", spec.providerName());
      config.setTierConfig(StorageTier.HOT, new TierConfiguration("spec", sentinel));
      return this;
    }

    /** Register one or more memory channels. */
    @SafeVarargs
    public final Builder withMemoryChannel(Channel<KeyedContextItem>... channels) {
      if (channels != null) {
        if (config.memoryChannels == null) {
          config.memoryChannels = new ArrayList<>();
        }
        for (Channel<KeyedContextItem> c : channels) {
          config.memoryChannels.add(c);
        }
      }
      return this;
    }

    /**
     * Configure WARM tier (long-term memory).
     *
     * @param backend Backend identifier (e.g., "redis", "dynamodb")
     * @param tierConfig Backend-specific configuration
     * @return This builder
     */
    public Builder withWarmTier(String backend, Map<String, String> tierConfig) {
      config.setTierConfig(StorageTier.WARM, new TierConfiguration(backend, tierConfig));
      return this;
    }

    /**
     * Configure COLD tier (historical storage).
     *
     * @param backend Backend identifier (e.g., "postgresql", "s3")
     * @param tierConfig Backend-specific configuration
     * @return This builder
     */
    public Builder withColdTier(String backend, Map<String, String> tierConfig) {
      config.setTierConfig(StorageTier.COLD, new TierConfiguration(backend, tierConfig));
      return this;
    }

    /**
     * Configure VECTOR tier (embeddings).
     *
     * @param backend Backend identifier (e.g., "qdrant", "pinecone")
     * @param tierConfig Backend-specific configuration
     * @return This builder
     */
    public Builder withVectorTier(String backend, Map<String, String> tierConfig) {
      config.setTierConfig(StorageTier.VECTOR, new TierConfiguration(backend, tierConfig));
      return this;
    }

    /**
     * Build the configuration.
     *
     * @return Configured StorageConfiguration instance
     */
    public StorageConfiguration build() {
      config.validate();
      return config;
    }
  }
}

package org.agentic.flink.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unified configuration for the Agentic Flink framework.
 *
 * <p>Resolves configuration values with the following priority (highest to lowest):
 * <ol>
 *   <li>Explicit properties passed via constructor or {@link #fromMap(Map)}</li>
 *   <li>Environment variables with {@code AGENTIC_FLINK_} prefix
 *       (e.g., {@code ollama.base.url} maps to {@code AGENTIC_FLINK_OLLAMA_BASE_URL})</li>
 *   <li>System properties with {@code agentic.flink.} prefix
 *       (e.g., {@code agentic.flink.ollama.base.url})</li>
 *   <li>Default values from {@link ConfigKeys}</li>
 * </ol>
 *
 * <p>This class implements {@link Serializable} so it can be used safely inside
 * Flink functions that are serialized across the cluster.
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 * // From environment (production)
 * AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();
 * String ollamaUrl = config.get(ConfigKeys.OLLAMA_BASE_URL, ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
 *
 * // From explicit properties (programmatic)
 * Map<String, String> props = new HashMap<>();
 * props.put(ConfigKeys.OLLAMA_BASE_URL, "http://my-ollama:11434");
 * AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
 *
 * // For testing (all defaults, no env vars)
 * AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see ConfigKeys
 */
public class AgenticFlinkConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String ENV_PREFIX = "AGENTIC_FLINK_";
  private static final String SYS_PROP_PREFIX = "agentic.flink.";

  /** Default values for all known config keys. */
  private static final Map<String, String> DEFAULTS;

  static {
    Map<String, String> defaults = new HashMap<>();
    defaults.put(ConfigKeys.OLLAMA_BASE_URL, ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    defaults.put(ConfigKeys.OLLAMA_MODEL, ConfigKeys.DEFAULT_OLLAMA_MODEL);
    defaults.put(ConfigKeys.REDIS_HOST, ConfigKeys.DEFAULT_REDIS_HOST);
    defaults.put(ConfigKeys.REDIS_PORT, ConfigKeys.DEFAULT_REDIS_PORT);
    defaults.put(ConfigKeys.POSTGRES_URL, ConfigKeys.DEFAULT_POSTGRES_URL);
    defaults.put(ConfigKeys.POSTGRES_USER, ConfigKeys.DEFAULT_POSTGRES_USER);
    defaults.put(ConfigKeys.POSTGRES_PASSWORD, ConfigKeys.DEFAULT_POSTGRES_PASSWORD);
    defaults.put(ConfigKeys.QDRANT_HOST, ConfigKeys.DEFAULT_QDRANT_HOST);
    defaults.put(ConfigKeys.QDRANT_PORT, ConfigKeys.DEFAULT_QDRANT_PORT);
    defaults.put(ConfigKeys.OPENAI_MODEL, ConfigKeys.DEFAULT_OPENAI_MODEL);
    DEFAULTS = Collections.unmodifiableMap(defaults);
  }

  private final Map<String, String> properties;
  private final boolean resolveEnv;

  private AgenticFlinkConfig(Map<String, String> properties, boolean resolveEnv) {
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.resolveEnv = resolveEnv;
  }

  // ==================== Factory Methods ====================

  /**
   * Creates a config that resolves from environment variables and system properties.
   *
   * <p>This is the standard factory for production use.
   *
   * @return config that reads env vars and system properties
   */
  public static AgenticFlinkConfig fromEnvironment() {
    return new AgenticFlinkConfig(Collections.emptyMap(), true);
  }

  /**
   * Creates a config from an explicit property map.
   *
   * <p>Environment variables and system properties are still consulted for keys
   * not present in the map.
   *
   * @param properties explicit property overrides
   * @return config backed by the given map with env/sysprop fallback
   */
  public static AgenticFlinkConfig fromMap(Map<String, String> properties) {
    return new AgenticFlinkConfig(properties, true);
  }

  /**
   * Creates a config suitable for unit tests.
   *
   * <p>Returns only defaults -- environment variables and system properties are
   * <b>not</b> consulted so tests are isolated from the host environment.
   *
   * @return config with default values only
   */
  public static AgenticFlinkConfig forTesting() {
    return new AgenticFlinkConfig(Collections.emptyMap(), false);
  }

  // ==================== Accessors ====================

  /**
   * Returns the resolved value for {@code key}, or {@code null} if no value is found
   * at any level (explicit, env, sysprop, defaults).
   *
   * @param key the configuration key (e.g., {@code "ollama.base.url"})
   * @return resolved value or {@code null}
   */
  public String get(String key) {
    return resolve(key);
  }

  /**
   * Returns the resolved value for {@code key}, falling back to the given default
   * if no value is found at any level.
   *
   * @param key          the configuration key
   * @param defaultValue value to return when the key cannot be resolved
   * @return resolved value or {@code defaultValue}
   */
  public String get(String key, String defaultValue) {
    String value = resolve(key);
    return value != null ? value : defaultValue;
  }

  /**
   * Returns the resolved value for {@code key} parsed as an {@code int}.
   *
   * @param key          the configuration key
   * @param defaultValue value to return when the key cannot be resolved or is not a valid integer
   * @return resolved integer value or {@code defaultValue}
   */
  public int getInt(String key, int defaultValue) {
    String value = resolve(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Exports all resolved values as a flat {@code Map<String, String>}.
   *
   * <p>This is useful for backwards compatibility with code that accepts
   * {@code Map<String, String>} configuration (e.g., storage stores).
   * The returned map contains every key that has a resolved non-null value,
   * combining explicit properties, environment/system overrides, and defaults.
   *
   * @return unmodifiable map of all resolved key-value pairs
   */
  public Map<String, String> toMap() {
    Map<String, String> result = new HashMap<>(DEFAULTS);
    // Layer on any resolved env/sysprop overrides for known keys
    for (String key : DEFAULTS.keySet()) {
      String resolved = resolve(key);
      if (resolved != null) {
        result.put(key, resolved);
      }
    }
    // Layer on explicit properties (may include keys not in DEFAULTS)
    result.putAll(properties);
    return Collections.unmodifiableMap(result);
  }

  // ==================== Internal Resolution ====================

  /**
   * Resolves a configuration key through the priority chain:
   * explicit property -> env var -> system property -> default.
   */
  private String resolve(String key) {
    // 1. Explicit properties (highest priority)
    String value = properties.get(key);
    if (value != null) {
      return value;
    }

    if (resolveEnv) {
      // 2. Environment variable: AGENTIC_FLINK_OLLAMA_BASE_URL
      String envKey = ENV_PREFIX + key.toUpperCase().replace('.', '_');
      value = System.getenv(envKey);
      if (value != null) {
        return value;
      }

      // 3. System property: agentic.flink.ollama.base.url
      String sysPropKey = SYS_PROP_PREFIX + key;
      value = System.getProperty(sysPropKey);
      if (value != null) {
        return value;
      }
    }

    // 4. Default value (lowest priority)
    return DEFAULTS.get(key);
  }

  @Override
  public String toString() {
    return String.format("AgenticFlinkConfig[properties=%d, resolveEnv=%s]",
        properties.size(), resolveEnv);
  }
}

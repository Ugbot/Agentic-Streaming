package org.agentic.flink.config;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.*;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;

/**
 * Unit tests for {@link AgenticFlinkConfig}.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Factory methods (forTesting, fromMap, fromEnvironment)
 *   <li>Default value resolution
 *   <li>Explicit property overrides via fromMap
 *   <li>Environment variable and system property resolution, including their precedence
 *   <li>get() with and without default
 *   <li>getInt() parsing and fallback
 *   <li>toMap() export of all resolved values
 * </ul>
 *
 * @author Agentic Flink Team
 */
class AgenticFlinkConfigTest {

  private static final String ENV_RETENTION = "AGENTIC_FLINK_CHECKPOINT_RETENTION";
  private static final String ENV_INTERVAL = "AGENTIC_FLINK_CHECKPOINT_INTERVAL_MS";
  private static final String ENV_CUSTOM = "AGENTIC_FLINK_MY_CUSTOM_KEY";
  private static final String SYSPROP_RETENTION = "agentic.flink.checkpoint.retention";
  private static final String SYSPROP_INTERVAL = "agentic.flink.checkpoint.interval.ms";

  // ==================== environment / system property resolution ====================

  @Test
  @SetEnvironmentVariable(key = ENV_RETENTION, value = "from-env")
  @SetEnvironmentVariable(key = ENV_CUSTOM, value = "custom-from-env")
  @DisplayName("fromEnvironment() should map AGENTIC_FLINK_<KEY> variables onto dotted keys")
  void testFromEnvironmentReadsEnvironmentVariables() {
    AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();

    assertEquals("from-env", config.get(ConfigKeys.CHECKPOINT_RETENTION));
    assertEquals("custom-from-env", config.get("my.custom.key"));
    assertEquals("from-env", config.toMap().get(ConfigKeys.CHECKPOINT_RETENTION));
    assertEquals(
        ConfigKeys.DEFAULT_CHECKPOINT_INTERVAL_MS, config.get(ConfigKeys.CHECKPOINT_INTERVAL_MS));
  }

  @Test
  @SetEnvironmentVariable(key = ENV_INTERVAL, value = "16390")
  @SetEnvironmentVariable(key = ENV_CUSTOM, value = "not-a-number")
  @DisplayName("fromEnvironment() should parse integer environment variables with getInt()")
  void testFromEnvironmentParsesIntegerVariables() {
    AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();

    assertEquals(16390, config.getInt(ConfigKeys.CHECKPOINT_INTERVAL_MS, -1));
    assertEquals(
        -1,
        config.getInt("my.custom.key", -1),
        "a variable that is present but not numeric falls back to the default");
  }

  @Test
  @SetEnvironmentVariable(key = ENV_RETENTION, value = "from-env")
  @SetSystemProperty(key = SYSPROP_RETENTION, value = "from-sysprop")
  @DisplayName("explicit properties beat environment variables, which beat system properties")
  void testResolutionPrecedence() {
    String explicit = "explicit-" + UUID.randomUUID();

    assertEquals(
        "from-env",
        AgenticFlinkConfig.fromEnvironment().get(ConfigKeys.CHECKPOINT_RETENTION),
        "env var beats sysprop");
    assertEquals(
        explicit,
        AgenticFlinkConfig.fromMap(Map.of(ConfigKeys.CHECKPOINT_RETENTION, explicit))
            .get(ConfigKeys.CHECKPOINT_RETENTION),
        "explicit property beats env var");
  }

  @Test
  @ClearEnvironmentVariable(key = ENV_RETENTION)
  @ClearEnvironmentVariable(key = ENV_INTERVAL)
  @ClearSystemProperty(key = SYSPROP_INTERVAL)
  @SetSystemProperty(key = SYSPROP_RETENTION, value = "from-sysprop")
  @DisplayName("system properties are consulted when no env var is set, and beat defaults")
  void testSystemPropertyBeatsDefault() {
    int port = ThreadLocalRandom.current().nextInt(1024, 65535);
    System.setProperty(SYSPROP_INTERVAL, Integer.toString(port));

    AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();

    assertEquals("from-sysprop", config.get(ConfigKeys.CHECKPOINT_RETENTION));
    assertEquals(port, config.getInt(ConfigKeys.CHECKPOINT_INTERVAL_MS, -1));
  }

  @Test
  @SetEnvironmentVariable(key = ENV_RETENTION, value = "from-env")
  @SetSystemProperty(key = SYSPROP_RETENTION, value = "from-sysprop")
  @DisplayName("forTesting() ignores environment variables and system properties")
  void testForTestingIgnoresEnvironment() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    assertEquals(
        ConfigKeys.DEFAULT_CHECKPOINT_RETENTION, config.get(ConfigKeys.CHECKPOINT_RETENTION));
    assertEquals(
        ConfigKeys.DEFAULT_CHECKPOINT_RETENTION,
        config.toMap().get(ConfigKeys.CHECKPOINT_RETENTION));
  }

  // ==================== forTesting() Factory ====================

  @Test
  @DisplayName("forTesting() should return config with known defaults")
  void testForTestingReturnsDefaults() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
    assertEquals(
        ConfigKeys.DEFAULT_CHECKPOINT_RETENTION, config.get(ConfigKeys.CHECKPOINT_RETENTION));
    assertEquals(
        ConfigKeys.DEFAULT_CHECKPOINT_INTERVAL_MS, config.get(ConfigKeys.CHECKPOINT_INTERVAL_MS));
  }

  @Test
  @DisplayName("forTesting() should return null for unknown keys with no default")
  void testForTestingReturnsNullForUnknownKeys() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    assertNull(config.get("totally.unknown.key"));
  }

  // ==================== fromMap() Factory ====================

  @Test
  @DisplayName("fromMap() should override defaults with explicit properties")
  void testFromMapOverridesDefaults() {
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_RETENTION, "overridden");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
    assertEquals("overridden", config.get(ConfigKeys.CHECKPOINT_RETENTION));
    // Non-overridden keys still resolve to defaults
    assertEquals(
        ConfigKeys.DEFAULT_CHECKPOINT_TIMEOUT_MS, config.get(ConfigKeys.CHECKPOINT_TIMEOUT_MS));
  }

  @Test
  @DisplayName("fromMap() should support keys not in DEFAULTS")
  void testFromMapSupportsCustomKeys() {
    Map<String, String> props = new HashMap<>();
    props.put("my.custom.key", "custom-value");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);

    assertEquals("custom-value", config.get("my.custom.key"));
  }

  // ==================== get() with default ====================

  @Test
  @DisplayName("get(key, default) should return resolved value when key exists")
  void testGetWithDefaultReturnsResolvedValue() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    String value = config.get(ConfigKeys.CHECKPOINT_RETENTION, "fallback");
    assertEquals(ConfigKeys.DEFAULT_CHECKPOINT_RETENTION, value);
  }

  @Test
  @DisplayName("get(key, default) should return default when key is unknown")
  void testGetWithDefaultReturnsFallback() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    String value = config.get("nonexistent.key", "my-fallback");
    assertEquals("my-fallback", value);
  }

  // ==================== getInt() ====================

  @Test
  @DisplayName("getInt() should parse integer values from config")
  void testGetIntParsesValue() {
    Map<String, String> props = new HashMap<>();
    props.put("my.port", "8080");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);

    assertEquals(8080, config.getInt("my.port", 9999));
  }

  @Test
  @DisplayName("getInt() should return default for missing key")
  void testGetIntReturnsDefaultForMissingKey() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    assertEquals(42, config.getInt("totally.missing.key", 42));
  }

  @Test
  @DisplayName("getInt() should return default for non-numeric value")
  void testGetIntReturnsDefaultForNonNumericValue() {
    Map<String, String> props = new HashMap<>();
    props.put("bad.number", "not-a-number");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);

    assertEquals(100, config.getInt("bad.number", 100));
  }

  @Test
  @DisplayName("getInt() should parse numeric values from DEFAULTS")
  void testGetIntParsesDefaultValue() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    int interval = config.getInt(ConfigKeys.CHECKPOINT_INTERVAL_MS, -1);
    assertEquals(Integer.parseInt(ConfigKeys.DEFAULT_CHECKPOINT_INTERVAL_MS), interval);
  }

  // ==================== toMap() ====================

  @Test
  @DisplayName("toMap() should contain all default keys")
  void testToMapContainsAllDefaults() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
    Map<String, String> map = config.toMap();
    assertTrue(map.containsKey(ConfigKeys.CHECKPOINT_RETENTION));
    assertTrue(map.containsKey(ConfigKeys.CHECKPOINT_INTERVAL_MS));
  }

  @Test
  @DisplayName("toMap() should include explicit overrides")
  void testToMapIncludesOverrides() {
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.CHECKPOINT_RETENTION, "overridden");
    props.put("extra.key", "extra-value");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
    Map<String, String> map = config.toMap();

    assertEquals("overridden", map.get(ConfigKeys.CHECKPOINT_RETENTION));
    assertEquals("extra-value", map.get("extra.key"));
    // Defaults still present for non-overridden keys
    assertEquals(
        ConfigKeys.DEFAULT_CHECKPOINT_TIMEOUT_MS, map.get(ConfigKeys.CHECKPOINT_TIMEOUT_MS));
  }

  @Test
  @DisplayName("toMap() should return an unmodifiable map")
  void testToMapIsUnmodifiable() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
    Map<String, String> map = config.toMap();

    assertThrows(UnsupportedOperationException.class, () -> map.put("new.key", "value"));
  }

  // ==================== toString() ====================

  @Test
  @DisplayName("toString() should include properties count and resolveEnv flag")
  void testToString() {
    AgenticFlinkConfig testConfig = AgenticFlinkConfig.forTesting();
    String str = testConfig.toString();

    assertTrue(str.contains("properties=0"));
    assertTrue(str.contains("resolveEnv=false"));

    Map<String, String> props = new HashMap<>();
    props.put("a", "b");
    AgenticFlinkConfig mapConfig = AgenticFlinkConfig.fromMap(props);
    String mapStr = mapConfig.toString();

    assertTrue(mapStr.contains("properties=1"));
    assertTrue(mapStr.contains("resolveEnv=true"));
  }
}

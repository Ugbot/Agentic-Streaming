package org.agentic.flink.config;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.*;

/**
 * Unit tests for {@link AgenticFlinkConfig}.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Factory methods (forTesting, fromMap, fromEnvironment)
 *   <li>Default value resolution
 *   <li>Explicit property overrides via fromMap
 *   <li>get() with and without default
 *   <li>getInt() parsing and fallback
 *   <li>toMap() export of all resolved values
 * </ul>
 *
 * @author Agentic Flink Team
 */
class AgenticFlinkConfigTest {

  // ==================== forTesting() Factory ====================

  @Test
  @DisplayName("forTesting() should return config with known defaults")
  void testForTestingReturnsDefaults() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    assertEquals(ConfigKeys.DEFAULT_OLLAMA_BASE_URL, config.get(ConfigKeys.OLLAMA_BASE_URL));
    assertEquals(ConfigKeys.DEFAULT_OLLAMA_MODEL, config.get(ConfigKeys.OLLAMA_MODEL));
    assertEquals(ConfigKeys.DEFAULT_REDIS_HOST, config.get(ConfigKeys.REDIS_HOST));
    assertEquals(ConfigKeys.DEFAULT_REDIS_PORT, config.get(ConfigKeys.REDIS_PORT));
    assertEquals(ConfigKeys.DEFAULT_POSTGRES_URL, config.get(ConfigKeys.POSTGRES_URL));
    assertEquals(ConfigKeys.DEFAULT_POSTGRES_USER, config.get(ConfigKeys.POSTGRES_USER));
    assertEquals(ConfigKeys.DEFAULT_POSTGRES_PASSWORD, config.get(ConfigKeys.POSTGRES_PASSWORD));
    assertEquals(ConfigKeys.DEFAULT_QDRANT_HOST, config.get(ConfigKeys.QDRANT_HOST));
    assertEquals(ConfigKeys.DEFAULT_QDRANT_PORT, config.get(ConfigKeys.QDRANT_PORT));
    assertEquals(ConfigKeys.DEFAULT_OPENAI_MODEL, config.get(ConfigKeys.OPENAI_MODEL));
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
    props.put(ConfigKeys.OLLAMA_BASE_URL, "http://custom-ollama:11434");
    props.put(ConfigKeys.REDIS_HOST, "redis.example.com");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);

    assertEquals("http://custom-ollama:11434", config.get(ConfigKeys.OLLAMA_BASE_URL));
    assertEquals("redis.example.com", config.get(ConfigKeys.REDIS_HOST));
    // Non-overridden keys still resolve to defaults
    assertEquals(ConfigKeys.DEFAULT_OLLAMA_MODEL, config.get(ConfigKeys.OLLAMA_MODEL));
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

    String value = config.get(ConfigKeys.REDIS_HOST, "fallback-host");
    assertEquals(ConfigKeys.DEFAULT_REDIS_HOST, value);
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
  @DisplayName("getInt() should parse default port values from DEFAULTS")
  void testGetIntParsesDefaultPort() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();

    int port = config.getInt(ConfigKeys.REDIS_PORT, -1);
    assertEquals(Integer.parseInt(ConfigKeys.DEFAULT_REDIS_PORT), port);
  }

  // ==================== toMap() ====================

  @Test
  @DisplayName("toMap() should contain all default keys")
  void testToMapContainsAllDefaults() {
    AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
    Map<String, String> map = config.toMap();

    assertTrue(map.containsKey(ConfigKeys.OLLAMA_BASE_URL));
    assertTrue(map.containsKey(ConfigKeys.OLLAMA_MODEL));
    assertTrue(map.containsKey(ConfigKeys.REDIS_HOST));
    assertTrue(map.containsKey(ConfigKeys.REDIS_PORT));
    assertTrue(map.containsKey(ConfigKeys.POSTGRES_URL));
    assertTrue(map.containsKey(ConfigKeys.POSTGRES_USER));
    assertTrue(map.containsKey(ConfigKeys.POSTGRES_PASSWORD));
    assertTrue(map.containsKey(ConfigKeys.QDRANT_HOST));
    assertTrue(map.containsKey(ConfigKeys.QDRANT_PORT));
    assertTrue(map.containsKey(ConfigKeys.OPENAI_MODEL));
  }

  @Test
  @DisplayName("toMap() should include explicit overrides")
  void testToMapIncludesOverrides() {
    Map<String, String> props = new HashMap<>();
    props.put(ConfigKeys.REDIS_HOST, "overridden-host");
    props.put("extra.key", "extra-value");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
    Map<String, String> map = config.toMap();

    assertEquals("overridden-host", map.get(ConfigKeys.REDIS_HOST));
    assertEquals("extra-value", map.get("extra.key"));
    // Defaults still present for non-overridden keys
    assertEquals(ConfigKeys.DEFAULT_OLLAMA_MODEL, map.get(ConfigKeys.OLLAMA_MODEL));
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

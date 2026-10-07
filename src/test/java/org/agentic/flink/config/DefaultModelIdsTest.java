package org.agentic.flink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the model identifiers shipped as defaults in {@link ConfigKeys} to the allowlist in {@code
 * src/test/resources/default-model-ids.allowlist}. A default can only change together with the
 * allowlist, which records where each identifier was verified against the vendor catalogue.
 */
class DefaultModelIdsTest {

  static final String ALLOWLIST = "/default-model-ids.allowlist";

  static Map<String, Set<String>> allowed;

  @BeforeAll
  static void loadAllowlist() throws IOException {
    Map<String, Set<String>> byProvider = new HashMap<>();
    try (InputStream in = DefaultModelIdsTest.class.getResourceAsStream(ALLOWLIST)) {
      assertNotNull(in, "missing test resource " + ALLOWLIST);
      for (String raw : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        String line = raw.strip();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        String[] parts = line.split("\\s+");
        assertEquals(2, parts.length, "allowlist line must be '<provider> <model id>': " + line);
        byProvider.computeIfAbsent(parts[0], p -> new HashSet<>()).add(parts[1]);
      }
    }
    assertFalse(byProvider.isEmpty(), "allowlist has no entries");
    allowed = byProvider;
  }

  @Test
  @DisplayName("Ollama chat and embedding defaults are allowlisted")
  void ollamaDefaults() {
    assertAllowed("ollama", ConfigKeys.DEFAULT_OLLAMA_MODEL);
    assertAllowed("ollama", ConfigKeys.DEFAULT_OLLAMA_EMBED_MODEL);
  }

  @Test
  @DisplayName("Anthropic default is allowlisted")
  void anthropicDefault() {
    assertAllowed("anthropic", ConfigKeys.DEFAULT_ANTHROPIC_MODEL);
  }

  @Test
  @DisplayName("OpenAI default is allowlisted")
  void openAiDefault() {
    assertAllowed("openai", ConfigKeys.DEFAULT_OPENAI_MODEL);
  }

  @Test
  @DisplayName("every allowlisted provider is a provider ConfigKeys has a default for")
  void noUnknownProviders() {
    assertEquals(Set.of("ollama", "anthropic", "openai"), allowed.keySet());
  }

  private static void assertAllowed(String provider, String modelId) {
    Set<String> ids = allowed.getOrDefault(provider, Set.of());
    assertTrue(
        ids.contains(modelId),
        () ->
            provider
                + " default '"
                + modelId
                + "' is not in "
                + ALLOWLIST
                + " (allowed: "
                + ids
                + "). Verify the id against the vendor catalogue and add it deliberately.");
  }
}

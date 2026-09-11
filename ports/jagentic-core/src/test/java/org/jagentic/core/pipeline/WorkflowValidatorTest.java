package org.jagentic.core.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.pipeline.WorkflowValidator.WorkflowValidationException;
import org.jagentic.core.LongTermStore;
import org.junit.jupiter.api.Test;

/** The seven loader rules from spec/README.md, the closed key set, and store unavailability. */
class WorkflowValidatorTest {

  private static String rnd() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  private static Map<String, Object> valid() {
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "a-" + rnd());
    agent.put("router", new LinkedHashMap<>(Map.of("kind", "keyword", "default", "main",
        "rules", new LinkedHashMap<>(Map.of("main", List.of("charge"), "other", List.of("card"))))));
    agent.put("paths", new LinkedHashMap<>(Map.of(
        "main", new LinkedHashMap<>(Map.of("brain", "rule", "prompt", "p",
            "tool_triggers", new LinkedHashMap<>(Map.of("charge", "lookup")))),
        "other", new LinkedHashMap<>(Map.of("brain", "rule", "prompt", "p")))));
    agent.put("verifier", new LinkedHashMap<>(Map.of("kind", "none")));
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("spec_version", "agentic/v1");
    s.put("backend", "local");
    s.put("agent", agent);
    s.put("tools", new java.util.ArrayList<>(List.of(
        new LinkedHashMap<>(Map.of("id", "lookup", "kind", "constant", "value", 1)),
        new LinkedHashMap<>(Map.of("id", "undo", "kind", "constant", "value", 2)))));
    return s;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> agent(Map<String, Object> s) {
    return (Map<String, Object>) s.get("agent");
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> tools(Map<String, Object> s) {
    return (List<Map<String, Object>>) s.get("tools");
  }

  private static WorkflowValidationException rejects(Map<String, Object> s) {
    return assertThrows(WorkflowValidationException.class, () -> WorkflowValidator.validate(s));
  }

  @Test
  void acceptsAValidV1WorkflowAndExtensions() {
    Map<String, Object> s = valid();
    s.put("x-owner", rnd());
    s.put("runtime", Map.of("anything", rnd(), "nested", Map.of("k", 1)));
    agent(s).put("x-note", rnd());
    tools(s).get(0).put("x-fail-attempts", 2);
    assertNotNull(WorkflowValidator.validate(s));
    assertNotNull(GraphBuilder.build(s, null).graph());
  }

  @Test
  void rejectsUnknownSpecVersionAndUnknownKeys() {
    Map<String, Object> s = valid();
    s.put("spec_version", "agentic/v" + rnd());
    assertTrue(rejects(s).getMessage().contains("spec_version"));

    s = valid();
    s.put("bogus_" + rnd(), 1);
    rejects(s);
    s = valid();
    agent(s).put("unknown_" + rnd(), 1);
    rejects(s);
    s = valid();
    s.put("policies", Map.of("retry", Map.of("kind", "fixed", "typo_" + rnd(), 1)));
    rejects(s);
    s = valid();
    tools(s).get(0).put("surprise", 1);
    rejects(s);
  }

  @Test
  void rule1RouterRulesMustNameDeclaredPaths() {
    Map<String, Object> s = valid();
    ((Map<String, Object>) ((Map<String, Object>) agent(s).get("router")).get("rules"))
        .put("missing_" + rnd(), List.of("x"));
    assertTrue(rejects(s).getMessage().contains("router"));
    s = valid();
    ((Map<String, Object>) agent(s).get("router")).put("default", "nowhere");
    rejects(s);
  }

  @Test
  void rule2ToolTriggersMustNameDeclaredTools() {
    Map<String, Object> s = valid();
    ((Map<String, Object>) ((Map<String, Object>) agent(s).get("paths")).get("main"))
        .put("tool_triggers", Map.of("charge", "ghost_" + rnd()));
    assertTrue(rejects(s).getMessage().contains("ghost_"));
  }

  @Test
  void rule3ToolIdsAreUnique() {
    Map<String, Object> s = valid();
    tools(s).add(new LinkedHashMap<>(Map.of("id", "lookup", "kind", "constant", "value", 3)));
    assertTrue(rejects(s).getMessage().contains("lookup"));
  }

  @Test
  void rule4LlmBrainNeedsAnLlmSection() {
    Map<String, Object> s = valid();
    ((Map<String, Object>) ((Map<String, Object>) agent(s).get("paths")).get("other")).put("brain", "llm");
    assertTrue(rejects(s).getMessage().contains("llm"));
    s.put("llm", Map.of("provider", "openai", "model", "m", "base_url", "http://127.0.0.1:1"));
    assertNotNull(WorkflowValidator.validate(s));
  }

  @Test
  void rule5EmbeddingAndRetrievalDimensionsAgree() {
    Map<String, Object> s = valid();
    s.put("embeddings", Map.of("provider", "hash", "dim", 64));
    s.put("retrieval", Map.of("dim", 32, "top_k", 2));
    assertTrue(rejects(s).getMessage().contains("dim"));
  }

  @Test
  void rule6SagaStepsAndCompensationsReferenceDeclaredTools() {
    Map<String, Object> s = valid();
    s.put("saga", Map.of("steps", List.of(Map.of("name", "s", "tool", "ghost_" + rnd()))));
    rejects(s);
    s = valid();
    s.put("saga", Map.of("steps", List.of(Map.of("name", "s", "tool", "lookup", "compensate_with", "nope"))));
    rejects(s);
    s = valid();
    tools(s).get(0).put("compensation", "nope_" + rnd());
    rejects(s);
    s = valid();
    tools(s).get(0).put("compensation", "undo");
    s.put("saga", Map.of("steps", List.of(Map.of("name", "s", "tool", "lookup"))));
    assertEquals("undo", GraphBuilder.build(s, null).graph().saga().steps().get(0).compensateWith());
  }

  @Test
  void rule7UnreachableStoreFailsUnlessDegradeIsExplicit() {
    Map<String, Object> pg = new HashMap<>(Map.of("kind", "postgres",
        "url", "jdbc:postgresql://127.0.0.1:1/" + rnd(), "user", "u", "password", "p"));
    StoreAvailability strict = new StoreAvailability();
    assertThrows(StoreAvailability.StoreUnavailableException.class,
        () -> PipelineLoader.buildLongTermStore(pg, strict));
    assertTrue(strict.degradations().isEmpty());

    pg.put("on_unavailable", "degrade");
    StoreAvailability lenient = new StoreAvailability();
    LongTermStore store = PipelineLoader.buildLongTermStore(pg, lenient);
    assertNotNull(store);
    assertEquals(1, lenient.degradations().size());
    assertTrue(lenient.degradations().get(0).startsWith("stores.long_term degraded to memory"));

    Map<String, Object> redis = new HashMap<>(Map.of("kind", "redis", "url", "redis://127.0.0.1:1"));
    assertThrows(StoreAvailability.StoreUnavailableException.class,
        () -> PipelineLoader.buildConversationStore(redis, new StoreAvailability()));
    redis.put("on_unavailable", "degrade");
    ConversationStore cs = PipelineLoader.buildConversationStore(redis, new StoreAvailability());
    assertTrue(cs instanceof ConversationStore.InMemory);

    Map<String, Object> s = valid();
    s.put("stores", Map.of("long_term", Map.of("kind", "memory", "on_unavailable", "explode")));
    rejects(s);
  }

  @Test
  void unsupportedOptionsAreRejectedNotIgnored() {
    Map<String, Object> s = valid();
    s.put("a2a", List.of(Map.of("name", "peer", "transport", "grpc", "url", "grpc://x")));
    assertThrows(IllegalArgumentException.class, () -> GraphBuilder.build(s, null));
    Map<String, Object> t = valid();
    tools(t).get(0).put("kind", "mystery_" + rnd());
    assertThrows(IllegalArgumentException.class, () -> GraphBuilder.build(t, null));
    Map<String, Object> v = valid();
    agent(v).put("verifier", Map.of("kind", "oracle_" + rnd()));
    assertThrows(IllegalArgumentException.class, () -> GraphBuilder.build(v, null));
  }
}

package org.agentic.flink.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

class AgentPlanRoundTripTest {

  @Test
  void roundTripPreservesAllFields() {
    String agentId = "agent-" + UUID.randomUUID();
    String prompt = "prompt-" + UUID.randomUUID();
    String chatFqn = "com.example.Chat" + ThreadLocalRandom.current().nextInt(1000);
    Map<String, String> chatCfg =
        Map.of("k1", UUID.randomUUID().toString(), "k2", UUID.randomUUID().toString());

    String pickle = Base64.getEncoder().encodeToString(randomBytes(64));
    ToolSpec javaTool =
        new ToolSpec(ToolSpec.KIND_JAVA, "web", "fetch", "com.example.Web", Map.of(), null, null);
    ToolSpec pyTool =
        new ToolSpec(
            ToolSpec.KIND_PYTHON, "classify", "py", null, null, pickle, List.of("text"));
    ActionSpec action =
        new ActionSpec(
            "handle",
            List.of("ticket", "alert"),
            Base64.getEncoder().encodeToString(randomBytes(48)));
    ListenerSpec javaListener =
        new ListenerSpec(ListenerSpec.KIND_JAVA, "com.example.Listener", null);
    ListenerSpec pyListener =
        new ListenerSpec(
            ListenerSpec.KIND_PYTHON, null, Base64.getEncoder().encodeToString(randomBytes(32)));

    AgentPlan plan =
        new AgentPlan(
            agentId,
            prompt,
            new ResourceSpec(chatFqn, chatCfg),
            Map.of("model", "qwen2.5:3b", "temperature", "0.3"),
            List.of(javaTool, pyTool),
            List.of(action),
            Map.of(
                "embedder",
                new ResourceSpec("com.example.Embedder", Map.of("dim", "384")),
                "corpus",
                new ResourceSpec("com.example.Corpus", Map.of())),
            List.of(javaListener, pyListener));

    String json = plan.toJson();
    assertNotNull(json);
    AgentPlan round = AgentPlan.fromJson(json);
    assertNotSame(plan, round);

    assertEquals(plan.getAgentId(), round.getAgentId());
    assertEquals(plan.getSystemPrompt(), round.getSystemPrompt());
    assertEquals(plan.getChatConnection().getFqn(), round.getChatConnection().getFqn());
    assertEquals(plan.getChatConnection().getConfig(), round.getChatConnection().getConfig());
    assertEquals(plan.getChatSetup(), round.getChatSetup());
    assertEquals(plan.getTools().size(), round.getTools().size());
    assertEquals(plan.getTools().get(0).getFqn(), round.getTools().get(0).getFqn());
    assertEquals(
        plan.getTools().get(1).getCloudpickleB64(), round.getTools().get(1).getCloudpickleB64());
    assertEquals(
        plan.getTools().get(1).getParamNames(), round.getTools().get(1).getParamNames());
    assertEquals(plan.getActions().get(0).getEvents(), round.getActions().get(0).getEvents());
    assertEquals(plan.getResources().keySet(), round.getResources().keySet());
    assertEquals(plan.getListeners().size(), round.getListeners().size());
    assertTrue(round.getListeners().get(0).isJava());
    assertTrue(round.getListeners().get(1).isPython());
  }

  @Test
  void missingAgentIdRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new AgentPlan(null, null, null, null, null, null, null, null));
  }

  @Test
  void javaToolRequiresFqn() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ToolSpec(ToolSpec.KIND_JAVA, "t", null, null, null, null, null));
  }

  @Test
  void pythonToolRequiresCloudpickle() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ToolSpec(ToolSpec.KIND_PYTHON, "t", null, null, null, null, List.of("x")));
  }

  @Test
  void planReaderInstantiatesNoArgClass() {
    Object inst =
        PlanReader.instantiate(new ResourceSpec(NoArgFixture.class.getName(), Map.of()));
    assertTrue(inst instanceof NoArgFixture);
  }

  @Test
  void planReaderCallsInitialize() {
    NoArgFixture f =
        (NoArgFixture)
            PlanReader.instantiate(
                new ResourceSpec(NoArgFixture.class.getName(), Map.of("hello", "world")));
    assertEquals("world", f.config.get("hello"));
  }

  @Test
  void planReaderUsesMapArgConstructor() {
    MapArgFixture f =
        (MapArgFixture)
            PlanReader.instantiate(
                new ResourceSpec(MapArgFixture.class.getName(), Map.of("k", "v")));
    assertEquals("v", f.config.get("k"));
  }

  @Test
  void planReaderRejectsUnknownFqn() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PlanReader.instantiate(new ResourceSpec("com.example.NotARealClass", Map.of())));
  }

  private static byte[] randomBytes(int n) {
    byte[] b = new byte[n];
    ThreadLocalRandom.current().nextBytes(b);
    return b;
  }

  public static final class NoArgFixture {
    public Map<String, String> config;

    public NoArgFixture() {}

    public void initialize(Map<String, String> cfg) {
      this.config = cfg;
    }
  }

  public static final class MapArgFixture {
    public final Map<String, String> config;

    public MapArgFixture(Map<String, String> cfg) {
      this.config = cfg;
    }
  }
}

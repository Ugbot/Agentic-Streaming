package org.agentic.flink.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.execution.LLMResponse;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.EchoChatConnection;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ClassifierGuardrailTest {

  /** Listener that counts hook invocations so we can assert observability. */
  static final class RecordingListener implements AgentEventListener {
    private static final long serialVersionUID = 1L;
    final AtomicInteger blocks = new AtomicInteger();
    final AtomicInteger rewrites = new AtomicInteger();

    @Override
    public void onGuardrailBlock(String agentId, String modelName, String label) {
      blocks.incrementAndGet();
    }

    @Override
    public void onGuardrailRewrite(String agentId, String modelName, String reason) {
      rewrites.incrementAndGet();
    }
  }

  @Test
  @DisplayName("Guardrail returns ALLOW when classifier label is not in the block-list")
  void allowsSafeContent() {
    EchoInferenceConnection conn = EchoInferenceConnection.withLabel("safe");
    ClassifierGuardrail guardrail =
        new ClassifierGuardrail(
            "safety",
            conn,
            InferenceSetup.builder().withModelName("safety-v1").withModelUri("u").build(),
            Set.of("unsafe", "toxic"),
            true,
            false);

    GuardrailDecision before = guardrail.beforeChat("agent-1", List.of(ChatMessage.user("hello")));
    assertEquals(GuardrailDecision.Action.ALLOW, before.getAction());
  }

  @Test
  @DisplayName("Guardrail returns BLOCK when classifier label matches block-list")
  void blocksUnsafeContent() {
    EchoInferenceConnection conn = EchoInferenceConnection.withLabel("unsafe");
    ClassifierGuardrail guardrail =
        new ClassifierGuardrail(
            "safety",
            conn,
            InferenceSetup.builder().withModelName("safety-v1").withModelUri("u").build(),
            Set.of("unsafe", "toxic"),
            true,
            false);

    GuardrailDecision before = guardrail.beforeChat("agent-1", List.of(ChatMessage.user("bad stuff")));
    assertEquals(GuardrailDecision.Action.BLOCK, before.getAction());
    assertTrue(before.getReason().contains("unsafe"));
    assertNotNull(before.getModelName());
  }

  @Test
  @DisplayName("Pre-chat block short-circuits LLMClient and fires listener hook")
  void preBlockShortCircuitsLLMClient() {
    EchoInferenceConnection inferConn = EchoInferenceConnection.withLabel("toxic");
    ClassifierGuardrail guardrail =
        new ClassifierGuardrail(
            "safety",
            inferConn,
            InferenceSetup.builder().withModelName("safety-v1").withModelUri("u").build(),
            Set.of("toxic"),
            true,
            false);

    // Use the framework's existing test ChatConnection so the chat path is real-but-stubbed.
    LLMClient llmClient =
        LLMClient.builder()
            .withModel("qwen2.5:3b")
            .withTemperature(0.5)
            .build(new EchoChatConnection("!!"));

    RecordingListener listener = new RecordingListener();
    llmClient.withGuardrails(List.of(guardrail), "agent-1", listener);

    LLMResponse resp =
        llmClient.chat(List.of(Map.of("role", "user", "content", "do something toxic")));

    assertEquals(1, listener.blocks.get());
    assertEquals(0, listener.rewrites.get());
    assertTrue(resp.getText().toLowerCase().contains("toxic"));
  }

  @Test
  @DisplayName("Post-chat rewrite swaps the response payload and fires the rewrite hook")
  void postRewriteSwapsPayload() {
    // Custom guardrail that always rewrites after.
    Guardrail rewriter =
        new Guardrail() {
          private static final long serialVersionUID = 1L;

          @Override
          public GuardrailDecision afterChat(String agentId, ChatResponse response) {
            return GuardrailDecision.rewrite("[REDACTED]", "policy-3", "rewriter-model");
          }
        };

    LLMClient llmClient =
        LLMClient.builder().withModel("qwen2.5:3b").build(new EchoChatConnection("!!"));
    RecordingListener listener = new RecordingListener();
    llmClient.withGuardrails(List.of(rewriter), "agent-2", listener);

    LLMResponse resp = llmClient.chat(List.of(Map.of("role", "user", "content", "anything")));
    assertEquals("[REDACTED]", resp.getText());
    assertEquals(1, listener.rewrites.get());
    assertEquals(0, listener.blocks.get());
  }

  @Test
  @DisplayName("With no guardrails registered, LLMClient behaviour is unchanged")
  void noGuardrailsZeroBehaviourChange() {
    LLMClient llmClient =
        LLMClient.builder().withModel("qwen2.5:3b").build(new EchoChatConnection("!"));
    LLMResponse resp = llmClient.chat(List.of(Map.of("role", "user", "content", "hi")));
    assertEquals("hi!", resp.getText());
    assertFalse(resp.getText().contains("Blocked"));
  }

  @Test
  @DisplayName("InferenceToolAdapter wraps a Classifier and returns label+score+probabilities")
  void inferenceToolAdapterClassifier() throws Exception {
    EchoInferenceConnection conn =
        new EchoInferenceConnection(
            "positive", 0.92, Map.of("positive", 0.92, "negative", 0.08), 0.5, 8);
    InferenceToolAdapter adapter =
        new InferenceToolAdapter(
            "sentiment",
            "sentiment classifier",
            conn,
            InferenceSetup.builder().withModelName("sst2").withModelUri("u").build(),
            InferenceToolAdapter.TaskKind.CLASSIFIER);

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>)
            adapter.execute(Map.of("text", "great product, loved it")).get();

    assertEquals("positive", result.get("label"));
    assertEquals(0.92, (Double) result.get("score"), 1e-9);
    @SuppressWarnings("unchecked")
    Map<String, Double> probs = (Map<String, Double>) result.get("probabilities");
    assertEquals(0.92, probs.get("positive"), 1e-9);
  }

  @Test
  @DisplayName("InferenceToolAdapter wraps a Scorer and returns just the numeric score")
  void inferenceToolAdapterScorer() throws Exception {
    EchoInferenceConnection conn =
        new EchoInferenceConnection("n/a", 0.0, Map.of(), 0.73, 8);
    InferenceToolAdapter adapter =
        new InferenceToolAdapter(
            "ranker",
            "relevance scorer",
            conn,
            InferenceSetup.builder().withModelName("ranker-v1").withModelUri("u").build(),
            InferenceToolAdapter.TaskKind.SCORER);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) adapter.execute(Map.of("text", "x")).get();
    assertEquals(0.73, (Double) result.get("score"), 1e-9);
  }
}

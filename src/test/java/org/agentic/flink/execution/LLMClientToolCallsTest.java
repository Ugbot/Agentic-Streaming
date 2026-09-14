package org.agentic.flink.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.GuardrailDecision;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatRole;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.ChatToolCall;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.Test;

/** F6: structured tool requests, Jackson arguments, labelled text fallback, guardrail history, listener re-resolution. */
@SuppressWarnings("deprecation")
class LLMClientToolCallsTest {

  @Test
  void structuredToolExecutionRequestsAreMappedWithJacksonArguments() {
    int amount = ThreadLocalRandom.current().nextInt(1, 10_000);
    String id = "call-" + UUID.randomUUID();
    AiMessage msg = AiMessage.from("thinking", List.of(
        ToolExecutionRequest.builder().id(id).name("transfer")
            .arguments("{\"amount\": " + amount + ", \"to\": {\"iban\": \"DE{1}\", \"note\": \"a \\\"quoted\\\" }brace\"}, \"tags\": [\"x\", \"y\"]}")
            .build(),
        ToolExecutionRequest.builder().id("").name("noargs").arguments("").build(),
        ToolExecutionRequest.builder().id("bad").name("broken").arguments("{not json").build()));

    List<ChatToolCall> calls = LangChain4jChatConnection.toolCallsOf(msg);
    assertEquals(2, calls.size(), "malformed structured arguments are dropped, not guessed");
    assertEquals(id, calls.get(0).getId());
    assertEquals("transfer", calls.get(0).getName());
    assertEquals(amount, calls.get(0).getArguments().get("amount"));
    assertEquals(Map.of("iban", "DE{1}", "note", "a \"quoted\" }brace"), calls.get(0).getArguments().get("to"));
    assertEquals(List.of("x", "y"), calls.get(0).getArguments().get("tags"));
    assertEquals("call_1", calls.get(1).getId(), "blank ids get a positional id");
    assertEquals(Map.of(), calls.get(1).getArguments());

    List<ToolCall> mapped = LLMClient.fromStructured(calls);
    assertEquals(id, mapped.get(0).getToolCallId());
    assertEquals(amount, mapped.get(0).getParameters().get("amount"));
  }

  @Test
  void textFallbackHandlesNestedAndQuotedJsonAndKeyValueForm() {
    int n = ThreadLocalRandom.current().nextInt(1, 1000);
    String text = "Sure.\nTOOL_CALL: lookup {\"q\": \"a } b\", \"filter\": {\"n\": " + n + ", \"deep\": {\"k\": [1, {\"z\": 2}]}}}\n"
        + "and TOOL_CALL: second {\"ok\": true} done";
    List<ToolCall> calls = LLMClient.parseToolCallsFromText(text);
    assertEquals(2, calls.size());
    assertEquals("lookup", calls.get(0).getToolName());
    assertEquals("a } b", calls.get(0).getParameters().get("q"));
    assertEquals(Map.of("n", n, "deep", Map.of("k", List.of(1, Map.of("z", 2)))), calls.get(0).getParameters().get("filter"));
    assertEquals(Map.of("ok", true), calls.get(1).getParameters());

    assertTrue(LLMClient.parseToolCallsFromText("TOOL_CALL: broken {\"a\": }").isEmpty(), "invalid JSON is skipped");
    assertTrue(LLMClient.parseToolCallsFromText("TOOL_CALL: open {\"a\": 1").isEmpty(), "unterminated JSON is skipped");
    assertTrue(LLMClient.parseToolCallsFromText("no tools here").isEmpty());

    List<ToolCall> kv = LLMClient.parseToolCallsFromText("TOOL_CALL: calc(a=" + n + ", b=2.5, name=\"bob\")");
    assertEquals(1, kv.size());
    assertEquals(Map.of("a", n, "b", 2.5, "name", "bob"), kv.get(0).getParameters());
  }

  @Test
  void structuredCallsTakePrecedenceOverTextProtocol() {
    ChatResponse r = new ChatResponse("TOOL_CALL: fromtext {\"x\": 1}", "m",
        List.of(new ChatToolCall("id1", "structured", Map.of("y", 2))), 1L, ChatResponse.FinishReason.TOOL_CALLS);
    LLMClient client = LLMClient.builder().withModel("m").build(new AgentExecutorTest.ScriptedConnection(List.of(r)));
    LLMResponse out = client.chat(List.of(Map.of("role", "user", "content", "hi")));
    assertEquals(1, out.getToolCalls().size());
    assertEquals("structured", out.getToolCalls().get(0).getToolName());

    LLMClient textOnly = LLMClient.builder().withModel("m").build(
        new AgentExecutorTest.ScriptedConnection(List.of(AgentExecutorTest.text("TOOL_CALL: fromtext {\"x\": 1}"))));
    assertEquals("fromtext", textOnly.chat(List.of(Map.of("role", "user", "content", "hi"))).getToolCalls().get(0).getToolName());
  }

  /** Captures what the model actually receives. */
  static final class CapturingConnection implements ChatConnection {
    private static final long serialVersionUID = 1L;
    static final AtomicReference<List<ChatMessage>> LAST = new AtomicReference<>();

    @Override
    public ChatClient bind(RuntimeContext rc) {
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          LAST.set(new ArrayList<>(messages));
          return AgentExecutorTest.text("ok");
        }

        @Override
        public String providerName() {
          return "capturing";
        }

        @Override
        public void close() {}
      };
    }
  }

  static final class RewriteLastUser implements Guardrail {
    private static final long serialVersionUID = 1L;
    final String replacement;

    RewriteLastUser(String replacement) {
      this.replacement = replacement;
    }

    @Override
    public GuardrailDecision beforeChat(String agentId, List<ChatMessage> messages) {
      return GuardrailDecision.rewrite(replacement, "pii", "rewriter");
    }
  }

  @Test
  void guardrailRewriteReplacesOnlyTheLatestUserMessage() {
    String replacement = "redacted-" + UUID.randomUUID();
    List<String> rewrites = new ArrayList<>();
    LLMClient client = LLMClient.builder().withModel("m").build(new CapturingConnection())
        .withGuardrails(List.of(new RewriteLastUser(replacement)), "agent-x", new AgentEventListener() {
          @Override
          public void onGuardrailRewrite(String agentId, String modelName, String reason) {
            rewrites.add(agentId + "/" + modelName + "/" + reason);
          }
        });

    List<Map<String, Object>> history = List.of(
        Map.of("role", "system", "content", "sys"),
        Map.of("role", "user", "content", "first"),
        Map.of("role", "assistant", "content", "reply"),
        Map.of("role", "tool", "content", "42", "toolCallId", "c1", "toolName", "calc"),
        Map.of("role", "user", "content", "my ssn is 123"));
    client.chat(history);

    List<ChatMessage> sent = CapturingConnection.LAST.get();
    assertNotNull(sent);
    assertEquals(5, sent.size(), "history preserved");
    assertEquals(ChatRole.SYSTEM, sent.get(0).getRole());
    assertEquals("first", sent.get(1).getContent());
    assertEquals("reply", sent.get(2).getContent());
    assertEquals(ChatRole.TOOL, sent.get(3).getRole());
    assertEquals(replacement, sent.get(4).getContent());
    assertEquals(List.of("agent-x/rewriter/pii"), rewrites);

    List<ChatMessage> noUser = LLMClient.replaceLastUserMessage(List.of(ChatMessage.system("s")), "u");
    assertEquals(2, noUser.size());
    assertEquals("u", noUser.get(1).getContent());
  }

  @Test
  void listenerIsReResolvedAfterDeserialization() throws Exception {
    AgentEventListener custom = new AgentEventListener() {};
    LLMClient client = LLMClient.builder().withModel("m").build(new CapturingConnection())
        .withGuardrails(List.of(new RewriteLastUser("x")), "a", custom);
    assertSame(custom, client.getListener());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(client);
    }
    LLMClient restored;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      restored = (LLMClient) ois.readObject();
    }
    assertNotNull(restored.getListener(), "transient listener is re-resolved, not left null");
    assertEquals("m", restored.getModelName());
    LLMResponse out = restored.chat(List.of(Map.of("role", "user", "content", "hello")));
    assertEquals("ok", out.getText());
    assertEquals("x", CapturingConnection.LAST.get().get(0).getContent(), "guardrails survive the round trip");
  }
}

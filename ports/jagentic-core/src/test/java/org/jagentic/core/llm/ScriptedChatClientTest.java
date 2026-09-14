package org.jagentic.core.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;
import org.junit.jupiter.api.Test;

/**
 * The spec's deterministic {@code llm.provider: stub} through the real {@link LlmBrain} loop:
 * {@code llm.script} replayed from the top on every turn, structured args recorded as written,
 * the {@code text} step returned verbatim, and undeclared tools refused (primitives.md, section 8).
 */
class ScriptedChatClientTest {

  private static String rnd() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  private static Map<String, Object> workflow(List<Map<String, Object>> script, Map<String, Object> llmPath,
                                              List<Map<String, Object>> tools) {
    Map<String, Object> llm = new LinkedHashMap<>();
    llm.put("provider", "stub");
    llm.put("script", script);
    Map<String, Object> general = new LinkedHashMap<>();
    general.put("brain", "rule");
    general.put("prompt", "You answer general questions.");
    Map<String, Object> paths = new LinkedHashMap<>();
    paths.put("payments", llmPath);
    paths.put("general", general);
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "a-" + rnd());
    agent.put("router", Map.of("kind", "keyword", "default", "general", "rules", Map.of("payments", List.of("balance"))));
    agent.put("paths", paths);
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("spec_version", "agentic/v1");
    s.put("llm", llm);
    s.put("agent", agent);
    s.put("tools", tools);
    return s;
  }

  private static LocalRuntime runtime(Map<String, Object> spec) {
    GraphBuilder.Built built = GraphBuilder.build(spec, null);
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), built.retriever(), new ConversationLog.InMemory());
  }

  private static Map<String, Object> llmPath(List<String> tools) {
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("brain", "llm");
    p.put("prompt", "You are a payments specialist.");
    if (tools != null) {
      p.put("tools", tools);
    }
    return p;
  }

  @Test
  void replaysTheScriptFromTheTopOnEveryTurnAndAnswersVerbatim() {
    String account = "acct-" + ThreadLocalRandom.current().nextInt(1000, 9999);
    String answer = "Your balance is " + ThreadLocalRandom.current().nextInt(1, 100_000) + " USD.";
    Map<String, Object> args = new LinkedHashMap<>();
    args.put("account", account);
    args.put("currency", "USD");
    args.put("nested", Map.of("limit", 3));
    List<Map<String, Object>> script = List.of(
        Map.of("tool", "get_balance", "args", args),
        Map.of("text", answer),
        Map.of("text", "never reached"));
    Map<String, Object> spec = workflow(script, llmPath(null),
        List.of(Map.of("id", "get_balance", "kind", "constant", "value", "1234.56")));
    LocalRuntime runtime = runtime(spec);

    String cid = "c-" + rnd();
    for (int turn = 1; turn <= 3; turn++) {
      TurnResult res = runtime.submit(Event.turn(cid, "t" + turn, "u", "what is my balance?"));
      assertEquals(TurnStatus.COMPLETED, res.status, "turn " + turn + ": " + res.error);
      assertEquals("payments", res.path);
      assertEquals(answer, res.reply, "reply is the text step verbatim, no [path] prefix");
      assertEquals(1, res.calls.size(), "one scripted tool call per turn");
      assertEquals("get_balance", res.calls.get(0).tool());
      assertEquals(0, res.calls.get(0).index());
      assertEquals(1, res.calls.get(0).attempt());
      assertEquals(args, res.calls.get(0).args(), "structured args recorded exactly as scripted");
      assertEquals(List.copyOf(args.keySet()), new ArrayList<>(res.calls.get(0).args().keySet()));
      assertNull(res.error);
    }

    TurnResult general = runtime.submit(Event.turn(cid, "t4", "u", "hello there"));
    assertEquals("general", general.path);
    assertTrue(general.reply.startsWith("[general] "), general.reply);
    assertTrue(general.calls.isEmpty());
  }

  @Test
  void textOnlyScriptCallsNoTool() {
    String answer = "Hi " + rnd();
    LocalRuntime runtime = runtime(workflow(List.of(Map.of("text", answer)), llmPath(List.of("get_balance")),
        List.of(Map.of("id", "get_balance", "kind", "constant", "value", "1"))));
    TurnResult res = runtime.submit(Event.turn("c-" + rnd(), "t1", "u", "balance please"));
    assertEquals(TurnStatus.COMPLETED, res.status);
    assertEquals(answer, res.reply);
    assertTrue(res.calls.isEmpty());
  }

  @Test
  void toolNotDeclaredOnThePathFailsTheTurnAsValidation() {
    String hidden = "transfer_" + rnd();
    List<Map<String, Object>> script = List.of(
        Map.of("tool", hidden, "args", Map.of("amount", 500)),
        Map.of("text", "Done."));
    Map<String, Object> spec = workflow(script, llmPath(List.of("get_balance")), List.of(
        Map.of("id", "get_balance", "kind", "constant", "value", "1"),
        Map.of("id", hidden, "kind", "constant", "value", "moved")));
    TurnResult res = runtime(spec).submit(Event.turn("c-" + rnd(), "t1", "u", "balance"));
    assertEquals(TurnStatus.FAILED, res.status);
    assertEquals("validation", res.error.errorClass().wire());
    assertTrue(res.error.message().contains(hidden), res.error.message());
    assertTrue(res.calls.isEmpty(), "the undeclared tool never ran");
    assertNull(res.reply);
  }

  @Test
  void scriptWithoutAFinalTextIsAFatalTurnNotAnInventedAnswer() {
    Map<String, Object> spec = workflow(List.of(Map.of("tool", "get_balance", "args", Map.of())), llmPath(null),
        List.of(Map.of("id", "get_balance", "kind", "constant", "value", "1")));
    TurnResult res = runtime(spec).submit(Event.turn("c-" + rnd(), "t1", "u", "balance"));
    assertEquals(TurnStatus.FAILED, res.status);
    assertEquals("fatal", res.error.errorClass().wire());
    assertEquals(1, res.calls.size(), "the scripted tool step still ran");
  }

  @Test
  void emptyScriptIsRejectedAtBuildTime() {
    Map<String, Object> spec = workflow(List.of(), llmPath(null), List.of());
    assertThrows(RuntimeException.class, () -> GraphBuilder.build(spec, null));
  }

  @Test
  void stubProviderNeedsNoFactoryAndOtherProvidersStillDo() {
    Map<String, Object> spec = workflow(List.of(Map.of("text", "ok")), llmPath(null), List.of());
    assertTrue(GraphBuilder.usesScriptedLlm(spec));
    GraphBuilder.build(spec, llm -> {
      throw new AssertionError("factory must not be consulted for provider stub");
    });

    Map<String, Object> real = new LinkedHashMap<>(spec);
    real.put("llm", Map.of("provider", "openai", "model", "m-" + rnd(), "base_url", "http://127.0.0.1:1"));
    assertFalse(GraphBuilder.usesScriptedLlm(real));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> GraphBuilder.build(real, null));
    assertTrue(e.getMessage().contains("ChatClientFactory"), e.getMessage());
  }

  @Test
  void positionIsReadFromTheTranscriptSoTheClientIsStateless() {
    ScriptedChatClient client = new ScriptedChatClient(List.of(
        ChatResult.toolCall("a", Map.of("k", 1)),
        ChatResult.toolCall("b", Map.of()),
        ChatResult.text("final")));
    List<Map<String, String>> messages = new ArrayList<>();
    messages.add(Map.of("role", "system", "content", "s"));
    messages.add(Map.of("role", "user", "content", "old turn"));
    messages.add(Map.of("role", "tool", "content", "leftover observation"));
    messages.add(Map.of("role", "assistant", "content", "old reply"));
    messages.add(Map.of("role", "user", "content", "new turn"));
    assertEquals("a", client.chat(messages, List.of()).tool());
    messages.add(Map.of("role", "assistant", "content", "{\"tool\":\"a\"}"));
    messages.add(Map.of("role", "tool", "content", "obs"));
    assertEquals("b", client.chat(messages, List.of()).tool());
    messages.add(Map.of("role", "assistant", "content", "{\"tool\":\"b\"}"));
    messages.add(Map.of("role", "tool", "content", "obs"));
    assertEquals("final", client.chat(messages, List.of()).text());
    messages.add(Map.of("role", "tool", "content", "one too many"));
    assertThrows(ScriptedChatClient.ScriptExhausted.class, () -> client.chat(messages, List.of()));
  }
}

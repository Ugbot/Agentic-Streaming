package org.jagentic.core.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.TransientFailure;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.Backends;
import org.jagentic.core.pipeline.GraphBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** A model outage is transient; a request the provider refuses is not, and retrying won't help. */
class ModelFailureClassificationTest {

  private static Map<String, Object> workflow(Map<String, Object> policies) {
    Map<String, Object> path = new LinkedHashMap<>();
    path.put("brain", "llm");
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "a");
    agent.put("router", Map.of("kind", "keyword", "default", "main"));
    agent.put("paths", Map.of("main", path));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> wf = new LinkedHashMap<>();
    wf.put("spec_version", "agentic/v1");
    wf.put("agent", agent);
    wf.put("llm", Map.of("provider", "scripted-by-test"));
    if (policies != null) {
      wf.put("policies", policies);
    }
    return wf;
  }

  private static Runtime runtime(Map<String, Object> wf, ChatClient client) {
    return Backends.create("local", GraphBuilder.build(wf, spec -> client),
        new ConversationStore.InMemory());
  }

  private static Map<String, Object> retry(int attempts) {
    return Map.of("retry", Map.of("kind", "fixed", "max_attempts", attempts,
        "initial_delay_ms", 0, "max_delay_ms", 0));
  }

  private static ChatClient failing(AtomicInteger calls, int failures, RuntimeException error) {
    return (messages, tools) -> {
      if (calls.incrementAndGet() <= failures) {
        throw error;
      }
      return ChatResult.text("hello");
    };
  }

  @Test
  void aTransientFailureIsRetriedInsideTheTurn() {
    AtomicInteger calls = new AtomicInteger();
    Runtime rt = runtime(workflow(retry(3)), failing(calls, 2, new TransientFailure("blip")));
    TurnResult r = rt.submit(Event.turn("c", "t", "u", "hi"));
    assertEquals(TurnStatus.COMPLETED, r.status);
    assertEquals(3, calls.get());
  }

  @Test
  void aTransientFailureOutlastingTheBudgetDoesNotSpendTheTurnId() {
    AtomicInteger calls = new AtomicInteger();
    Runtime rt = runtime(workflow(null), failing(calls, 1, new TransientFailure("blip")));
    TurnResult first = rt.submit(Event.turn("c", "t", "u", "hi"));
    assertEquals(TurnStatus.FAILED, first.status);
    assertEquals("transient", first.error.errorClass().wire());
    TurnResult second = rt.submit(Event.turn("c", "t", "u", "hi"));
    assertEquals(TurnStatus.COMPLETED, second.status);
    assertEquals(1L, ((Number) second.state.get("turn_count")).longValue());
    assertEquals(TurnStatus.DUPLICATE, rt.submit(Event.turn("c", "t", "u", "hi")).status);
  }

  @Test
  void withoutTurnIdempotencyARedeliveryIsSimplyANewTurn() {
    AtomicInteger calls = new AtomicInteger();
    Runtime rt = runtime(workflow(Map.of("idempotency", "none")),
        failing(calls, 1, new TransientFailure("blip")));
    assertEquals(TurnStatus.FAILED, rt.submit(Event.turn("c", "t", "u", "hi")).status);
    TurnResult second = rt.submit(Event.turn("c", "t", "u", "hi"));
    assertEquals(TurnStatus.COMPLETED, second.status);
    assertEquals(2L, ((Number) second.state.get("turn_count")).longValue());
  }

  @Test
  void anyOtherModelFailureIsFatalAndIsNotRetried() {
    AtomicInteger calls = new AtomicInteger();
    Runtime rt = runtime(workflow(retry(3)), failing(calls, 99, new IllegalStateException("bug")));
    TurnResult r = rt.submit(Event.turn("c", "t", "u", "hi"));
    assertEquals("fatal", r.error.errorClass().wire());
    assertEquals(1, calls.get());
    assertEquals(TurnStatus.DUPLICATE, rt.submit(Event.turn("c", "t", "u", "hi")).status);
  }

  @Test
  void aResumedTurnThatFailsTransientlyRunsAgainOnItsSignal() {
    Map<String, Object> wf = workflow(null);
    @SuppressWarnings("unchecked")
    Map<String, Object> agent = (Map<String, Object>) wf.get("agent");
    @SuppressWarnings("unchecked")
    Map<String, Object> path = (Map<String, Object>) ((Map<String, Object>) agent.get("paths")).get("main");
    path.put("x-suspend-until", "approval");
    AtomicInteger calls = new AtomicInteger();
    List<String> seen = new ArrayList<>();
    ChatClient client = (messages, tools) -> {
      seen.add(messages.get(messages.size() - 1).get("content"));
      if (calls.incrementAndGet() == 1) {
        throw new TransientFailure("blip");
      }
      return ChatResult.text("hello");
    };
    Runtime rt = runtime(wf, client);
    String cid = "c-" + UUID.randomUUID();
    assertEquals(TurnStatus.SUSPENDED, rt.submit(Event.turn(cid, "t", "u", "refund")).status);
    assertEquals(TurnStatus.FAILED, rt.submit(Event.resume(cid, "t", Map.of("ok", true))).status);
    TurnResult again = rt.submit(Event.resume(cid, "t", Map.of("ok", true)));
    assertEquals(TurnStatus.COMPLETED, again.status);
    assertEquals(List.of("refund", "refund"), seen, "the original text, not the signal's");
  }


  private static RuntimeException chatAgainst(int status) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/api/chat", e -> {
      e.getRequestBody().readAllBytes();
      byte[] body = "{\"error\":\"nope\"}".getBytes(StandardCharsets.UTF_8);
      e.sendResponseHeaders(status, body.length);
      e.getResponseBody().write(body);
      e.close();
    });
    server.start();
    try {
      OllamaChatClient client = new OllamaChatClient("m",
          "http://127.0.0.1:" + server.getAddress().getPort());
      return assertThrows(RuntimeException.class,
          () -> client.chat(List.of(Map.of("role", "user", "content", "hi")), List.of()));
    } finally {
      server.stop(0);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 425, 429, 500, 502, 503, 504, 529})
  void aProviderOutageIsTransient(int status) throws IOException {
    assertTrue(chatAgainst(status) instanceof TransientFailure, "HTTP " + status);
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404, 422})
  void aRefusedRequestIsNotTransient(int status) throws IOException {
    assertFalse(chatAgainst(status) instanceof TransientFailure, "HTTP " + status);
  }

  @Test
  void anUnreachableProviderIsTransient() {
    OllamaChatClient client = new OllamaChatClient("m", "http://127.0.0.1:1");
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> client.chat(List.of(Map.of("role", "user", "content", "hi")), List.of()));
    assertTrue(e instanceof TransientFailure, e.toString());
  }
}

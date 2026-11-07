package org.jagentic.core.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.jagentic.core.Deadlines;
import org.jagentic.core.Event;
import org.jagentic.core.TransientFailure;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.embedding.Embedder;
import org.jagentic.core.llm.ChatResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Every tool and model call is bounded: a hung dependency ends an attempt, never a thread. */
class CallDeadlineTest {

  private HttpServer server;
  private final CountDownLatch release = new CountDownLatch(1);

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
    server.createContext("/hang", e -> {
      awaitRelease();
      e.close();
    });
    server.createContext("/trickle", e -> {
      e.sendResponseHeaders(200, 0);
      e.getResponseBody().write('{');
      e.getResponseBody().flush();
      awaitRelease();
      e.close();
    });
    server.createContext("/boom", e -> reply(e, 500, "{\"error\":\"insufficient funds\"}"));
    server.createContext("/ok", e -> reply(e, 200, "{\"ok\":true}"));
    server.createContext("/plain", e -> reply(e, 200, "not json"));
    server.start();
  }

  @AfterEach
  void stop() {
    release.countDown();
    server.stop(0);
  }

  private void awaitRelease() {
    try {
      release.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void reply(com.sun.net.httpserver.HttpExchange e, int code, String body)
      throws IOException {
    e.getRequestBody().readAllBytes();
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    e.sendResponseHeaders(code, bytes.length);
    e.getResponseBody().write(bytes);
    e.close();
  }

  private String url(String path) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + path;
  }

  private static Map<String, Object> workflow(Map<String, Object> tool) {
    Map<String, Object> path = new LinkedHashMap<>();
    path.put("brain", "rule");
    path.put("tool_triggers", Map.of("go", "call"));
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "a");
    agent.put("router", Map.of("kind", "keyword", "default", "main"));
    agent.put("paths", Map.of("main", path));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> wf = new LinkedHashMap<>();
    wf.put("spec_version", "agentic/v1");
    wf.put("agent", agent);
    wf.put("tools", List.of(tool));
    return wf;
  }

  private static Map<String, Object> httpTool(String url, int timeoutMs) {
    Map<String, Object> t = new LinkedHashMap<>();
    t.put("id", "call");
    t.put("kind", "http");
    t.put("url", url);
    t.put("timeout_ms", timeoutMs);
    return t;
  }

  private static TurnResult run(Map<String, Object> wf) {
    return PipelineLoader.buildSystem(wf, "local")
        .submit(Event.turn("c-" + UUID.randomUUID(), "t-" + UUID.randomUUID(), "u", "go"));
  }

  @Test
  void aHttpToolThatNeverAnswersFailsTheAttempt() {
    TurnResult r = assertTimeoutPreemptively(Duration.ofSeconds(5),
        () -> run(workflow(httpTool(url("/hang"), 100))));
    assertEquals(TurnStatus.FAILED, r.status);
    assertEquals("tool", r.error.errorClass().wire());
    assertTrue(r.error.message().contains("timed out"), r.error.message());
  }

  @Test
  void aHttpToolThatStallsMidBodyFailsTheAttempt() {
    TurnResult r = assertTimeoutPreemptively(Duration.ofSeconds(5),
        () -> run(workflow(httpTool(url("/trickle"), 100))));
    assertEquals(TurnStatus.FAILED, r.status);
    assertTrue(r.error.message().contains("timed out"), r.error.message());
  }

  @Test
  void aHttpToolAnsweringWithAServerErrorIsAToolFailure() {
    TurnResult r = run(workflow(httpTool(url("/boom"), 2_000)));
    assertEquals(TurnStatus.FAILED, r.status);
    assertEquals("tool", r.error.errorClass().wire());
    assertTrue(r.error.message().contains("500"), r.error.message());
  }

  @Test
  void aHttpToolStillReturnsItsBody() {
    TurnResult r = run(workflow(httpTool(url("/ok"), 2_000)));
    assertEquals(TurnStatus.COMPLETED, r.status);
    assertEquals(Map.of("ok", true), r.calls.get(0).result());
    TurnResult plain = run(workflow(httpTool(url("/plain"), 2_000)));
    assertEquals(TurnStatus.COMPLETED, plain.status, "a text body is a result, as in Python");
    assertEquals("not json", plain.calls.get(0).result());
  }

  @Test
  void aModelThatNeverAnswersFailsTheTurn() {
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
    wf.put("llm", Map.of("provider", "stuck", "timeout_ms", 100));
    List<Object> unused = new ArrayList<>();
    GraphBuilder.Built built = GraphBuilder.build(wf, spec -> (messages, tools) -> {
      awaitRelease();
      unused.add(messages);
      return ChatResult.text("late");
    });
    var runtime = org.jagentic.core.pipeline.Backends.create("local", built,
        new org.jagentic.core.ConversationStore.InMemory());
    TurnResult r = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> runtime.submit(
        Event.turn("c-" + UUID.randomUUID(), "t-" + UUID.randomUUID(), "u", "hello")));
    assertEquals(TurnStatus.FAILED, r.status);
    assertTrue(r.error.message().contains("timed out"), r.error.message());
  }

  @Test
  void anEmbeddingModelThatNeverAnswersIsTransientAtItsLimit() {
    Embedder stuck = new Embedder() {
      @Override
      public float[] embed(String text) {
        awaitRelease();
        return new float[] {1f};
      }

      @Override
      public int dim() {
        return 1;
      }
    };
    Embedder bounded = Deadlines.bound("embedding call", 100, stuck);
    assertEquals(1, bounded.dim());
    TransientFailure e = assertTimeoutPreemptively(Duration.ofSeconds(5),
        () -> assertThrows(TransientFailure.class, () -> bounded.embed("hello")));
    assertTrue(e.getMessage().contains("timed out"), e.getMessage());
  }

  @Test
  void aDeclaredEmbeddingProviderIsBuiltWithItsLimit() {
    Map<String, Object> wf = new LinkedHashMap<>(workflow(httpTool(url("/ok"), 2_000)));
    wf.put("embeddings", Map.of("provider", "ollama", "base_url", url("/hang"), "timeout_ms", 100));
    TransientFailure e = assertTimeoutPreemptively(Duration.ofSeconds(5),
        () -> assertThrows(TransientFailure.class, () -> GraphBuilder.build(wf, null)));
    assertTrue(e.getMessage().contains("timed out"), e.getMessage());
  }
}

package org.agentic.flink.a2a.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.AuthSpec;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2AJsonTypeInfo;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.a2a.bridge.InProcA2ABridge;
import org.agentic.flink.a2a.storage.InMemoryA2ATaskStore;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P5 tests: {@code message/stream} SSE (ordered working→completed events for one task) and the
 * push-notification dispatcher (terminal Task POSTed to a registered webhook with auth + token).
 * The resource's {@code @Inject} fields are package-private, so we wire them directly (no Quarkus
 * boot); the SSE {@link io.smallrye.mutiny.Multi} is subscribed to in-process.
 */
final class A2AStreamingTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private JobClient job;

  @AfterEach
  void tearDown() throws Exception {
    if (job != null) {
      job.cancel().get();
      job = null;
    }
    InProcA2ABridge.Hub.reset();
  }

  @Test
  @DisplayName("message/stream emits ordered SSE events: working… then a final completed status")
  void streamEmitsWorkingThenCompleted() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      // Job emits a working update then a completed artifact per request.
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
      bridge
          .requestChannel()
          .open(env)
          .flatMap(new WorkingThenDone())
          .returns(A2AJsonTypeInfo.of(A2AResponse.class))
          .sinkTo(bridge.responseSink());
      job = env.executeAsync("stream-working-then-done");
      Thread.sleep(300);

      A2AResource resource = new A2AResource();
      resource.config = new GatewayConfig();
      resource.connector = connector;
      resource.taskStore = newStore();

      List<String> events = new CopyOnWriteArrayList<>();
      CountDownLatch done = new CountDownLatch(1);
      AtomicReference<Throwable> err = new AtomicReference<>();
      resource
          .rpcStream(streamBody("stream me", "ctx-" + UUID.randomUUID()), new A2AResourceLifecycleTest.FakeHeaders(null))
          .subscribe()
          .with(events::add, t -> { err.set(t); done.countDown(); }, done::countDown);

      assertTrue(done.await(20, TimeUnit.SECONDS), "stream did not complete");
      assertEquals(null, err.get());
      assertTrue(events.size() >= 2, "expected >=2 SSE events, got " + events);

      JsonNode first = JSON.readTree(events.get(0));
      assertEquals("working", first.path("status").path("state").asText());
      assertEquals(false, first.path("final").asBoolean());

      JsonNode last = JSON.readTree(events.get(events.size() - 1));
      assertEquals("completed", last.path("status").path("state").asText());
      assertTrue(last.path("final").asBoolean());
      assertTrue(last.path("artifacts").get(0).path("text").asText().startsWith("done:"));
    }
  }

  @Test
  @DisplayName("push dispatcher POSTs the terminal Task to the registered webhook with auth + token")
  void pushDispatcherPostsTerminalTask() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    List<String> bodies = new CopyOnWriteArrayList<>();
    AtomicReference<String> authHeader = new AtomicReference<>();
    AtomicReference<String> tokenHeader = new AtomicReference<>();
    CountDownLatch hit = new CountDownLatch(1);
    server.createContext(
        "/hook",
        ex -> {
          authHeader.set(ex.getRequestHeaders().getFirst("X-Key"));
          tokenHeader.set(ex.getRequestHeaders().getFirst("X-A2A-Notification-Token"));
          bodies.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          ex.sendResponseHeaders(200, -1);
          ex.close();
          hit.countDown();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      InMemoryA2ATaskStore store = newStore();
      String taskId = "task-" + UUID.randomUUID();
      store.savePushConfig(
          taskId,
          new A2APushConfig(
              "cfg1", "http://127.0.0.1:" + port + "/hook", "tok-xyz",
              AuthSpec.apiKey("X-Key", "secret")));

      PushDispatcher dispatcher = new PushDispatcher();
      dispatcher.taskStore = store;
      // Drive a terminal response directly (bypassing the StartupEvent registration).
      dispatcher.onResponse(
          A2AResponse.completed(
              taskId, "ctx-1", List.of(A2AArtifact.text(UUID.randomUUID().toString(), "r", "the result"))));

      assertTrue(hit.await(10, TimeUnit.SECONDS), "webhook was not called");
      assertEquals("secret", authHeader.get(), "AuthSpec header must be sent");
      assertEquals("tok-xyz", tokenHeader.get(), "notification token must be sent");
      JsonNode task = JSON.readTree(bodies.get(0));
      assertEquals("task", task.path("kind").asText());
      assertEquals(taskId, task.path("id").asText());
      assertEquals("completed", task.path("status").path("state").asText());
      assertEquals("the result", task.path("artifacts").get(0).path("text").asText());
    } finally {
      server.stop(0);
    }
  }

  private static InMemoryA2ATaskStore newStore() throws Exception {
    InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
    store.initialize(java.util.Map.of());
    return store;
  }

  private static String streamBody(String text, String contextId) throws Exception {
    var root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.put("id", "1");
    root.put("method", "message/stream");
    var msg = root.putObject("params").putObject("message");
    msg.put("contextId", contextId);
    msg.putArray("parts").addObject().put("kind", "text").put("text", text);
    return JSON.writeValueAsString(root);
  }

  /** Emits a working update then a completed artifact for each request. */
  static final class WorkingThenDone implements FlatMapFunction<A2ARequest, A2AResponse> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(A2ARequest req, Collector<A2AResponse> out) {
      out.collect(A2AResponse.working(req.getTaskId(), req.getContextId(), "thinking"));
      String text = req.getMessage() == null ? "" : req.getMessage().textContent();
      out.collect(
          A2AResponse.completed(
              req.getTaskId(),
              req.getContextId(),
              List.of(A2AArtifact.text(UUID.randomUUID().toString(), "echo", "done: " + text))));
    }
  }
}

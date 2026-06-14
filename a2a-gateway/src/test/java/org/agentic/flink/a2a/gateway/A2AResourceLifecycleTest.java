package org.agentic.flink.a2a.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APart;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2AJsonTypeInfo;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.a2a.bridge.InProcA2ABridge;
import org.agentic.flink.a2a.storage.A2ATaskStore;
import org.agentic.flink.a2a.storage.InMemoryA2ATaskStore;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Exercises the {@link A2AResource} JSON-RPC surface — message/send lifecycle persistence,
 * tasks/get, tasks/cancel, and Authorization→claims — against a real Flink echo job over the
 * in-process bridge. The resource's {@code @Inject} fields are package-private, so we wire them
 * directly (no Quarkus boot).
 */
final class A2AResourceLifecycleTest {

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

  /** A2AResource wired with an echo Flink job + in-memory task store. */
  private A2AResource resource(A2AGatewayConnector connector, A2ATaskStore store) {
    A2AResource r = new A2AResource();
    r.config = new GatewayConfig();
    r.connector = connector;
    r.taskStore = store;
    return r;
  }

  private void startEchoJob(InProcA2ABridge bridge) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
    DataStream<A2ARequest> requests = bridge.requestChannel().open(env);
    requests
        .map(new EchoResponder())
        .returns(A2AJsonTypeInfo.of(A2AResponse.class))
        .sinkTo(bridge.responseSink());
    job = env.executeAsync("resource-lifecycle-echo");
    Thread.sleep(300);
  }

  private static String sendBody(String text, String contextId, Object id) throws Exception {
    var root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.put("id", String.valueOf(id));
    root.put("method", "message/send");
    var msg = root.putObject("params").putObject("message");
    if (contextId != null) {
      msg.put("contextId", contextId);
    }
    var part = msg.putArray("parts").addObject();
    part.put("kind", "text");
    part.put("text", text);
    return JSON.writeValueAsString(root);
  }

  private static String taskMethodBody(String method, String taskId, Object id) throws Exception {
    var root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.put("id", String.valueOf(id));
    root.put("method", method);
    root.putObject("params").put("id", taskId);
    return JSON.writeValueAsString(root);
  }

  @Test
  @DisplayName("message/send returns the reply Message AND persists a COMPLETED task with an artifact")
  void messageSendPersistsLifecycle() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      startEchoJob(bridge);
      InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
      store.initialize(java.util.Map.of());
      A2AResource resource = resource(connector, store);

      String ctx = "ctx-" + UUID.randomUUID();
      String resp = resource.rpc(sendBody("ping " + UUID.randomUUID(), ctx, 1), new FakeHeaders(null));
      JsonNode node = JSON.readTree(resp);

      // The harness-facing result is still a Message echoing the reply.
      assertEquals("message", node.path("result").path("kind").asText());
      String reply = node.path("result").path("parts").get(0).path("text").asText();
      assertTrue(reply.startsWith("echo:"), "reply: " + reply);

      // And the task lifecycle was persisted: exactly one task for this context, COMPLETED, w/ artifact.
      List<A2ATask> tasks = store.listTasksByContext(ctx);
      assertEquals(1, tasks.size());
      A2ATask task = tasks.get(0);
      assertEquals(A2ATaskState.COMPLETED, task.getState());
      assertEquals(1, task.getArtifacts().size());
      assertTrue(task.getArtifacts().get(0).textContent().startsWith("echo:"));
    }
  }

  @Test
  @DisplayName("tasks/get returns the persisted Task envelope; tasks/cancel marks a live task CANCELED")
  void tasksGetAndCancel() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      startEchoJob(bridge);
      InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
      store.initialize(java.util.Map.of());
      A2AResource resource = resource(connector, store);

      String ctx = "ctx-" + UUID.randomUUID();
      resource.rpc(sendBody("hello", ctx, 1), new FakeHeaders(null));
      String taskId = store.listTasksByContext(ctx).get(0).getId();

      // tasks/get -> Task envelope in completed state.
      JsonNode got = JSON.readTree(resource.rpc(taskMethodBody("tasks/get", taskId, 2), new FakeHeaders(null)));
      assertEquals("task", got.path("result").path("kind").asText());
      assertEquals(taskId, got.path("result").path("id").asText());
      assertEquals("completed", got.path("result").path("status").path("state").asText());

      // tasks/get for an unknown id -> JSON-RPC error.
      JsonNode missing =
          JSON.readTree(resource.rpc(taskMethodBody("tasks/get", "nope-" + UUID.randomUUID(), 3), new FakeHeaders(null)));
      assertTrue(missing.has("error"));

      // tasks/cancel on a fresh WORKING task -> CANCELED.
      A2ATask working =
          A2ATask.submitted("live-" + UUID.randomUUID(), ctx, A2AMessage.userText(UUID.randomUUID().toString(), "x"), 0L)
              .withState(A2ATaskState.WORKING, null, 0L);
      store.saveTask(working);
      JsonNode canceled =
          JSON.readTree(resource.rpc(taskMethodBody("tasks/cancel", working.getId(), 4), new FakeHeaders(null)));
      assertEquals("canceled", canceled.path("result").path("status").path("state").asText());
      assertEquals(A2ATaskState.CANCELED, store.loadTask(working.getId()).orElseThrow().getState());
    }
  }

  @Test
  @DisplayName("an Authorization header is extracted into the request claims propagated to the job")
  void authorizationBecomesClaims() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      // A job that captures the claims it received and echoes the subject back as the artifact text.
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
      bridge
          .requestChannel()
          .open(env)
          .map(new ClaimsEchoResponder())
          .returns(A2AJsonTypeInfo.of(A2AResponse.class))
          .sinkTo(bridge.responseSink());
      job = env.executeAsync("resource-claims-echo");
      Thread.sleep(300);

      InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
      store.initialize(java.util.Map.of());
      A2AResource resource = resource(connector, store);

      String resp =
          resource.rpc(sendBody("who am i", "ctx-" + UUID.randomUUID(), 1), new FakeHeaders("Bearer tok-12345"));
      String reply = JSON.readTree(resp).path("result").path("parts").get(0).path("text").asText();
      assertEquals("token=tok-12345", reply, "the job must observe the Bearer token from claims");
    }
  }

  /** Echoes the inbound text as a COMPLETED artifact. */
  static final class EchoResponder implements MapFunction<A2ARequest, A2AResponse> {
    private static final long serialVersionUID = 1L;

    @Override
    public A2AResponse map(A2ARequest req) {
      String text = req.getMessage() == null ? "" : req.getMessage().textContent();
      A2AArtifact artifact =
          A2AArtifact.text(UUID.randomUUID().toString(), "echo", "echo: " + text);
      return A2AResponse.completed(req.getTaskId(), req.getContextId(), List.of(artifact));
    }
  }

  /** Reports back the Bearer token it received via the request claims. */
  static final class ClaimsEchoResponder implements MapFunction<A2ARequest, A2AResponse> {
    private static final long serialVersionUID = 1L;

    @Override
    public A2AResponse map(A2ARequest req) {
      Object token = req.getClaims() == null ? null : req.getClaims().get("token");
      A2AArtifact artifact =
          A2AArtifact.text(UUID.randomUUID().toString(), "claims", "token=" + token);
      return A2AResponse.completed(req.getTaskId(), req.getContextId(), List.of(artifact));
    }
  }

  /** Minimal HttpHeaders stub exposing a single Authorization header. */
  static final class FakeHeaders implements jakarta.ws.rs.core.HttpHeaders {
    private final String authorization;

    FakeHeaders(String authorization) {
      this.authorization = authorization;
    }

    @Override
    public String getHeaderString(String name) {
      return AUTHORIZATION.equalsIgnoreCase(name) ? authorization : null;
    }

    @Override
    public List<String> getRequestHeader(String name) {
      String v = getHeaderString(name);
      return v == null ? List.of() : List.of(v);
    }

    @Override
    public jakarta.ws.rs.core.MultivaluedMap<String, String> getRequestHeaders() {
      return new jakarta.ws.rs.core.MultivaluedHashMap<>();
    }

    @Override
    public List<jakarta.ws.rs.core.MediaType> getAcceptableMediaTypes() {
      return List.of();
    }

    @Override
    public List<java.util.Locale> getAcceptableLanguages() {
      return List.of();
    }

    @Override
    public jakarta.ws.rs.core.MediaType getMediaType() {
      return null;
    }

    @Override
    public java.util.Locale getLanguage() {
      return null;
    }

    @Override
    public java.util.Map<String, jakarta.ws.rs.core.Cookie> getCookies() {
      return java.util.Map.of();
    }

    @Override
    public java.util.Date getDate() {
      return null;
    }

    @Override
    public int getLength() {
      return -1;
    }
  }
}

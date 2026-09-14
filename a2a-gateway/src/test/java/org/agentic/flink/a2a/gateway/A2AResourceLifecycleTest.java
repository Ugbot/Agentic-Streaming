package org.agentic.flink.a2a.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
  /** Randomized per-run tokens for two distinct principals. */
  static final String ALICE_TOKEN = "tok-" + UUID.randomUUID();
  static final String BOB_TOKEN = "tok-" + UUID.randomUUID();
  static final FakeHeaders ALICE = new FakeHeaders("Bearer " + ALICE_TOKEN);
  static final FakeHeaders BOB = new FakeHeaders("Bearer " + BOB_TOKEN);

  static GatewayConfig authedConfig() {
    return new GatewayConfig(
        org.agentic.flink.config.AgenticFlinkConfig.fromMap(
            java.util.Map.of("a2a.auth.tokens", "alice=" + ALICE_TOKEN + ",bob=" + BOB_TOKEN)));
  }

  static A2AResource resource(A2AGatewayConnector connector, A2ATaskStore store) {
    return resource(connector, store, authedConfig());
  }

  static A2AResource resource(A2AGatewayConnector connector, A2ATaskStore store, GatewayConfig cfg) {
    A2AResource r = new A2AResource();
    r.config = cfg;
    r.connector = connector;
    r.taskStore = store;
    r.auth = new GatewayAuth(cfg);
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
      String resp = resource.rpc(sendBody("ping " + UUID.randomUUID(), ctx, 1), ALICE);
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
      resource.rpc(sendBody("hello", ctx, 1), ALICE);
      String taskId = store.listTasksByContext(ctx).get(0).getId();

      // tasks/get -> Task envelope in completed state.
      JsonNode got = JSON.readTree(resource.rpc(taskMethodBody("tasks/get", taskId, 2), ALICE));
      assertEquals("task", got.path("result").path("kind").asText());
      assertEquals(taskId, got.path("result").path("id").asText());
      assertEquals("completed", got.path("result").path("status").path("state").asText());

      // tasks/get for an unknown id -> JSON-RPC error.
      JsonNode missing =
          JSON.readTree(resource.rpc(taskMethodBody("tasks/get", "nope-" + UUID.randomUUID(), 3), ALICE));
      assertTrue(missing.has("error"));

      // tasks/cancel on a fresh WORKING task -> CANCELED.
      A2ATask working =
          A2AResource.ownedTask(
                  "live-" + UUID.randomUUID(), ctx, A2AMessage.userText(UUID.randomUUID().toString(), "x"), 0L,
                  new GatewayAuth(authedConfig()).authenticate("Bearer " + ALICE_TOKEN))
              .withState(A2ATaskState.WORKING, null, 0L);
      store.saveTask(working);
      JsonNode canceled =
          JSON.readTree(resource.rpc(taskMethodBody("tasks/cancel", working.getId(), 4), ALICE));
      assertEquals("canceled", canceled.path("result").path("status").path("state").asText());
      assertEquals(A2ATaskState.CANCELED, store.loadTask(working.getId()).orElseThrow().getState());
    }
  }

  @Test
  @DisplayName("the authenticated subject, not the raw token, is propagated to the job as claims")
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

      String resp = resource.rpc(sendBody("who am i", "ctx-" + UUID.randomUUID(), 1), BOB);
      String reply = JSON.readTree(resp).path("result").path("parts").get(0).path("text").asText();
      assertEquals("subject=bob token=null", reply, "the job must see the subject and never the token");
    }
  }

  @Test
  @DisplayName("missing, malformed or wrong bearer tokens are rejected with the unauthorized RPC error")
  void unauthenticatedCallsAreRejected() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
      store.initialize(java.util.Map.of());
      A2AResource resource = resource(connector, store);
      String ctx = "ctx-" + UUID.randomUUID();
      for (String header :
          new String[] {null, "", "Bearer", "Bearer ", "Basic " + ALICE_TOKEN, "Bearer wrong-" + UUID.randomUUID(), ALICE_TOKEN}) {
        JsonNode node = JSON.readTree(resource.rpc(sendBody("hi", ctx, 1), new FakeHeaders(header)));
        assertEquals(A2AResource.RPC_UNAUTHORIZED, node.path("error").path("code").asInt(), "header=" + header);
        JsonNode get = JSON.readTree(resource.rpc(taskMethodBody("tasks/get", "t-" + UUID.randomUUID(), 2), new FakeHeaders(header)));
        assertEquals(A2AResource.RPC_UNAUTHORIZED, get.path("error").path("code").asInt(), "header=" + header);
      }
      assertTrue(store.listTasksByContext(ctx).isEmpty(), "no task may be created by an unauthenticated caller");
    }
  }

  @Test
  @DisplayName("with no token configured the gateway fails closed unless dev mode is explicitly enabled")
  void failsClosedWithoutConfiguredToken() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      startEchoJob(bridge);
      InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
      store.initialize(java.util.Map.of());

      GatewayConfig closed = new GatewayConfig(org.agentic.flink.config.AgenticFlinkConfig.fromMap(java.util.Map.of()));
      org.junit.jupiter.api.Assumptions.assumeTrue(closed.authTokens().isEmpty(), "AGENTIC_A2A_TOKEN is set in this environment");
      A2AResource resource = resource(connector, store, closed);
      JsonNode node = JSON.readTree(resource.rpc(sendBody("hi", "ctx-" + UUID.randomUUID(), 1), new FakeHeaders("Bearer " + UUID.randomUUID())));
      assertEquals(A2AResource.RPC_UNAUTHORIZED, node.path("error").path("code").asInt());

      GatewayConfig dev = new GatewayConfig(org.agentic.flink.config.AgenticFlinkConfig.fromMap(java.util.Map.of("a2a.auth.dev.mode", "true")));
      A2AResource devResource = resource(connector, store, dev);
      String ctx = "ctx-" + UUID.randomUUID();
      JsonNode ok = JSON.readTree(devResource.rpc(sendBody("hi", ctx, 1), new FakeHeaders(null)));
      assertEquals("message", ok.path("result").path("kind").asText());
      assertEquals(GatewayAuth.DEV_SUBJECT, A2AResource.ownerOf(store.listTasksByContext(ctx).get(0)));
    }
  }

  @Test
  @DisplayName("tasks/get, tasks/cancel and pushNotificationConfig/* refuse tasks owned by another principal")
  void ownerMismatchIsForbidden() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      startEchoJob(bridge);
      InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
      store.initialize(java.util.Map.of());
      A2AResource resource = resource(connector, store);
      resource.setPushUrlPolicy(
          org.agentic.flink.net.OutboundUrlPolicy.defaults().withResolver(h -> new java.net.InetAddress[] {java.net.InetAddress.getByName("93.184.216.34")}));

      String ctx = "ctx-" + UUID.randomUUID();
      resource.rpc(sendBody("hello", ctx, 1), ALICE);
      A2ATask task = store.listTasksByContext(ctx).get(0);
      assertEquals("alice", A2AResource.ownerOf(task));
      String taskId = task.getId();

      // Owner can read.
      assertEquals("task", JSON.readTree(resource.rpc(taskMethodBody("tasks/get", taskId, 2), ALICE)).path("result").path("kind").asText());
      // Other principal gets forbidden on every task-addressed method.
      assertForbidden(resource.rpc(taskMethodBody("tasks/get", taskId, 3), BOB));
      assertForbidden(resource.rpc(taskMethodBody("tasks/cancel", taskId, 4), BOB));
      assertForbidden(resource.rpc(pushSetBody(taskId, "https://hooks.example.com/" + UUID.randomUUID(), 5), BOB));
      assertForbidden(resource.rpc(pushIdBody("tasks/pushNotificationConfig/list", taskId, null, 6), BOB));
      assertForbidden(resource.rpc(pushIdBody("tasks/pushNotificationConfig/get", taskId, "c1", 7), BOB));
      assertForbidden(resource.rpc(pushIdBody("tasks/pushNotificationConfig/delete", taskId, "c1", 8), BOB));
      // Unknown task ids are forbidden for push config too (no pre-registration on foreign ids).
      assertForbidden(resource.rpc(pushSetBody("nope-" + UUID.randomUUID(), "https://hooks.example.com/x", 9), BOB));
      assertEquals(A2ATaskState.COMPLETED, store.loadTask(taskId).orElseThrow().getState());

      // Owner can register a public webhook and list it back.
      JsonNode set = JSON.readTree(resource.rpc(pushSetBody(taskId, "https://hooks.example.com/" + UUID.randomUUID(), 10), ALICE));
      assertFalse(set.has("error"), set.toString());
      assertEquals(1, JSON.readTree(resource.rpc(pushIdBody("tasks/pushNotificationConfig/list", taskId, null, 11), ALICE)).path("result").size());

      // A legacy task without owner metadata is not readable by anyone.
      A2ATask legacy = A2ATask.submitted("legacy-" + UUID.randomUUID(), ctx, A2AMessage.userText(UUID.randomUUID().toString(), "x"), 0L);
      store.saveTask(legacy);
      assertForbidden(resource.rpc(taskMethodBody("tasks/get", legacy.getId(), 12), ALICE));
    }
  }

  @Test
  @DisplayName("push webhook URLs pointing at loopback, private, link-local or metadata addresses are rejected")
  void pushWebhookUrlsAreValidated() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      startEchoJob(bridge);
      InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
      store.initialize(java.util.Map.of());
      A2AResource resource = resource(connector, store);
      java.util.Random rnd = new java.util.Random();
      // Hostnames resolve to a random private address; literal IPs resolve to themselves.
      String privateIp = new String[] {"10.", "192.168.", "172.16."}[rnd.nextInt(3)] + rnd.nextInt(256) + "." + rnd.nextInt(1, 255);
      if (privateIp.startsWith("10.")) {
        privateIp = "10." + rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(1, 255);
      }
      final String resolved = privateIp;
      resource.setPushUrlPolicy(
          org.agentic.flink.net.OutboundUrlPolicy.defaults().withResolver(h ->
              h.equals("hooks.example.com")
                  ? new java.net.InetAddress[] {java.net.InetAddress.getByName("93.184.216.34")}
                  : new java.net.InetAddress[] {java.net.InetAddress.getByName(resolved)}));

      String ctx = "ctx-" + UUID.randomUUID();
      resource.rpc(sendBody("hello", ctx, 1), ALICE);
      String taskId = store.listTasksByContext(ctx).get(0).getId();

      String[] bad = {
        "http://127.0.0.1:8080/hook", "http://localhost/hook", "http://169.254.169.254/latest/meta-data",
        "http://" + resolved + "/hook", "http://internal.corp/hook", "ftp://hooks.example.com/x",
        "http://user:pw@hooks.example.com/x", "http://[::1]/hook", "http://0.0.0.0/hook",
      };
      int i = 2;
      for (String url : bad) {
        JsonNode node = JSON.readTree(resource.rpc(pushSetBody(taskId, url, i++), ALICE));
        assertEquals(-32602, node.path("error").path("code").asInt(), "url should be rejected: " + url);
      }
      assertTrue(store.listPushConfigs(taskId).isEmpty(), "no rejected webhook may be persisted");

      JsonNode ok = JSON.readTree(resource.rpc(pushSetBody(taskId, "https://hooks.example.com/ok", i), ALICE));
      assertFalse(ok.has("error"), ok.toString());
      assertEquals(1, store.listPushConfigs(taskId).size());
    }
  }

  private static void assertForbidden(String rpcResponse) throws Exception {
    JsonNode node = JSON.readTree(rpcResponse);
    assertEquals(A2AResource.RPC_FORBIDDEN, node.path("error").path("code").asInt(), rpcResponse);
  }

  private static String pushSetBody(String taskId, String url, Object id) throws Exception {
    var root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.put("id", String.valueOf(id));
    root.put("method", "tasks/pushNotificationConfig/set");
    var params = root.putObject("params");
    params.put("taskId", taskId);
    params.putObject("pushNotificationConfig").put("url", url);
    return JSON.writeValueAsString(root);
  }

  private static String pushIdBody(String method, String taskId, String configId, Object id) throws Exception {
    var root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.put("id", String.valueOf(id));
    root.put("method", method);
    var params = root.putObject("params");
    params.put("taskId", taskId);
    if (configId != null) {
      params.put("pushNotificationConfigId", configId);
    }
    return JSON.writeValueAsString(root);
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
      Object subject = req.getClaims() == null ? null : req.getClaims().get("subject");
      Object token = req.getClaims() == null ? null : req.getClaims().get("token");
      A2AArtifact artifact =
          A2AArtifact.text(UUID.randomUUID().toString(), "claims", "subject=" + subject + " token=" + token);
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

package org.agentic.flink.a2a.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APart;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.AuthSpec;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.a2a.storage.A2ATaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The A2A wire for the gateway, hand-rolled to the <b>spec</b> (not the a2a-java SDK's proto method
 * names): serves the Agent Card at {@code /.well-known/agent-card.json} and JSON-RPC at {@code /}.
 *
 * <p>Methods: {@code message/send} (each message becomes an {@link A2ARequest} published over the
 * A2A bridge to the embedded Flink job; {@link A2ARequestBridge} blocks for the verifier's response),
 * plus {@code tasks/get} and {@code tasks/cancel} served from the {@link A2ATaskStore}. The full task
 * lifecycle (submitted → working → completed/failed) is persisted to that store, and any
 * {@code Authorization} header is extracted into {@link A2ARequest#getClaims() claims} propagated to
 * the job.
 *
 * <p>{@code message/send} returns a JSON-RPC <b>Message</b> result (the reply text) — exactly what the
 * tau2 harness reads — while the Task envelope is available via {@code tasks/get}. Persistence is
 * best-effort: a task-store hiccup logs and never fails the live turn.
 */
@ApplicationScoped
@Path("/")
public class A2AResource {

  private static final Logger LOG = LoggerFactory.getLogger(A2AResource.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  @Inject GatewayConfig config;
  @Inject A2AGatewayConnector connector;
  @Inject A2ATaskStore taskStore;

  // ---- Agent Card ----

  @GET
  @Path("/.well-known/agent-card.json")
  @Produces(MediaType.APPLICATION_JSON)
  public String agentCard() throws Exception {
    ObjectNode card = JSON.createObjectNode();
    card.put("protocolVersion", config.protocolVersion());
    card.put("name", config.agentName());
    card.put("description", config.agentDescription());
    card.put("version", config.agentVersion());
    card.put("url", config.publicUrl());
    card.put("preferredTransport", "JSONRPC");
    ObjectNode caps = card.putObject("capabilities");
    caps.put("streaming", config.streamingEnabled());
    caps.put("pushNotifications", config.pushEnabled());
    card.putArray("defaultInputModes").add("text/plain");
    card.putArray("defaultOutputModes").add("text/plain");
    card.putArray("skills");
    return JSON.writeValueAsString(card);
  }

  // ---- JSON-RPC ----

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public String rpc(String body, @Context HttpHeaders headers) {
    JsonNode req;
    try {
      req = JSON.readTree(body);
    } catch (Exception e) {
      return rpcError(null, -32700, "Parse error");
    }
    JsonNode idNode = req.get("id");
    String method = req.path("method").asText("");
    try {
      switch (method) {
        case "message/send":
          return messageSend(idNode, req, headers);
        case "tasks/get":
          return tasksGet(idNode, req);
        case "tasks/cancel":
          return tasksCancel(idNode, req);
        case "tasks/pushNotificationConfig/set":
          return pushConfigSet(idNode, req);
        case "tasks/pushNotificationConfig/get":
          return pushConfigGet(idNode, req);
        case "tasks/pushNotificationConfig/list":
          return pushConfigList(idNode, req);
        case "tasks/pushNotificationConfig/delete":
          return pushConfigDelete(idNode, req);
        default:
          return rpcError(idNode, -32601, "Unsupported method: " + method);
      }
    } catch (Exception e) {
      LOG.warn("{} failed", method, e);
      return rpcError(idNode, -32603, "Internal error: " + e.getMessage());
    }
  }

  private String messageSend(JsonNode idNode, JsonNode req, HttpHeaders headers) throws Exception {
    JsonNode message = req.path("params").path("message");
    String contextId = message.path("contextId").asText(null);
    if (contextId == null || contextId.isBlank()) {
      contextId = UUID.randomUUID().toString();
    }
    String userText = textOf(message);
    String taskId = UUID.randomUUID().toString();
    Map<String, Object> claims = claimsFrom(headers);

    A2AMessage userMessage = A2AMessage.userText(UUID.randomUUID().toString(), userText);
    long now = System.currentTimeMillis();
    // Persist the lifecycle: submitted -> working (best-effort, never fails the turn).
    A2ATask task = A2ATask.submitted(taskId, contextId, userMessage, now);
    persist(task);
    task = task.withState(A2ATaskState.WORKING, null, now);
    persist(task);

    A2ARequest request =
        new A2ARequest(
            taskId, contextId, config.agentId(), userMessage, false, null, claims);

    CollectingGatewayEmitter emitter = new CollectingGatewayEmitter();
    new A2ARequestBridge(connector, config.requestTimeoutMs()).run(request, emitter);

    // Persist the terminal task with the reply as an artifact + the agent message in history.
    long done = System.currentTimeMillis();
    String reply = emitter.text();
    A2ATask terminal = task.withState(emitter.finalState(), emitter.statusMessage(), done);
    if (reply != null && !reply.isEmpty()) {
      terminal = terminal.withArtifact(A2AArtifact.text(UUID.randomUUID().toString(), "response", reply), done);
      terminal =
          terminal.withMessage(
              new A2AMessage(
                  A2AMessage.Role.AGENT, UUID.randomUUID().toString(),
                  List.of(A2APart.text(reply)), contextId, taskId, null),
              done);
    }
    persist(terminal);

    return rpcResult(idNode, reply, contextId);
  }

  private String tasksGet(JsonNode idNode, JsonNode req) throws Exception {
    String taskId = req.path("params").path("id").asText(null);
    if (taskId == null || taskId.isBlank()) {
      return rpcError(idNode, -32602, "tasks/get requires params.id");
    }
    Optional<A2ATask> task = taskStore.loadTask(taskId);
    if (task.isEmpty()) {
      return rpcError(idNode, -32001, "Task not found: " + taskId);
    }
    return rpcTaskResult(idNode, task.get());
  }

  private String tasksCancel(JsonNode idNode, JsonNode req) throws Exception {
    String taskId = req.path("params").path("id").asText(null);
    if (taskId == null || taskId.isBlank()) {
      return rpcError(idNode, -32602, "tasks/cancel requires params.id");
    }
    Optional<A2ATask> existing = taskStore.loadTask(taskId);
    if (existing.isEmpty()) {
      return rpcError(idNode, -32001, "Task not found: " + taskId);
    }
    A2ATask task = existing.get();
    if (!task.getState().isFinal()) {
      // Best-effort: mark canceled. The in-flight bridge request, if any, will time out or complete
      // independently; this records the caller's intent and stops the task being treated as live.
      task = task.withState(A2ATaskState.CANCELED, "Canceled by caller", System.currentTimeMillis());
      persist(task);
    }
    return rpcTaskResult(idNode, task);
  }

  // ---- message/stream (SSE) ----

  /**
   * {@code message/stream}: same JSON-RPC body, but the response is an SSE stream of A2A events for
   * the new task (status-update for {@code working}, artifact-update + final status on terminal).
   * Routed here by content negotiation (client sends {@code Accept: text/event-stream}).
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.SERVER_SENT_EVENTS)
  public Multi<String> rpcStream(String body, @Context HttpHeaders headers) {
    final JsonNode req;
    try {
      req = JSON.readTree(body);
    } catch (Exception e) {
      return Multi.createFrom().item(sseError("Parse error"));
    }
    JsonNode message = req.path("params").path("message");
    String contextId = message.path("contextId").asText(null);
    if (contextId == null || contextId.isBlank()) {
      contextId = UUID.randomUUID().toString();
    }
    final String ctx = contextId;
    final String userText = textOf(message);
    final String taskId = UUID.randomUUID().toString();
    final Map<String, Object> claims = claimsFrom(headers);

    A2AMessage userMessage = A2AMessage.userText(UUID.randomUUID().toString(), userText);
    persist(A2ATask.submitted(taskId, ctx, userMessage, System.currentTimeMillis()));

    return Multi.createFrom()
        .<String>emitter(
            em -> {
              // Per-task listener: forward each matching response as an SSE event; complete on final.
              Consumer<A2AResponse> listener =
                  resp -> {
                    if (!taskId.equals(resp.getTaskId())) {
                      return;
                    }
                    em.emit(sseEvent(resp, ctx));
                    if (resp.isFinal()) {
                      em.complete();
                    }
                  };
              connector.onResponse(listener);
              // Deregister when the stream terminates (completed/cancelled) so listeners don't leak.
              em.onTermination(() -> connector.removeResponseListener(listener));
              try {
                connector.publishRequest(
                    new A2ARequest(taskId, ctx, config.agentId(), userMessage, true, null, claims));
              } catch (Exception e) {
                em.emit(sseError("dispatch failed: " + e.getMessage()));
                em.complete();
              }
            });
  }

  private String sseEvent(A2AResponse resp, String contextId) {
    ObjectNode ev = JSON.createObjectNode();
    ev.put("taskId", resp.getTaskId());
    ev.put("contextId", contextId);
    if (resp.isFinal()) {
      ev.put("kind", "status-update");
      ev.put("final", true);
      ArrayNode arts = ev.putArray("artifacts");
      for (A2AArtifact a : resp.getArtifacts()) {
        String t = a.textContent();
        if (t != null && !t.isEmpty()) {
          arts.addObject().put("name", a.getName()).put("text", t);
        }
      }
    } else {
      ev.put("kind", "status-update");
      ev.put("final", false);
    }
    ObjectNode status = ev.putObject("status");
    status.put("state", resp.getState().wire());
    if (resp.getStatusMessage() != null) {
      status.put("message", resp.getStatusMessage());
    }
    try {
      return JSON.writeValueAsString(ev);
    } catch (Exception e) {
      return "{\"kind\":\"status-update\",\"status\":{\"state\":\"failed\"}}";
    }
  }

  private String sseError(String message) {
    ObjectNode ev = JSON.createObjectNode();
    ev.put("kind", "status-update");
    ev.put("final", true);
    ev.putObject("status").put("state", "failed").put("message", message);
    try {
      return JSON.writeValueAsString(ev);
    } catch (Exception e) {
      return "{\"kind\":\"status-update\",\"status\":{\"state\":\"failed\"}}";
    }
  }

  // ---- tasks/pushNotificationConfig/* ----

  private String pushConfigSet(JsonNode idNode, JsonNode req) throws Exception {
    String taskId = req.path("params").path("taskId").asText(null);
    JsonNode cfg = req.path("params").path("pushNotificationConfig");
    if (taskId == null || taskId.isBlank() || cfg.isMissingNode()) {
      return rpcError(idNode, -32602, "set requires params.taskId + pushNotificationConfig");
    }
    String configId = cfg.path("id").asText(UUID.randomUUID().toString());
    String url = cfg.path("url").asText(null);
    if (url == null || url.isBlank()) {
      return rpcError(idNode, -32602, "pushNotificationConfig.url is required");
    }
    String token = cfg.path("token").asText(null);
    AuthSpec auth = AuthSpec.none();
    JsonNode authNode = cfg.path("authentication");
    if (authNode.hasNonNull("credential") && authNode.hasNonNull("headerName")) {
      auth = AuthSpec.apiKey(authNode.get("headerName").asText(), authNode.get("credential").asText());
    }
    A2APushConfig stored = new A2APushConfig(configId, url, token, auth);
    taskStore.savePushConfig(taskId, stored);
    return rpcPushConfigResult(idNode, taskId, stored);
  }

  private String pushConfigGet(JsonNode idNode, JsonNode req) throws Exception {
    String taskId = req.path("params").path("taskId").asText(null);
    String configId = req.path("params").path("pushNotificationConfigId").asText(null);
    if (taskId == null || configId == null) {
      return rpcError(idNode, -32602, "get requires params.taskId + pushNotificationConfigId");
    }
    Optional<A2APushConfig> cfg = taskStore.getPushConfig(taskId, configId);
    if (cfg.isEmpty()) {
      return rpcError(idNode, -32001, "push config not found");
    }
    return rpcPushConfigResult(idNode, taskId, cfg.get());
  }

  private String pushConfigList(JsonNode idNode, JsonNode req) throws Exception {
    String taskId = req.path("params").path("taskId").asText(null);
    if (taskId == null) {
      return rpcError(idNode, -32602, "list requires params.taskId");
    }
    ObjectNode root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.set("id", idNode == null ? JSON.nullNode() : idNode);
    ArrayNode result = root.putArray("result");
    for (A2APushConfig cfg : taskStore.listPushConfigs(taskId)) {
      result.add(pushConfigNode(taskId, cfg));
    }
    return JSON.writeValueAsString(root);
  }

  private String pushConfigDelete(JsonNode idNode, JsonNode req) throws Exception {
    String taskId = req.path("params").path("taskId").asText(null);
    String configId = req.path("params").path("pushNotificationConfigId").asText(null);
    if (taskId == null || configId == null) {
      return rpcError(idNode, -32602, "delete requires params.taskId + pushNotificationConfigId");
    }
    taskStore.deletePushConfig(taskId, configId);
    ObjectNode root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.set("id", idNode == null ? JSON.nullNode() : idNode);
    root.putNull("result");
    return JSON.writeValueAsString(root);
  }

  private ObjectNode pushConfigNode(String taskId, A2APushConfig cfg) {
    ObjectNode node = JSON.createObjectNode();
    node.put("taskId", taskId);
    ObjectNode c = node.putObject("pushNotificationConfig");
    c.put("id", cfg.getId());
    c.put("url", cfg.getUrl());
    if (cfg.getToken() != null) {
      c.put("token", cfg.getToken());
    }
    return node;
  }

  private String rpcPushConfigResult(JsonNode idNode, String taskId, A2APushConfig cfg)
      throws Exception {
    ObjectNode root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.set("id", idNode == null ? JSON.nullNode() : idNode);
    root.set("result", pushConfigNode(taskId, cfg));
    return JSON.writeValueAsString(root);
  }

  // ---- helpers ----

  private void persist(A2ATask task) {
    try {
      taskStore.saveTask(task);
    } catch (Exception e) {
      LOG.warn("task persist failed for {} ({}): {}", task.getId(), task.getState().wire(), e.toString());
    }
  }

  /** Extract authenticated-caller claims from the Authorization header, if present. */
  private Map<String, Object> claimsFrom(HttpHeaders headers) {
    if (headers == null) {
      return null;
    }
    String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
    if (auth == null || auth.isBlank()) {
      return null;
    }
    Map<String, Object> claims = new LinkedHashMap<>();
    claims.put("authorization", auth);
    int sp = auth.indexOf(' ');
    if (sp > 0) {
      claims.put("scheme", auth.substring(0, sp));
      claims.put("token", auth.substring(sp + 1).trim());
    }
    return claims;
  }

  private static String textOf(JsonNode message) {
    StringBuilder sb = new StringBuilder();
    JsonNode parts = message.path("parts");
    if (parts.isArray()) {
      for (JsonNode p : parts) {
        if ("text".equals(p.path("kind").asText()) && p.hasNonNull("text")) {
          if (sb.length() > 0) {
            sb.append('\n');
          }
          sb.append(p.get("text").asText());
        }
      }
    }
    return sb.toString();
  }

  private String rpcResult(JsonNode id, String reply, String contextId) throws Exception {
    ObjectNode root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.set("id", id == null ? JSON.nullNode() : id);
    ObjectNode result = root.putObject("result");
    result.put("role", "agent");
    result.put("messageId", UUID.randomUUID().toString());
    result.put("contextId", contextId);
    result.put("kind", "message");
    ObjectNode part = result.putArray("parts").addObject();
    part.put("kind", "text");
    part.put("text", reply == null ? "" : reply);
    return JSON.writeValueAsString(root);
  }

  /** Render an {@link A2ATask} as a JSON-RPC Task result (spec shape) for tasks/get + tasks/cancel. */
  private String rpcTaskResult(JsonNode id, A2ATask task) throws Exception {
    ObjectNode root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.set("id", id == null ? JSON.nullNode() : id);
    ObjectNode result = root.putObject("result");
    result.put("kind", "task");
    result.put("id", task.getId());
    if (task.getContextId() != null) {
      result.put("contextId", task.getContextId());
    }
    ObjectNode status = result.putObject("status");
    status.put("state", task.getState().wire());
    if (task.getStatusMessage() != null) {
      status.put("message", task.getStatusMessage());
    }
    ArrayNode artifacts = result.putArray("artifacts");
    for (A2AArtifact a : task.getArtifacts()) {
      ObjectNode an = artifacts.addObject();
      an.put("artifactId", a.getArtifactId());
      if (a.getName() != null) {
        an.put("name", a.getName());
      }
      ArrayNode parts = an.putArray("parts");
      for (A2APart p : a.getParts()) {
        if (p.getKind() == A2APart.Kind.TEXT && p.getText() != null) {
          ObjectNode pn = parts.addObject();
          pn.put("kind", "text");
          pn.put("text", p.getText());
        }
      }
    }
    return JSON.writeValueAsString(root);
  }

  private String rpcError(JsonNode id, int code, String message) {
    ObjectNode root = JSON.createObjectNode();
    root.put("jsonrpc", "2.0");
    root.set("id", id == null ? JSON.nullNode() : id);
    ObjectNode err = root.putObject("error");
    err.put("code", code);
    err.put("message", message);
    try {
      return JSON.writeValueAsString(root);
    } catch (Exception e) {
      return "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\"error\"}}";
    }
  }

  /**
   * Accumulates the artifact/fail text from the bridge into the reply string and tracks the terminal
   * task state so the gateway can persist an accurate lifecycle.
   */
  static final class CollectingGatewayEmitter implements GatewayEmitter {
    private final StringBuilder sb = new StringBuilder();
    private A2ATaskState finalState = A2ATaskState.COMPLETED;
    private String statusMessage;

    @Override
    public void working(String statusMessage) {
      // intermediate updates are never read by the harness
    }

    @Override
    public void artifact(String name, List<A2APart> parts) {
      if (parts == null) {
        return;
      }
      for (A2APart p : parts) {
        if (p.getKind() == A2APart.Kind.TEXT && p.getText() != null) {
          if (sb.length() > 0) {
            sb.append('\n');
          }
          sb.append(p.getText());
        }
      }
    }

    @Override
    public void complete() {
      finalState = A2ATaskState.COMPLETED;
    }

    @Override
    public void fail(String message) {
      finalState = A2ATaskState.FAILED;
      statusMessage = message;
      if (sb.length() == 0 && message != null) {
        sb.append(message);
      }
    }

    @Override
    public void inputRequired(String message) {
      finalState = A2ATaskState.INPUT_REQUIRED;
      statusMessage = message;
      if (message != null) {
        if (sb.length() > 0) {
          sb.append('\n');
        }
        sb.append(message);
      }
    }

    String text() {
      return sb.toString();
    }

    A2ATaskState finalState() {
      return finalState;
    }

    String statusMessage() {
      return statusMessage;
    }
  }
}

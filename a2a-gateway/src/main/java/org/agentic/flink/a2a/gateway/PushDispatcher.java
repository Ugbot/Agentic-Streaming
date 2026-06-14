package org.agentic.flink.a2a.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.AuthSpec;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.a2a.storage.A2ATaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A2A push notifications: when push is enabled, registers a single global response listener on the
 * bridge connector and, on each <b>terminal</b> task response, POSTs the resulting {@code Task}
 * envelope to every webhook registered for that task via
 * {@code tasks/pushNotificationConfig/set} (honoring its {@link AuthSpec} + notification token).
 *
 * <p>Lets a caller fire-and-forget a {@code message/send} and be notified out-of-band when the task
 * finishes, instead of holding an SSE stream open. No-op unless {@code a2a.gateway.push.enabled=true}.
 */
@ApplicationScoped
public class PushDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(PushDispatcher.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  @Inject GatewayConfig config;
  @Inject A2AGatewayConnector connector;
  @Inject A2ATaskStore taskStore;

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  void onStart(@Observes StartupEvent ev) {
    if (!config.pushEnabled()) {
      LOG.info("A2A push notifications disabled (a2a.gateway.push.enabled=false)");
      return;
    }
    connector.onResponse(this::onResponse);
    LOG.info("A2A push dispatcher registered");
  }

  /** Package-visible for tests: dispatch one response (only terminal ones trigger a POST). */
  void onResponse(A2AResponse resp) {
    if (resp == null || !resp.isFinal()) {
      return;
    }
    try {
      for (A2APushConfig cfg : taskStore.listPushConfigs(resp.getTaskId())) {
        post(cfg, resp);
      }
    } catch (Exception e) {
      LOG.warn("push dispatch failed for task {}: {}", resp.getTaskId(), e.toString());
    }
  }

  private void post(A2APushConfig cfg, A2AResponse resp) {
    try {
      String body = taskJson(resp);
      HttpRequest.Builder b =
          HttpRequest.newBuilder()
              .uri(URI.create(cfg.getUrl()))
              .timeout(Duration.ofSeconds(10))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body));
      AuthSpec auth = cfg.getAuth();
      if (auth != null && auth.getScheme() != AuthSpec.Scheme.NONE && auth.getHeaderName() != null) {
        b.header(auth.getHeaderName(), auth.getCredential() == null ? "" : auth.getCredential());
      }
      if (cfg.getToken() != null) {
        b.header("X-A2A-Notification-Token", cfg.getToken());
      }
      HttpResponse<Void> r = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
      LOG.debug("pushed task {} to {} -> {}", resp.getTaskId(), cfg.getUrl(), r.statusCode());
    } catch (Exception e) {
      LOG.warn("push POST to {} failed: {}", cfg.getUrl(), e.toString());
    }
  }

  private String taskJson(A2AResponse resp) throws Exception {
    ObjectNode task = JSON.createObjectNode();
    task.put("kind", "task");
    task.put("id", resp.getTaskId());
    if (resp.getContextId() != null) {
      task.put("contextId", resp.getContextId());
    }
    ObjectNode status = task.putObject("status");
    status.put("state", resp.getState().wire());
    if (resp.getStatusMessage() != null) {
      status.put("message", resp.getStatusMessage());
    }
    ArrayNode arts = task.putArray("artifacts");
    for (A2AArtifact a : resp.getArtifacts()) {
      String t = a.textContent();
      if (t != null && !t.isEmpty()) {
        arts.addObject().put("name", a.getName()).put("text", t);
      }
    }
    return JSON.writeValueAsString(task);
  }
}

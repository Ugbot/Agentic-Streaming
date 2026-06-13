package org.agentic.flink.a2a.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.UUID;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APart;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The A2A wire for the gateway, hand-rolled to the <b>spec</b> (not the a2a-java SDK's proto method
 * names): serves the Agent Card at {@code /.well-known/agent-card.json} and JSON-RPC {@code
 * message/send} at {@code /}. Each inbound message becomes an {@link A2ARequest} published over the
 * A2A bridge (Redis) to the embedded Flink job; {@link A2ARequestBridge} blocks for the verifier's
 * response and we return its text as a JSON-RPC {@code Message} result — exactly what the tau2
 * harness (and held-out agents) read.
 *
 * <p>The gateway is a thin, stateless front: all reasoning/state lives in the Flink graph and Redis.
 */
@ApplicationScoped
@Path("/")
public class A2AResource {

  private static final Logger LOG = LoggerFactory.getLogger(A2AResource.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  @Inject GatewayConfig config;
  @Inject A2AGatewayConnector connector;

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
    caps.put("streaming", false); // force the simple non-streaming message/send path
    caps.put("pushNotifications", false);
    card.putArray("defaultInputModes").add("text/plain");
    card.putArray("defaultOutputModes").add("text/plain");
    card.putArray("skills");
    return JSON.writeValueAsString(card);
  }

  // ---- JSON-RPC message/send ----

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public String rpc(String body) {
    JsonNode req;
    try {
      req = JSON.readTree(body);
    } catch (Exception e) {
      return rpcError(null, -32700, "Parse error");
    }
    JsonNode idNode = req.get("id");
    String method = req.path("method").asText("");
    if (!"message/send".equals(method)) {
      return rpcError(idNode, -32601, "Unsupported method: " + method);
    }
    try {
      JsonNode message = req.path("params").path("message");
      String contextId = message.path("contextId").asText(null);
      if (contextId == null || contextId.isBlank()) {
        contextId = UUID.randomUUID().toString();
      }
      String userText = textOf(message);

      A2ARequest request =
          new A2ARequest(
              UUID.randomUUID().toString(),
              contextId,
              config.agentId(),
              A2AMessage.userText(UUID.randomUUID().toString(), userText),
              false,
              null,
              null);

      CollectingGatewayEmitter emitter = new CollectingGatewayEmitter();
      new A2ARequestBridge(connector, config.requestTimeoutMs()).run(request, emitter);
      return rpcResult(idNode, emitter.text(), contextId);
    } catch (Exception e) {
      LOG.warn("message/send failed", e);
      return rpcError(idNode, -32603, "Internal error: " + e.getMessage());
    }
  }

  // ---- helpers ----

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

  /** Accumulates the artifact/fail text from the bridge into the reply string. */
  static final class CollectingGatewayEmitter implements GatewayEmitter {
    private final StringBuilder sb = new StringBuilder();

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
      // terminal; text already collected
    }

    @Override
    public void fail(String message) {
      if (sb.length() == 0 && message != null) {
        sb.append(message);
      }
    }

    @Override
    public void inputRequired(String message) {
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
  }
}

package org.agentic.flink.example.banking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import org.agentic.flink.example.banking.env.EnvSession;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.agentic.flink.screening.ScreeningResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal, spec-conformant A2A JSON-RPC server for the banking agents — serves the Agent Card and
 * {@code message/send} (text in/out) and nothing more, which is exactly the harness's wire
 * contract.
 *
 * <p>We hand-roll the wire (JDK {@link HttpServer} + Jackson, no new deps) rather than use the
 * a2a-java Alpha3 reference server, because that server's JSON-RPC binding registers proto-style
 * method names ({@code SendMessage}) instead of the spec's {@code message/send} the tau2 harness's
 * Python client calls. Each request runs the same safety stack + bounded ReAct brain as the Flink
 * operator; personal→CS uses the matching {@link #httpCsClient} so both legs speak {@code
 * message/send}.
 *
 * <pre>
 *   A2A_BANKING_ROLE=cs       QUARKUS_HTTP_PORT=9002 java ... BankingA2AServer
 *   A2A_BANKING_ROLE=personal PORT=9001 CS_AGENT_URL=... java ... BankingA2AServer
 * </pre>
 */
public final class BankingA2AServer {
  private static final Logger LOG = LoggerFactory.getLogger(BankingA2AServer.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final BankingAgentSetup setup;
  private final String agentName;
  private final String publicUrl;
  private final Map<String, RoutingBudget> budgets = new ConcurrentHashMap<>();
  private HttpServer server;

  private BankingA2AServer(BankingAgentSetup setup, String agentName, String publicUrl) {
    this.setup = setup;
    this.agentName = agentName;
    this.publicUrl = publicUrl;
  }

  public static void main(String[] args) throws Exception {
    String roleEnv = env("A2A_BANKING_ROLE", "personal");
    BankingAgentSetup.Role role =
        "cs".equalsIgnoreCase(roleEnv) ? BankingAgentSetup.Role.CS : BankingAgentSetup.Role.PERSONAL;
    int port = Integer.parseInt(env("PORT", env("QUARKUS_HTTP_PORT", role == BankingAgentSetup.Role.CS ? "9002" : "9001")));
    String publicUrl = env("PUBLIC_URL", "http://localhost:" + port);
    String name =
        env("AGENT_NAME", role == BankingAgentSetup.Role.CS ? "Rho-Bank Customer Service" : "Rho-Bank Personal Assistant");

    BankingTurnContext.CustomerServiceClient cs =
        role == BankingAgentSetup.Role.PERSONAL ? httpCsClient(env("CS_AGENT_URL", null)) : null;
    BankingAgentSetup setup = BankingAgentSetup.fromEnv(role, cs);

    new BankingA2AServer(setup, name, publicUrl).start(port);
    Thread.currentThread().join(); // run until killed
  }

  void start(int port) throws IOException {
    server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.setExecutor(Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "a2a-banking-" + port);
      t.setDaemon(true);
      return t;
    }));
    server.createContext("/.well-known/agent-card.json", this::handleCard);
    server.createContext("/", this::handleRpc);
    server.start();
    LOG.info("Banking A2A server [{}] listening on :{} ({})", setup.role(), port, agentName);
  }

  // ---- agent card ----

  private void handleCard(HttpExchange ex) throws IOException {
    ObjectNode card = JSON.createObjectNode();
    card.put("protocolVersion", "0.3.0");
    card.put("name", agentName);
    card.put("description", "Rho-Bank " + setup.role() + " agent (Agentic-Flink, safety-wrapped)");
    card.put("version", "1.0.0");
    card.put("url", publicUrl);
    card.put("preferredTransport", "JSONRPC");
    ObjectNode caps = card.putObject("capabilities");
    caps.put("streaming", false); // force the simple non-streaming message/send path
    caps.put("pushNotifications", false);
    arr(card, "defaultInputModes", "text/plain");
    arr(card, "defaultOutputModes", "text/plain");
    card.putArray("skills");
    respond(ex, 200, JSON.writeValueAsBytes(card));
  }

  // ---- JSON-RPC message/send ----

  private void handleRpc(HttpExchange ex) throws IOException {
    if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
      respond(ex, 405, "{}".getBytes(StandardCharsets.UTF_8));
      return;
    }
    JsonNode req;
    try {
      req = JSON.readTree(ex.getRequestBody().readAllBytes());
    } catch (Exception e) {
      respond(ex, 200, rpcError(null, -32700, "Parse error").getBytes(StandardCharsets.UTF_8));
      return;
    }
    JsonNode idNode = req.get("id");
    String method = req.path("method").asText("");
    if (!"message/send".equals(method)) {
      respond(ex, 200, rpcError(idNode, -32601, "Unsupported method: " + method).getBytes(StandardCharsets.UTF_8));
      return;
    }
    try {
      JsonNode message = req.path("params").path("message");
      String contextId = message.path("contextId").asText(null);
      if (contextId == null || contextId.isBlank()) {
        contextId = UUID.randomUUID().toString();
      }
      String userText = textOf(message);
      String reply = handleTurn(contextId, userText);
      respond(ex, 200, rpcResult(idNode, reply, contextId).getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      LOG.warn("turn failed", e);
      respond(ex, 200, rpcError(idNode, -32603, "Internal error: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
    }
  }

  private String handleTurn(String contextId, String userText) {
    long now = System.currentTimeMillis();
    RoutingBudget budget =
        budgets.computeIfAbsent(
            contextId,
            k -> new RoutingBudget(setup.maxRoundTrips(), setup.maxIterations(), setup.turnDeadlineMs(), 8));
    budget.startTurn(now);

    ScreeningResult screen = setup.screening().screen(contextId, userText, now);
    if ("BLOCK".equals(screen.verdict)) {
      LOG.info("Screening BLOCK ctx {}: {}", contextId, screen.reason);
      return "I'm sorry, I can't help with that request.";
    }
    final RoutingBudget b = budget;
    return EnvSession.withContext(
        contextId,
        () -> setup.brain().respond(userText, new BankingTurnContext(contextId, b, now, setup.cs())));
  }

  // ---- outbound personal->CS client (also speaks message/send) ----

  static BankingTurnContext.CustomerServiceClient httpCsClient(String csUrl) {
    if (csUrl == null || csUrl.isBlank()) {
      LOG.warn("CS_AGENT_URL not set — ask_customer_service unavailable");
      return null;
    }
    HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    return (contextId, messageText) -> {
      try {
        ObjectNode msg = JSON.createObjectNode();
        msg.put("role", "user");
        msg.put("messageId", UUID.randomUUID().toString());
        msg.put("contextId", contextId);
        msg.put("kind", "message");
        ArrayNode parts = msg.putArray("parts");
        ObjectNode part = parts.addObject();
        part.put("kind", "text");
        part.put("text", messageText);
        ObjectNode rpc = JSON.createObjectNode();
        rpc.put("jsonrpc", "2.0");
        rpc.put("id", UUID.randomUUID().toString());
        rpc.put("method", "message/send");
        rpc.putObject("params").set("message", msg);

        HttpRequest request =
            HttpRequest.newBuilder(URI.create(csUrl))
                .timeout(Duration.ofSeconds(300))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(rpc)))
                .build();
        HttpResponse<byte[]> resp = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        JsonNode result = JSON.readTree(resp.body()).path("result");
        return replyText(result);
      } catch (Exception e) {
        return "[customer service unavailable: " + e.getMessage() + "]";
      }
    };
  }

  /** Extract reply text from a message/send result (Message, or Task artifacts + status message). */
  private static String replyText(JsonNode result) {
    if (result == null || result.isMissingNode() || result.isNull()) {
      return "[no response from customer service]";
    }
    StringBuilder sb = new StringBuilder();
    appendParts(sb, result.path("parts")); // Message
    JsonNode artifacts = result.path("artifacts"); // Task
    if (artifacts.isArray()) {
      for (JsonNode a : artifacts) {
        appendParts(sb, a.path("parts"));
      }
    }
    appendParts(sb, result.path("status").path("message").path("parts"));
    return sb.length() == 0 ? "[no response from customer service]" : sb.toString().trim();
  }

  private static void appendParts(StringBuilder sb, JsonNode parts) {
    if (parts != null && parts.isArray()) {
      for (JsonNode p : parts) {
        if ("text".equals(p.path("kind").asText()) && p.hasNonNull("text")) {
          if (sb.length() > 0) {
            sb.append('\n');
          }
          sb.append(p.get("text").asText());
        }
      }
    }
  }

  // ---- helpers ----

  private static String textOf(JsonNode message) {
    StringBuilder sb = new StringBuilder();
    appendParts(sb, message.path("parts"));
    return sb.toString();
  }

  private String rpcResult(JsonNode id, String reply, String contextId) throws IOException {
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
    } catch (IOException e) {
      return "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\"error\"}}";
    }
  }

  private static void arr(ObjectNode node, String field, String... values) {
    ArrayNode a = node.putArray(field);
    for (String v : values) {
      a.add(v);
    }
  }

  private void respond(HttpExchange ex, int status, byte[] body) throws IOException {
    ex.getResponseHeaders().add("Content-Type", "application/json");
    ex.sendResponseHeaders(status, body.length);
    ex.getResponseBody().write(body);
    ex.close();
  }

  private static String env(String key, String def) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? def : v;
  }
}

package org.agentic.flink.tools.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal JSON-RPC 2.0 client for MCP servers.
 *
 * <p>Implements just enough of the protocol to {@code initialize}, {@code tools/list} and
 * {@code tools/call} — which is enough to turn any MCP server into a set of
 * {@link org.agentic.flink.tools.ToolExecutor}s. Notifications and server-initiated
 * messages are ignored (they are not relevant to the tool-execution flow).
 *
 * <p>Not thread-safe: each operator task should hold its own client instance, constructed once
 * in {@code RichFunction.open()}.
 */
public final class McpClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(McpClient.class);
  private static final String PROTOCOL_VERSION = "2024-11-05";

  private final McpServerSpec spec;
  private final ObjectMapper mapper = new ObjectMapper();
  private final AtomicLong nextId = new AtomicLong(1);

  // STDIO-specific state
  private Process process;
  private BufferedWriter stdin;
  private BufferedReader stdout;

  // HTTP-specific state
  private HttpClient http;

  private volatile boolean initialized = false;

  public McpClient(McpServerSpec spec) {
    this.spec = spec;
  }

  /** Connect and complete the MCP {@code initialize} handshake. Idempotent. */
  public synchronized void initialize() throws IOException {
    if (initialized) return;
    switch (spec.getTransport()) {
      case STDIO:
        startStdio();
        break;
      case HTTP:
        startHttp();
        break;
    }

    ObjectNode params = mapper.createObjectNode();
    params.put("protocolVersion", PROTOCOL_VERSION);
    params.set("capabilities", mapper.createObjectNode());
    ObjectNode info = mapper.createObjectNode();
    info.put("name", "agentic-flink");
    info.put("version", "1.0.0");
    params.set("clientInfo", info);

    JsonNode result = rpc("initialize", params);
    LOG.info(
        "MCP server '{}' initialized: serverInfo={}",
        spec.getName(), result == null ? null : result.path("serverInfo"));
    initialized = true;
  }

  /** List the tools exposed by the connected server. */
  public List<McpToolMetadata> listTools() throws IOException {
    ensureInitialized();
    JsonNode result = rpc("tools/list", mapper.createObjectNode());
    List<McpToolMetadata> tools = new ArrayList<>();
    if (result == null) return tools;
    JsonNode arr = result.path("tools");
    if (!arr.isArray()) return tools;
    for (JsonNode t : arr) {
      String name = t.path("name").asText();
      String description = t.path("description").asText("");
      Map<String, Object> schema = new HashMap<>();
      JsonNode schemaNode = t.path("inputSchema");
      if (schemaNode.isObject()) {
        Iterator<Map.Entry<String, JsonNode>> it = schemaNode.fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> e = it.next();
          schema.put(e.getKey(), mapper.convertValue(e.getValue(), Object.class));
        }
      }
      tools.add(new McpToolMetadata(name, description, schema));
    }
    return tools;
  }

  /** Invoke a tool by name with the given argument map. Returns the tool's content payload. */
  public Object callTool(String toolName, Map<String, Object> arguments) throws IOException {
    ensureInitialized();
    ObjectNode params = mapper.createObjectNode();
    params.put("name", toolName);
    params.set("arguments", mapper.valueToTree(arguments == null ? Map.of() : arguments));
    JsonNode result = rpc("tools/call", params);
    if (result == null) return null;
    // MCP returns { content: [{ type: "text", text: "..." }, ...], isError: bool }
    boolean isError = result.path("isError").asBoolean(false);
    JsonNode content = result.path("content");
    StringBuilder text = new StringBuilder();
    if (content.isArray()) {
      for (JsonNode part : content) {
        if ("text".equals(part.path("type").asText())) {
          if (text.length() > 0) text.append('\n');
          text.append(part.path("text").asText());
        }
      }
    }
    String body = text.toString();
    if (isError) {
      throw new IOException("MCP tool '" + toolName + "' returned error: " + body);
    }
    return body;
  }

  @Override
  public synchronized void close() {
    try {
      if (stdin != null) stdin.close();
    } catch (IOException ignored) {
    }
    try {
      if (stdout != null) stdout.close();
    } catch (IOException ignored) {
    }
    if (process != null) {
      process.destroy();
      try {
        if (!process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
          process.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
    }
    initialized = false;
  }

  // ---- internals ----

  private void ensureInitialized() throws IOException {
    if (!initialized) {
      initialize();
    }
  }

  private void startStdio() throws IOException {
    ProcessBuilder pb = new ProcessBuilder(spec.getCommand());
    pb.environment().putAll(spec.getEnv());
    pb.redirectErrorStream(false);
    process = pb.start();
    stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
    stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
  }

  private void startHttp() {
    http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  private JsonNode rpc(String method, ObjectNode params) throws IOException {
    long id = nextId.getAndIncrement();
    ObjectNode req = mapper.createObjectNode();
    req.put("jsonrpc", "2.0");
    req.put("id", id);
    req.put("method", method);
    req.set("params", params);

    String payload = mapper.writeValueAsString(req);
    JsonNode response;
    if (spec.getTransport() == McpServerSpec.Transport.STDIO) {
      stdin.write(payload);
      stdin.write('\n');
      stdin.flush();
      String line = readResponseLine(id);
      response = mapper.readTree(line);
    } else {
      response = httpRpc(payload);
    }
    if (response.has("error")) {
      throw new IOException("MCP rpc error: " + response.get("error").toString());
    }
    return response.get("result");
  }

  private String readResponseLine(long expectedId) throws IOException {
    // The server may interleave notifications (no id) with our response. Skip until we see the
    // matching id.
    String line;
    while ((line = stdout.readLine()) != null) {
      JsonNode parsed;
      try {
        parsed = mapper.readTree(line);
      } catch (Exception e) {
        LOG.debug("Skipping non-JSON line from MCP server: {}", line);
        continue;
      }
      if (parsed.has("id") && parsed.get("id").asLong() == expectedId) {
        return line;
      }
      // Notification or unrelated — log and continue.
      LOG.debug("MCP notification ignored: {}", line);
    }
    throw new IOException("MCP server closed before responding to id=" + expectedId);
  }

  private JsonNode httpRpc(String payload) throws IOException {
    try {
      HttpRequest.Builder b =
          HttpRequest.newBuilder()
              .uri(URI.create(spec.getUrl()))
              .timeout(Duration.ofSeconds(30))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(payload));
      for (Map.Entry<String, String> h : spec.getHeaders().entrySet()) {
        b.header(h.getKey(), h.getValue());
      }
      HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() / 100 != 2) {
        throw new IOException("HTTP " + resp.statusCode() + ": " + resp.body());
      }
      return mapper.readTree(resp.body());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException(ie);
    }
  }
}

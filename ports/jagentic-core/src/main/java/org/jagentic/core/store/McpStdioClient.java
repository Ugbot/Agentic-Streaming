package org.jagentic.core.store;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.ToolRegistry;

/** Pure-Java MCP client over the stdio transport: launches an MCP server process and speaks
 * newline-delimited JSON-RPC 2.0 (initialize → notifications/initialized → tools/list →
 * tools/call). Discovered tools are registered into a {@link ToolRegistry} so an agent can
 * call external MCP tool servers. No third-party MCP dependency — just Jackson + ProcessBuilder.
 * Mirrors the Python ({@code mcp} SDK) and Go ({@code mark3labs/mcp-go}) clients. */
public final class McpStdioClient implements AutoCloseable {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PROTOCOL_VERSION = "2024-11-05";

  private final Process process;
  private final BufferedWriter stdin;
  private final BufferedReader stdout;
  private final List<Map<String, String>> specs = new ArrayList<>();
  private int nextId = 1;

  public McpStdioClient(List<String> command) throws IOException {
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectError(ProcessBuilder.Redirect.INHERIT);
    this.process = pb.start();
    this.stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
    this.stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    handshake();
  }

  private void handshake() throws IOException {
    Map<String, Object> initParams = new LinkedHashMap<>();
    initParams.put("protocolVersion", PROTOCOL_VERSION);
    initParams.put("capabilities", Map.of());
    initParams.put("clientInfo", Map.of("name", "jagentic-core", "version", "0.1.0"));
    request("initialize", initParams);
    notification("notifications/initialized", Map.of());

    JsonNode listed = request("tools/list", Map.of());
    JsonNode tools = listed.path("result").path("tools");
    if (tools.isArray()) {
      for (JsonNode t : tools) {
        specs.add(Map.of(
            "name", t.path("name").asText(""),
            "description", t.path("description").asText("")));
      }
    }
  }

  /** Discovered MCP tool specs ({@code [{name, description}]}). */
  public List<Map<String, String>> tools() {
    return List.copyOf(specs);
  }

  private synchronized String callTool(String name, Map<String, Object> args) {
    try {
      Map<String, Object> params = new LinkedHashMap<>();
      params.put("name", name);
      params.put("arguments", args == null ? Map.of() : args);
      JsonNode resp = request("tools/call", params);
      JsonNode content = resp.path("result").path("content");
      StringBuilder sb = new StringBuilder();
      if (content.isArray()) {
        for (JsonNode part : content) {
          if ("text".equals(part.path("type").asText())) {
            if (sb.length() > 0) {
              sb.append("\n");
            }
            sb.append(part.path("text").asText(""));
          }
        }
      }
      return sb.toString();
    } catch (IOException e) {
      throw new RuntimeException("MCP tools/call " + name + ": " + e.getMessage(), e);
    }
  }

  /** Register every MCP tool into {@code reg} (optionally id-prefixed); returns the ids. */
  public List<String> register(ToolRegistry reg, String prefix) {
    List<String> ids = new ArrayList<>();
    String p = prefix == null ? "" : prefix;
    for (Map<String, String> spec : specs) {
      String name = spec.get("name");
      String id = p + name;
      reg.register(id, spec.getOrDefault("description", ""), params -> callTool(name, params));
      ids.add(id);
    }
    return ids;
  }

  private JsonNode request(String method, Object params) throws IOException {
    int id = nextId++;
    Map<String, Object> msg = new LinkedHashMap<>();
    msg.put("jsonrpc", "2.0");
    msg.put("id", id);
    msg.put("method", method);
    msg.put("params", params);
    writeLine(MAPPER.writeValueAsString(msg));
    // read until the matching-id response (skip notifications / log messages with no/other id)
    String line;
    while ((line = stdout.readLine()) != null) {
      if (line.isBlank()) {
        continue;
      }
      JsonNode node;
      try {
        node = MAPPER.readTree(line);
      } catch (IOException parseErr) {
        continue; // ignore non-JSON lines
      }
      if (node.has("id") && node.path("id").asInt(-1) == id) {
        if (node.has("error")) {
          throw new IOException("MCP error: " + node.path("error").toString());
        }
        return node;
      }
    }
    throw new IOException("MCP server closed before responding to " + method);
  }

  private void notification(String method, Object params) throws IOException {
    Map<String, Object> msg = new LinkedHashMap<>();
    msg.put("jsonrpc", "2.0");
    msg.put("method", method);
    msg.put("params", params);
    writeLine(MAPPER.writeValueAsString(msg));
  }

  private void writeLine(String json) throws IOException {
    stdin.write(json);
    stdin.write("\n");
    stdin.flush();
  }

  @Override
  public void close() {
    try {
      stdin.close();
    } catch (IOException ignored) {
      // best-effort
    }
    process.destroy();
    try {
      process.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}

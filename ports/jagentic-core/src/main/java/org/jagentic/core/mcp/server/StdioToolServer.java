package org.jagentic.core.mcp.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.ToolRegistry;

/** Runs a {@link ToolServer} over the MCP <b>stdio</b> transport: newline-delimited JSON-RPC
 * 2.0 on stdin/stdout (initialize → notifications/initialized → tools/list → tools/call).
 * This is the byte-for-byte counterpart of {@link org.jagentic.core.store.McpStdioClient},
 * so launching this as a subprocess and pointing that client at it is a full round-trip. */
public final class StdioToolServer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ToolServer server;

  public StdioToolServer(ToolRegistry registry) {
    this.server = new ToolServer(registry);
  }

  public StdioToolServer(ToolServer server) {
    this.server = server;
  }

  /** Serve on {@code System.in}/{@code System.out} until stdin closes. */
  public void run() throws IOException {
    run(System.in, System.out);
  }

  /** Serve a single read/write loop on the given streams (used by tests too). */
  public void run(InputStream in, OutputStream out) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.isBlank()) {
        continue;
      }
      JsonNode request;
      try {
        request = MAPPER.readTree(line);
      } catch (IOException parseErr) {
        continue; // ignore non-JSON lines
      }
      JsonNode response = server.handle(request);
      if (response != null) {
        writer.write(MAPPER.writeValueAsString(response));
        writer.write("\n");
        writer.flush();
      }
    }
  }

  public ToolServer server() {
    return server;
  }
}

package org.jagentic.tools.app.mcp;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;

import org.jagentic.core.mcp.server.ToolServer;

/** MCP over Streamable-HTTP: a single JSON-RPC endpoint. POST a JSON-RPC request, get the
 * JSON-RPC response (or 204 for a notification, which has no reply). Any MCP-capable client
 * that speaks the HTTP transport — or our own Flink {@code McpClient} (HTTP) — can call it. */
@Path("/mcp")
public class McpHttpResource {

  @Inject
  ToolServer toolServer;

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response rpc(JsonNode request) {
    JsonNode response = toolServer.handle(request);
    if (response == null) {
      return Response.noContent().build(); // notification: no body
    }
    return Response.ok(response).build();
  }
}

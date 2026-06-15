package org.jagentic.tools.app.rest;

import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.eclipse.microprofile.openapi.annotations.Operation;

import org.jagentic.core.ToolRegistry;

/** Plain REST/OpenAPI facade over the tool registry, for non-MCP frameworks and curl:
 * {@code GET /tools} lists tools with their input schemas; {@code POST /tools/{name}}
 * invokes a tool with a JSON argument map. The OpenAPI spec is served at {@code /q/openapi}. */
@Path("/tools")
@Produces(MediaType.APPLICATION_JSON)
public class ToolsResource {

  @Inject
  ToolRegistry registry;

  @GET
  @Operation(summary = "List available tools with their input JSON-schemas.")
  public List<Map<String, Object>> list() {
    return registry.toolDescriptors();
  }

  @POST
  @Path("/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "Invoke a tool by name with a JSON argument map.")
  public Response invoke(@PathParam("name") String name, Map<String, Object> args) {
    if (registry.get(name) == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("ok", false, "error", "no such tool: " + name)).build();
    }
    try {
      Object result = registry.execute(name, args == null ? Map.of() : args);
      return Response.ok(Map.of("ok", true, "tool", name, "result", result)).build();
    } catch (RuntimeException e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(Map.of("ok", false, "tool", name, "error", String.valueOf(e.getMessage()))).build();
    }
  }
}

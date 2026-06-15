package org.jagentic.tools.app.grpc;

import java.util.Map;

import jakarta.inject.Inject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

import org.jagentic.core.ToolRegistry;
import org.jagentic.tools.grpc.CallToolRequest;
import org.jagentic.tools.grpc.CallToolResponse;
import org.jagentic.tools.grpc.ListToolsRequest;
import org.jagentic.tools.grpc.ListToolsResponse;
import org.jagentic.tools.grpc.MutinyToolServiceGrpc;
import org.jagentic.tools.grpc.ToolDescriptor;

/**
 * gRPC transport over the shared {@link ToolRegistry}. Mirrors the REST/MCP delegation:
 * {@code ListTools} maps {@code registry.toolDescriptors()} to protobuf {@link ToolDescriptor}s
 * (with the inputSchema map serialized to JSON), and {@code CallTool} parses the JSON argument
 * map, runs {@code registry.execute(...)} and serializes the result to JSON.
 *
 * <p>Quarkus serves this on the HTTP port in tests ({@code quarkus.grpc.server.use-separate-server
 * =false}) and on the dedicated gRPC port (9000) otherwise. Errors (unknown tool / bad args /
 * tool failure) come back as {@code ok=false} with an {@code error} message rather than a gRPC
 * status, so a client gets a uniform envelope.
 */
@GrpcService
public class ToolGrpcService extends MutinyToolServiceGrpc.ToolServiceImplBase {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  @Inject
  ToolRegistry registry;

  @Inject
  ObjectMapper mapper;

  @Override
  public Uni<ListToolsResponse> listTools(ListToolsRequest request) {
    return Uni.createFrom().item(() -> {
      ListToolsResponse.Builder resp = ListToolsResponse.newBuilder();
      for (Map<String, Object> d : registry.toolDescriptors()) {
        Object schema = d.get("inputSchema");
        String schemaJson;
        try {
          schemaJson = schema == null ? "{}" : mapper.writeValueAsString(schema);
        } catch (Exception e) {
          schemaJson = "{}";
        }
        resp.addTools(ToolDescriptor.newBuilder()
            .setName(String.valueOf(d.get("name")))
            .setDescription(d.get("description") == null ? "" : String.valueOf(d.get("description")))
            .setInputSchemaJson(schemaJson)
            .build());
      }
      return resp.build();
    });
  }

  @Override
  public Uni<CallToolResponse> callTool(CallToolRequest request) {
    return Uni.createFrom().item(() -> {
      String name = request.getName();
      try {
        String argsJson = request.getArgsJson();
        Map<String, Object> args = (argsJson == null || argsJson.isBlank())
            ? Map.of()
            : mapper.readValue(argsJson, MAP_TYPE);
        if (registry.get(name) == null) {
          return CallToolResponse.newBuilder()
              .setOk(false).setError("no such tool: " + name).build();
        }
        Object result = registry.execute(name, args);
        return CallToolResponse.newBuilder()
            .setOk(true)
            .setResultJson(mapper.writeValueAsString(result))
            .build();
      } catch (Exception e) {
        return CallToolResponse.newBuilder()
            .setOk(false)
            .setError(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage())
            .build();
      }
    });
  }
}

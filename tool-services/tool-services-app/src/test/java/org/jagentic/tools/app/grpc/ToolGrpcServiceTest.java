package org.jagentic.tools.app.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcClientUtils;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import org.jagentic.tools.grpc.CallToolRequest;
import org.jagentic.tools.grpc.CallToolResponse;
import org.jagentic.tools.grpc.ListToolsRequest;
import org.jagentic.tools.grpc.ListToolsResponse;
import org.jagentic.tools.grpc.ToolDescriptor;
import org.jagentic.tools.grpc.ToolService;

/** Drives the gRPC transport in-process: a {@code @GrpcClient} Mutiny stub reaches the
 * {@link ToolGrpcService} over the HTTP test port (use-separate-server=false). Confirms
 * ListTools surfaces {@code util_add} and CallTool computes 40 + 2 = 42. */
@QuarkusTest
class ToolGrpcServiceTest {

  @GrpcClient
  ToolService rawToolService;

  ToolService toolService;

  @org.junit.jupiter.api.BeforeEach
  void attachToken() {
    Metadata md = new Metadata();
    md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
        "Bearer test-tool-services-token");
    toolService = GrpcClientUtils.attachHeaders(rawToolService, md);
  }

  @Test
  void callWithoutTokenIsUnauthenticated() {
    StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
        rawToolService.listTools(ListToolsRequest.newBuilder().build())
            .await().atMost(Duration.ofSeconds(10)));
    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void callWithWrongTokenIsUnauthenticated() {
    Metadata md = new Metadata();
    md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
        "Bearer " + java.util.UUID.randomUUID());
    ToolService wrong = GrpcClientUtils.attachHeaders(rawToolService, md);
    StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
        wrong.callTool(CallToolRequest.newBuilder().setName("util_add")
            .setArgsJson("{\"a\":1,\"b\":2}").build())
            .await().atMost(Duration.ofSeconds(10)));
    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listToolsIncludesUtilAdd() {
    ListToolsResponse resp =
        toolService.listTools(ListToolsRequest.newBuilder().build())
            .await().atMost(Duration.ofSeconds(10));

    List<ToolDescriptor> tools = resp.getToolsList();
    assertTrue(tools.stream().anyMatch(t -> t.getName().equals("util_add")),
        "ListTools should include util_add; got: "
            + tools.stream().map(ToolDescriptor::getName).toList());

    ToolDescriptor add = tools.stream()
        .filter(t -> t.getName().equals("util_add")).findFirst().orElseThrow();
    // The input schema is carried as a JSON string; util_add declares numeric params.
    assertTrue(add.getInputSchemaJson().contains("\"type\""),
        "input_schema_json should be a JSON object: " + add.getInputSchemaJson());
  }

  @Test
  void callToolAddsTwoNumbers() {
    CallToolResponse resp = toolService.callTool(
        CallToolRequest.newBuilder().setName("util_add")
            .setArgsJson("{\"a\":40,\"b\":2}").build())
        .await().atMost(Duration.ofSeconds(10));

    assertTrue(resp.getOk(), "CallTool should succeed; error=" + resp.getError());
    // result_json is a serialized JSON number (40 + 2 = 42, possibly 42.0).
    assertTrue(resp.getResultJson().startsWith("42"),
        "result_json should be 42, was: " + resp.getResultJson());
  }

  @Test
  void callUnknownToolReturnsError() {
    CallToolResponse resp = toolService.callTool(
        CallToolRequest.newBuilder().setName("does_not_exist")
            .setArgsJson("{}").build())
        .await().atMost(Duration.ofSeconds(10));

    assertFalse(resp.getOk(), "unknown tool should report ok=false");
    assertTrue(resp.getError().contains("does_not_exist"),
        "error should name the missing tool: " + resp.getError());
    assertEquals("", resp.getResultJson());
  }
}

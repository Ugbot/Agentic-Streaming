package org.agentic.flink.example.banking.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Verifies {@link EnvApiClient}/{@link EnvApiToolExecutor} against a stub harness env API. */
class EnvApiToolExecutorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HttpServer server;
  private String baseUrl;
  private final ConcurrentLinkedQueue<String[]> requests = new ConcurrentLinkedQueue<>(); // {method, path, auth, body}

  @BeforeEach
  void startStub() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/sessions/",
        exchange -> {
          String path = exchange.getRequestURI().getPath();
          String auth = exchange.getRequestHeaders().getFirst("Authorization");
          byte[] reqBody;
          try (InputStream in = exchange.getRequestBody()) {
            reqBody = in.readAllBytes();
          }
          requests.add(
              new String[] {
                exchange.getRequestMethod(), path, auth, new String(reqBody, StandardCharsets.UTF_8)
              });
          byte[] resp;
          if (path.endsWith("/tools")) {
            resp =
                MAPPER.writeValueAsBytes(
                    Map.of(
                        "tools",
                        List.of(
                            Map.of(
                                "type", "function",
                                "function",
                                Map.of("name", "submit_referral", "description", "Submit a referral")))));
          } else {
            // tool call: echo the path + body so the test can assert routing.
            resp =
                MAPPER.writeValueAsBytes(
                    Map.of("error", false, "content", "ok:" + path));
          }
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          exchange.getResponseBody().write(resp);
          exchange.close();
        });
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopStub() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  @DisplayName("listTools hits /sessions/{cid}/tools with bearer auth and parses schemas")
  void listTools() {
    EnvApiClient client = new EnvApiClient(baseUrl, "dev-token", 5000);
    String cid = "ctx-" + UUID.randomUUID();
    List<Map<String, Object>> tools = client.listTools(cid);
    assertEquals(1, tools.size());
    assertEquals("submit_referral", ((Map<?, ?>) tools.get(0).get("function")).get("name"));

    String[] req = requests.poll();
    assertEquals("GET", req[0]);
    assertEquals("/sessions/" + cid + "/tools", req[1]);
    assertEquals("Bearer dev-token", req[2]);
  }

  @Test
  @DisplayName("per-tool executor calls /sessions/{cid}/tools/{name} with the bound contextId")
  void perToolExecutorRoutesByContext() throws Exception {
    EnvApiClient client = new EnvApiClient(baseUrl, "dev-token", 5000);
    EnvApiToolExecutor exec = new EnvApiToolExecutor(client, "submit_referral", "Submit a referral");
    String cid = "ctx-" + UUID.randomUUID();

    Object result =
        EnvSession.withContext(cid, () -> exec.execute(Map.of("account_type", "Blue Account")).join());

    assertEquals(Boolean.FALSE, ((Map<?, ?>) result).get("error"));
    String[] req = requests.poll();
    assertEquals("POST", req[0]);
    assertEquals("/sessions/" + cid + "/tools/submit_referral", req[1]);
    assertEquals("Bearer dev-token", req[2]);
    assertTrue(req[3].contains("\"arguments\""), req[3]);
    assertTrue(req[3].contains("Blue Account"), req[3]);
  }

  @Test
  @DisplayName("generic fallback routes by tool_name + arguments_json")
  void fallbackExecutor() throws Exception {
    EnvApiClient client = new EnvApiClient(baseUrl, "dev-token", 5000);
    EnvApiToolExecutor fallback = EnvApiToolExecutor.fallback(client);
    assertEquals("call_env_tool", fallback.getToolId());
    String cid = "ctx-" + UUID.randomUUID();

    EnvSession.withContext(
        cid,
        () ->
            fallback
                .execute(Map.of("tool_name", "lookup_account", "arguments_json", "{\"id\":\"x1\"}"))
                .join());

    String[] req = requests.poll();
    assertEquals("/sessions/" + cid + "/tools/lookup_account", req[1]);
    assertTrue(req[3].contains("x1"), req[3]);
  }

  @Test
  @DisplayName("no bound contextId yields a tool error, never a fabricated session")
  void missingContextIsError() throws Exception {
    EnvApiClient client = new EnvApiClient(baseUrl, "dev-token", 5000);
    EnvApiToolExecutor exec = new EnvApiToolExecutor(client, "submit_referral", "x");
    EnvSession.clear();
    Object result = exec.execute(Map.of()).join();
    assertEquals(Boolean.TRUE, ((Map<?, ?>) result).get("error"));
    assertTrue(requests.isEmpty(), "must not call the env API without a contextId");
  }
}

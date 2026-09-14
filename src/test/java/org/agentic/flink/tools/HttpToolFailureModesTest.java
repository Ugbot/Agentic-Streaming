package org.agentic.flink.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.tools.mcp.McpClient;
import org.agentic.flink.tools.mcp.McpServerSpec;
import org.agentic.flink.web.WebFetchTool;
import org.agentic.flink.net.OutboundUrlPolicy;
import org.agentic.flink.web.WebToolkitOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * F5: HTTP-backed tools bound connect/request time and treat non-2xx responses as failures
 * instead of parsing the body.
 */
class HttpToolFailureModesTest {

  private HttpServer server;
  private final CountDownLatch release = new CountDownLatch(1);

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/status", ex -> {
      int status = Integer.parseInt(ex.getRequestURI().getQuery().substring("code=".length()));
      byte[] body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"looks\":\"valid\"}}".getBytes(StandardCharsets.UTF_8);
      ex.getResponseHeaders().add("Content-Type", "application/json");
      ex.sendResponseHeaders(status, body.length);
      ex.getResponseBody().write(body);
      ex.close();
    });
    server.createContext("/slow", ex -> {
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
      ex.sendResponseHeaders(200, -1);
      ex.close();
    });
    server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
    server.start();
  }

  @AfterEach
  void stop() {
    release.countDown();
    server.stop(0);
  }

  private String url(String path) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + path;
  }

  @Test
  void mcpHttpRejectsNon2xxEvenWhenBodyLooksLikeAValidResponse() {
    int status = List.of(400, 401, 403, 404, 429, 500, 502, 503).get(ThreadLocalRandom.current().nextInt(8));
    McpServerSpec spec = McpServerSpec.http("s", url("/status?code=" + status));
    try (McpClient client = new McpClient(spec)) {
      IOException e = assertThrows(IOException.class, client::initialize);
      assertTrue(e.getMessage().startsWith("HTTP " + status), e.getMessage());
    }
  }

  @Test
  void mcpHttpRequestTimeoutIsConfigurableAndEnforced() {
    long timeoutMs = ThreadLocalRandom.current().nextLong(100, 500);
    McpServerSpec spec = McpServerSpec.builder().withName("s").withTransport(McpServerSpec.Transport.HTTP)
        .withUrl(url("/slow")).withRequestTimeout(Duration.ofMillis(timeoutMs))
        .withConnectTimeout(Duration.ofSeconds(1)).build();
    assertEquals(Duration.ofMillis(timeoutMs), spec.getRequestTimeout());
    assertEquals(McpServerSpec.DEFAULT_REQUEST_TIMEOUT, McpServerSpec.http("d", url("/x")).getRequestTimeout());
    assertEquals(McpServerSpec.DEFAULT_CONNECT_TIMEOUT, McpServerSpec.http("d", url("/x")).getConnectTimeout());
    assertThrows(IllegalArgumentException.class, () -> McpServerSpec.builder().withName("s")
        .withTransport(McpServerSpec.Transport.HTTP).withUrl(url("/x")).withRequestTimeout(Duration.ZERO).build());

    try (McpClient client = new McpClient(spec)) {
      long started = System.nanoTime();
      IOException e = assertThrows(IOException.class, client::initialize);
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      assertInstanceOf(HttpTimeoutException.class, e);
      assertTrue(elapsed < timeoutMs + 5_000, "gave up after the request timeout: " + elapsed + "ms");
    }
  }

  @Test
  void webFetchToolReportsNon2xxAsNotOkWithoutExtracting() throws Exception {
    int status = List.of(404, 410, 500, 503).get(ThreadLocalRandom.current().nextInt(4));
    WebFetchTool tool = new WebFetchTool(WebToolkitOptions.defaults().withRespectRobots(false).withUrlPolicy(OutboundUrlPolicy.defaults().allowingPrivateAddresses()));
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) tool.execute(Map.of("url", url("/status?code=" + status)))
        .get(10, TimeUnit.SECONDS);
    assertEquals(false, result.get("ok"));
    assertEquals(status, result.get("status"));
    assertFalse(result.containsKey("text"), "body of a failed response is not extracted");
  }

  @Test
  void webFetchToolTimesOutOnSlowServer() throws Exception {
    long timeoutMs = ThreadLocalRandom.current().nextLong(100, 500);
    WebFetchTool tool = new WebFetchTool(WebToolkitOptions.defaults().withRespectRobots(false).withUrlPolicy(OutboundUrlPolicy.defaults().allowingPrivateAddresses())
        .withFetchTimeout(Duration.ofMillis(timeoutMs)));
    long started = System.nanoTime();
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) tool.execute(Map.of("url", url("/slow")))
        .get(10, TimeUnit.SECONDS);
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    assertEquals(false, result.get("ok"));
    assertTrue(String.valueOf(result.get("error")).toLowerCase().contains("timed out"), String.valueOf(result));
    assertTrue(elapsed < timeoutMs + 5_000, "gave up after the fetch timeout: " + elapsed + "ms");
  }
}

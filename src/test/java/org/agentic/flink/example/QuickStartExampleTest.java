package org.agentic.flink.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import dev.langchain4j.http.client.HttpClientBuilderFactory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/** The quick start tells the truth: a failed model call is a failed run. */
class QuickStartExampleTest {

  private static final String REPLY =
      "{\"model\":\"m\",\"message\":{\"role\":\"assistant\",\"content\":\"Flink is a stream"
          + " processor.\"},\"done\":true}";

  private record Run(int exit, String output) {}

  private static Run against(int status, String body) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        e -> {
          e.getRequestBody().readAllBytes();
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          e.sendResponseHeaders(status, bytes.length);
          e.getResponseBody().write(bytes);
          e.close();
        });
    server.start();
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try (PrintStream out = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
      int exit = QuickStartExample.run("http://127.0.0.1:" + server.getAddress().getPort(), out);
      return new Run(exit, captured.toString(StandardCharsets.UTF_8));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void aWorkingModelExitsZeroAndSaysSo() throws Exception {
    Run run = against(200, REPLY);
    assertEquals(0, run.exit(), run.output());
    assertTrue(run.output().contains("stream processor"), run.output());
    assertTrue(run.output().contains(QuickStartExample.SUCCESS_BANNER), run.output());
  }

  @Test
  void aFailedModelCallExitsNonZeroWithNoSuccessBanner() throws Exception {
    Run run = against(500, "{\"error\":\"model not found\"}");
    assertNotEquals(0, run.exit(), run.output());
    assertTrue(run.output().contains("FAILED"), run.output());
    assertFalse(run.output().contains(QuickStartExample.SUCCESS_BANNER), run.output());
  }

  /** The baseline run failed with "No HTTP client has been found in the classpath". */
  @Test
  void anHttpClientIsOnTheClasspath() {
    assertTrue(
        ServiceLoader.load(HttpClientBuilderFactory.class).findFirst().isPresent(),
        "langchain4j needs an HTTP client implementation at run time");
  }
}

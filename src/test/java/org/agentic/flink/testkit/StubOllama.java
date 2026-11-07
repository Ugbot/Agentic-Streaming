package org.agentic.flink.testkit;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A local server speaking Ollama's chat format, shared across JVM threads of a mini cluster. It
 * proves that a document's model is called, not that any model answers well.
 */
public final class StubOllama implements AutoCloseable {
  private final HttpServer server;
  private final List<String> bodies = new CopyOnWriteArrayList<>();

  public StubOllama(String answer) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    byte[] reply =
        ("{\"message\":{\"content\":\"{\\\"text\\\":\\\"" + answer + "\\\"}\"}}")
            .getBytes(StandardCharsets.UTF_8);
    server.createContext(
        "/api/chat",
        e -> {
          bodies.add(new String(e.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          e.sendResponseHeaders(200, reply.length);
          e.getResponseBody().write(reply);
          e.close();
        });
    server.start();
  }

  /** The {@code llm:} block of a document whose model is this server. */
  public Map<String, Object> llm() {
    return Map.of(
        "provider",
        "ollama",
        "model",
        "some-model",
        "base_url",
        "http://127.0.0.1:" + server.getAddress().getPort());
  }

  public List<String> requests() {
    return bodies;
  }

  @Override
  public void close() {
    server.stop(0);
  }
}

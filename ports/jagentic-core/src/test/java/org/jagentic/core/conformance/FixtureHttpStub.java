package org.jagentic.core.conformance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * The local server the fixtures' {@code {{http}}} tools talk to, serving the routes of {@code
 * spec/tools/http_stub.py}: {@code /ok}, {@code /status/<code>}, {@code /hang} and {@code /echo}.
 * It stands in for the outside world.
 */
public final class FixtureHttpStub implements AutoCloseable {
  private final HttpServer server;

  public FixtureHttpStub() {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    server.setExecutor(Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "fixture-http-stub");
      t.setDaemon(true);
      return t;
    }));
    server.createContext("/", FixtureHttpStub::handle);
    server.start();
  }

  public String base() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private static void handle(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    String path = exchange.getRequestURI().getPath();
    if (path.equals("/hang")) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      answer(exchange, 200, "{\"ok\":true}");
    } else if (path.equals("/echo")) {
      answer(exchange, 200, "{\"key\":\"" + exchange.getRequestHeaders().getFirst("Idempotency-Key") + "\"}");
    } else if (path.startsWith("/status/")) {
      answer(exchange, Integer.parseInt(path.substring("/status/".length())), "{\"error\":\"stub\"}");
    } else {
      answer(exchange, 200, "{\"ok\":true}");
    }
  }

  private static void answer(HttpExchange exchange, int code, String body) throws IOException {
    byte[] raw = body.getBytes(StandardCharsets.UTF_8);
    try (exchange) {
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(code, raw.length);
      exchange.getResponseBody().write(raw);
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}

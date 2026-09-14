package org.agentic.flink.example.banking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The personal agent's outbound message/send client must authenticate to the CS gateway. */
final class BankingCsClientAuthTest {

  private static String[] ask(String token) throws Exception {
    AtomicReference<String> seenAuth = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        ex -> {
          seenAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
          byte[] body =
              ("{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{\"kind\":\"message\",\"parts\":"
                      + "[{\"kind\":\"text\",\"text\":\"pong\"}]}}")
                  .getBytes(StandardCharsets.UTF_8);
          ex.sendResponseHeaders(200, body.length);
          ex.getResponseBody().write(body);
          ex.close();
        });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
      String reply =
          BankingA2AServer.httpCsClient(url, token).ask("ctx-" + UUID.randomUUID(), "ping");
      return new String[] {seenAuth.get(), reply};
    } finally {
      server.stop(0);
    }
  }

  @Test
  @DisplayName("configured CS_AGENT_TOKEN is sent as a Bearer header")
  void sendsBearer() throws Exception {
    String token = UUID.randomUUID().toString();
    String[] r = ask(token);
    assertEquals("Bearer " + token, r[0]);
    assertEquals("pong", r[1]);
  }

  @Test
  @DisplayName("no token configured: no Authorization header is invented")
  void omitsHeaderWhenUnset() throws Exception {
    assertNull(ask(null)[0]);
    assertNull(ask("  ")[0]);
  }
}

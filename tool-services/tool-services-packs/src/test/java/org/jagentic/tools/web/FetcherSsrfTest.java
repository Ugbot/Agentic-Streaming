package org.jagentic.tools.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.jagentic.tools.web.OutboundUrlPolicy.BlockedUrlException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SSRF regression tests for {@link Fetcher} and {@link WebFetchTool}. A single loopback server
 * plays two roles: addressed as {@code 127.0.0.1} it is treated as a public host by a pinned
 * resolver, while {@code localhost} resolves normally and therefore counts as an internal target.
 */
class FetcherSsrfTest {

  private HttpServer server;
  private String base;
  private final AtomicInteger hits = new AtomicInteger();

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/robots.txt", ex -> respond(ex, 200, "User-agent: *\nAllow: /\n", null));
    server.createContext(
        "/secret", ex -> {
          hits.incrementAndGet();
          respond(ex, 200, "<html><title>internal</title>leaked</html>", null);
        });
    server.createContext(
        "/bounce-internal",
        ex -> respond(ex, 302, "", "http://localhost:" + port() + "/secret"));
    server.createContext(
        "/bounce-public", ex -> respond(ex, 301, "", "http://127.0.0.1:" + port() + "/ok"));
    server.createContext(
        "/loop", ex -> respond(ex, 307, "", "http://127.0.0.1:" + port() + "/loop"));
    server.createContext("/ok", ex -> respond(ex, 200, "<html><title>fine</title>ok</html>", null));
    server.start();
    base = "http://127.0.0.1:" + port();
  }

  private int port() {
    return server.getAddress().getPort();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private static void respond(HttpExchange ex, int status, String body, String location)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    if (location != null) {
      ex.getResponseHeaders().add("Location", location);
    }
    ex.getResponseHeaders().add("Content-Type", "text/html");
    ex.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      if (bytes.length > 0) {
        os.write(bytes);
      }
    }
  }

  /** Resolver that pretends the literal {@code 127.0.0.1} is a public address. */
  private OutboundUrlPolicy publicLookingPolicy() {
    return OutboundUrlPolicy.defaults()
        .withMaxRedirects(3)
        .withResolver(
            h -> {
              if (h.equals("127.0.0.1")) {
                return new InetAddress[] {InetAddress.getByName("93.184.216.34")};
              }
              return InetAddress.getAllByName(h);
            });
  }

  private Fetcher fetcher(OutboundUrlPolicy policy) {
    return new Fetcher(WebToolkitOptions.defaults().withUrlPolicy(policy).withRespectRobots(false));
  }

  @Test
  void loopbackUrlIsBlockedBeforeAnyConnection() {
    Fetcher f = fetcher(OutboundUrlPolicy.defaults());
    assertThrows(BlockedUrlException.class, () -> f.fetch(base + "/secret"));
    assertThrows(BlockedUrlException.class, () -> f.fetch("http://localhost:" + port() + "/secret"));
    assertThrows(BlockedUrlException.class, () -> f.fetch("http://[::1]:" + port() + "/secret"));
    assertThrows(BlockedUrlException.class, () -> f.fetch("file:///etc/hostname"));
    assertEquals(0, hits.get(), "no request must reach the internal endpoint");
  }

  @Test
  void redirectToInternalAddressIsBlocked() {
    Fetcher f = fetcher(publicLookingPolicy());
    assertThrows(BlockedUrlException.class, () -> f.fetch(base + "/bounce-internal"));
    assertEquals(0, hits.get(), "redirect target must not be followed");
  }

  @Test
  void redirectToPublicAddressIsFollowed() throws IOException {
    Fetcher.FetchResult r = fetcher(publicLookingPolicy()).fetch(base + "/bounce-public");
    assertTrue(r.isOk());
    assertEquals(base + "/ok", r.getFinalUrl());
  }

  @Test
  void redirectLoopIsCapped() {
    IOException e =
        assertThrows(IOException.class, () -> fetcher(publicLookingPolicy()).fetch(base + "/loop"));
    assertTrue(e.getMessage().contains("too many redirects"), e.getMessage());
  }

  @Test
  void redirectsAreNotFollowedWhenDisabled() throws IOException {
    Fetcher f =
        new Fetcher(
            WebToolkitOptions.defaults()
                .withUrlPolicy(publicLookingPolicy())
                .withRespectRobots(false)
                .withFollowRedirects(false));
    Fetcher.FetchResult r = f.fetch(base + "/bounce-internal");
    assertEquals(302, r.getStatus());
    assertEquals(0, hits.get());
  }

  @Test
  void webFetchToolReportsBlockedUrlAsFailure() {
    WebFetchTool tool = new WebFetchTool(fetcher(OutboundUrlPolicy.defaults()), new DocumentExtractor());
    @SuppressWarnings("unchecked")
    Map<String, Object> r =
        (Map<String, Object>)
            tool.execute(Map.of("url", "http://169.254.169.254/latest/meta-data/")).join();
    assertEquals(false, r.get("ok"));
    assertTrue(String.valueOf(r.get("error")).contains("non-public"), String.valueOf(r));
    assertFalse(hits.get() > 0);
  }

  @Test
  void devPolicyStillAllowsLocalTargets() throws IOException {
    Fetcher f = fetcher(OutboundUrlPolicy.defaults().allowingPrivateAddresses());
    Fetcher.FetchResult r = f.fetch("http://127.0.0.1:" + port() + "/ok");
    assertTrue(r.isOk());
  }
}

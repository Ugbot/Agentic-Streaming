package org.jagentic.tools.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jagentic.core.ToolRegistry;

/** The web+Tika pack against a local in-process HttpServer (no network): fetch/links/crawl
 * honour robots, and doc_extract pulls text via the HTML (Jsoup) and Tika branches. */
class WebDocumentPackTest {

  private static final String PAGE =
      "<html><head><title>Acme Bank</title></head><body><h1>Welcome</h1>"
          + "<p>Open a savings account today.</p>"
          + "<a href='/cards'>Cards</a> <a href='https://example.org/help'>Help</a></body></html>";

  private HttpServer server;
  private String base;
  private final AtomicReference<String> robots = new AtomicReference<>("User-agent: *\nAllow: /\n");

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/robots.txt", ex -> respond(ex, robots.get(), "text/plain"));
    server.createContext("/", ex -> respond(ex, PAGE, "text/html; charset=utf-8"));
    server.start();
    base = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private static void respond(com.sun.net.httpserver.HttpExchange ex, String body, String ct) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", ct);
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  private ToolRegistry reg() {
    ToolRegistry reg = new ToolRegistry();
    new WebDocumentPack().register(reg);
    return reg;
  }

  @Test
  @SuppressWarnings("unchecked")
  void webFetchReturnsTitleTextAndLinks() {
    Map<String, Object> r = (Map<String, Object>) reg().execute("web_fetch", Map.of("url", base + "/page"));
    assertEquals(true, r.get("ok"));
    assertEquals("Acme Bank", r.get("title"));
    assertTrue(((String) r.get("text")).contains("savings account"));
    assertTrue(((List<String>) r.get("links")).contains("https://example.org/help"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void webLinksReturnsLinks() {
    Map<String, Object> r = (Map<String, Object>) reg().execute("web_links", Map.of("url", base + "/page"));
    List<String> links = (List<String>) r.get("links");
    assertTrue(links.contains(base + "/cards"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void webCrawlIsBoundedAndStaysOnHost() {
    Map<String, Object> r = (Map<String, Object>) reg().execute("web_crawl",
        Map.of("url", base + "/start", "max_pages", 5, "max_depth", 1, "same_host", true));
    int count = ((Number) r.get("page_count")).intValue();
    assertTrue(count >= 1 && count <= 5, "page_count=" + count);
    List<Map<String, Object>> pages = (List<Map<String, Object>>) r.get("pages");
    // every crawled page is on the seed host (the external example.org link is not followed)
    assertTrue(pages.stream().allMatch(p -> ((String) p.get("url")).startsWith(base)));
  }

  @Test
  void robotsDisallowBlocksFetch() {
    robots.set("User-agent: *\nDisallow: /secret\n");
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) reg().execute("web_fetch", Map.of("url", base + "/secret"));
    assertEquals(false, r.get("ok"));
    assertEquals(true, r.get("disallowed"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void docExtractHtmlViaJsoup() {
    Map<String, Object> r = (Map<String, Object>) reg().execute("doc_extract",
        Map.of("text", PAGE, "content_type", "text/html"));
    assertEquals(true, r.get("ok"));
    assertEquals("Acme Bank", r.get("title"));
    assertTrue(((String) r.get("text")).contains("savings account"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void docExtractPlainTextViaTika() {
    String payload = "Quarterly revenue grew by twelve percent.";
    String b64 = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    Map<String, Object> r = (Map<String, Object>) reg().execute("doc_extract",
        Map.of("content_base64", b64, "content_type", "text/plain"));
    assertEquals(true, r.get("ok"));
    assertTrue(((String) r.get("text")).contains("twelve percent"));
    assertFalse(((String) r.get("text")).contains("<"));
  }
}

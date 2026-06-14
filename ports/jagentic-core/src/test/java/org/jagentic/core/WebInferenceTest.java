package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Test;

import org.jagentic.core.inference.ClassifierGuardrail;
import org.jagentic.core.inference.ClassifierScorer;
import org.jagentic.core.inference.EmbeddingClassifier;
import org.jagentic.core.inference.LexiconClassifier;
import org.jagentic.core.web.WebFetcher;

/** Phase E — web toolkit (robots-aware fetch + link extraction against an in-process
 * HttpServer) and the DL inference SPI (lexicon + nearest-centroid classifiers, classifier
 * guardrail). All offline/deterministic. */
class WebInferenceTest {

  private static final String PAGE =
      "<html><head><title>Acme Bank</title><style>.x{color:red}</style></head>"
          + "<body><h1>Welcome</h1><p>Open a savings account today.</p>"
          + "<script>var x=1;</script>"
          + "<a href='/cards'>Cards</a> <a href='https://example.org/help'>Help</a>"
          + "</body></html>";

  private static HttpServer startServer(AtomicReference<String> robots) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/robots.txt", ex -> respond(ex, robots.get(), "text/plain"));
    server.createContext("/", ex -> respond(ex, PAGE, "text/html; charset=utf-8"));
    server.start();
    return server;
  }

  private static void respond(com.sun.net.httpserver.HttpExchange ex, String body, String ct) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", ct);
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  @Test
  void webFetchExtractsTextAndLinks() throws IOException {
    AtomicReference<String> robots = new AtomicReference<>("User-agent: *\nAllow: /\n");
    HttpServer server = startServer(robots);
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort();
      WebFetcher.FetchResult r = new WebFetcher().fetch(base + "/page");
      assertEquals("Acme Bank", r.title());
      assertTrue(r.text().contains("Open a savings account today."), r.text());
      assertFalse(r.text().contains("var x"));
      assertFalse(r.text().contains("color:red"));
      assertTrue(r.links().contains(base + "/cards"), r.links().toString());
      assertTrue(r.links().contains("https://example.org/help"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void webToolsRegisterAndRun() throws IOException {
    AtomicReference<String> robots = new AtomicReference<>("User-agent: *\nAllow: /\n");
    HttpServer server = startServer(robots);
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort();
      ToolRegistry reg = new ToolRegistry();
      List<String> ids = new WebFetcher().register(reg);
      assertTrue(ids.contains("web_fetch") && ids.contains("web_links"));
      String text = (String) reg.execute("web_fetch", Map.of("url", base + "/page"));
      assertTrue(text.contains("Acme Bank") && text.contains("savings account"));
      String links = (String) reg.execute("web_links", Map.of("url", base + "/page"));
      assertTrue(links.contains("https://example.org/help"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void robotsDisallowBlocksFetch() throws IOException {
    AtomicReference<String> robots = new AtomicReference<>("User-agent: *\nDisallow: /blocked\n");
    HttpServer server = startServer(robots);
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort();
      WebFetcher f = new WebFetcher();
      assertThrows(SecurityException.class, () -> f.fetch(base + "/blocked"));
      assertEquals("Acme Bank", f.fetch(base + "/page").title());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void lexiconClassifierLabelsByKeywords() {
    LexiconClassifier clf = new LexiconClassifier(Map.of(
        "billing", List.of("invoice", "charge", "refund", "payment"),
        "tech", List.of("error", "crash", "bug", "login")), "other");
    assertEquals("billing", clf.classify("I want a refund for this charge").label());
    assertEquals("tech", clf.classify("the app keeps showing a login error").label());
    assertEquals("other", clf.classify("hello there").label());
    double sum = clf.classify("refund my payment please").scores().values().stream().mapToDouble(d -> d).sum();
    assertTrue(Math.abs(sum - 1.0) < 1e-9, "scores sum=" + sum);
  }

  @Test
  void embeddingClassifierNearestCentroid() {
    EmbeddingClassifier clf = new EmbeddingClassifier().fit(Map.of(
        "billing", List.of("refund my invoice", "dispute a charge", "payment failed"),
        "tech", List.of("app crashed on login", "error message bug", "cannot sign in")));
    assertEquals("billing", clf.classify("please refund this invoice charge").label());
    assertEquals("tech", clf.classify("login error keeps crashing").label());
    double sum = clf.classify("refund invoice").scores().values().stream().mapToDouble(d -> d).sum();
    assertTrue(Math.abs(sum - 1.0) < 1e-6, "scores sum=" + sum);
  }

  @Test
  void classifierGuardrailBlocksLabel() {
    LexiconClassifier clf = new LexiconClassifier(Map.of(
        "toxic", List.of("idiot", "stupid", "hate"),
        "ok", List.of("please", "thanks", "help")), "ok");
    ClassifierGuardrail guard = new ClassifierGuardrail(clf, List.of("toxic"), 0.3, null, false);
    assertNotNull(guard.checkInput("you stupid idiot"));
    assertNull(guard.checkInput("please help, thanks"));
    ClassifierScorer scorer = new ClassifierScorer(clf, "toxic");
    assertTrue(scorer.score("idiot") > scorer.score("thanks please help"));
  }
}

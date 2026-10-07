package org.agentic.flink.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.net.OutboundUrlPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A deserialized {@link RobotsCache} (and everything that carries one: {@link Fetcher}, {@link
 * WebFetchTool}, {@link CrawlerCore.FetchAndExtractFn}) must still fetch and enforce robots.txt.
 * Before the fix the transient cache map was {@code null} after deserialization, the NPE was
 * swallowed and every URL was allowed.
 */
@Timeout(30)
class RobotsCacheSerializationTest {

  private HttpServer server;
  private String base;
  private String deniedPath;
  private String allowedPath;
  private final AtomicInteger robotsHits = new AtomicInteger();
  private final AtomicInteger deniedHits = new AtomicInteger();

  @BeforeEach
  void start() throws IOException {
    deniedPath = "/" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    allowedPath = "/" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/robots.txt",
        ex -> {
          robotsHits.incrementAndGet();
          respond(ex, "User-agent: *\nDisallow: " + deniedPath + "\n", "text/plain");
        });
    server.createContext(
        deniedPath,
        ex -> {
          deniedHits.incrementAndGet();
          respond(ex, "<html><title>denied</title>should not be fetched</html>", "text/html");
        });
    server.createContext(
        allowedPath, ex -> respond(ex, "<html><title>fine</title>ok</html>", "text/html"));
    server.start();
    base = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private static void respond(HttpExchange ex, String body, String contentType) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", contentType);
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  private WebToolkitOptions options() {
    return WebToolkitOptions.defaults()
        .withRespectRobots(true)
        .withFetchTimeout(Duration.ofSeconds(5))
        .withUrlPolicy(OutboundUrlPolicy.defaults().allowingPrivateAddresses());
  }

  @SuppressWarnings("unchecked")
  private static <S extends Serializable> S viaJava(S value) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(value);
    }
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return (S) ois.readObject();
    }
  }

  private static <S extends Serializable> S viaFlink(S value) throws Exception {
    byte[] bytes = InstantiationUtil.serializeObject(value);
    return InstantiationUtil.deserializeObject(bytes, value.getClass().getClassLoader());
  }

  @Test
  void freshCacheHonoursDisallow() {
    RobotsCache cache = new RobotsCache("agentic-test", Duration.ofSeconds(5));
    assertFalse(cache.isAllowed(base + deniedPath));
    assertTrue(cache.isAllowed(base + allowedPath));
    assertEquals(1, robotsHits.get(), "one robots.txt fetch per host");
    assertEquals(1, cache.cachedHosts());
  }

  @Test
  void deserializedCacheStillDeniesAfterJavaSerialization() throws Exception {
    RobotsCache original = new RobotsCache("agentic-test", Duration.ofSeconds(5));
    assertFalse(original.isAllowed(base + deniedPath));
    int hitsBefore = robotsHits.get();

    RobotsCache copy = viaJava(original);
    assertEquals(0, copy.cachedHosts(), "transient cache is not serialized");
    assertFalse(copy.isAllowed(base + deniedPath), "deserialized copy must not fail open");
    assertTrue(copy.isAllowed(base + allowedPath));
    assertEquals(hitsBefore + 1, robotsHits.get(), "copy refetches robots.txt once");
    assertEquals(1, copy.cachedHosts());
  }

  @Test
  void deserializedCacheStillDeniesAfterFlinkInstantiationUtil() throws Exception {
    RobotsCache copy = viaFlink(new RobotsCache("agentic-test", Duration.ofSeconds(5)));
    assertFalse(copy.isAllowed(base + deniedPath));
    assertTrue(copy.isAllowed(base + allowedPath));
    assertEquals(1, robotsHits.get());
  }

  @Test
  void deserializedFetcherReturnsDisallowedInsteadOfFetching() throws Exception {
    Fetcher original = new Fetcher(options());
    assertTrue(original.fetch(base + deniedPath).isDisallowed());

    for (Fetcher copy : List.of(viaJava(original), viaFlink(original))) {
      Fetcher.FetchResult denied = copy.fetch(base + deniedPath);
      assertTrue(denied.isDisallowed());
      assertEquals(403, denied.getStatus());
      Fetcher.FetchResult ok = copy.fetch(base + allowedPath);
      assertTrue(ok.isOk());
    }
    assertEquals(0, deniedHits.get(), "disallowed path must never be requested");
    assertEquals(3, robotsHits.get(), "original plus each copy fetched robots.txt");
  }

  @Test
  @SuppressWarnings("unchecked")
  void deserializedWebFetchToolReportsDisallowed() throws Exception {
    WebFetchTool tool = viaFlink(new WebFetchTool(options()));
    Map<String, Object> denied =
        (Map<String, Object>) tool.execute(Map.of("url", base + deniedPath)).join();
    assertEquals(false, denied.get("ok"), String.valueOf(denied));
    assertEquals(true, denied.get("disallowed"), String.valueOf(denied));
    Map<String, Object> ok =
        (Map<String, Object>) tool.execute(Map.of("url", base + allowedPath)).join();
    assertEquals(true, ok.get("ok"), String.valueOf(ok));
    assertEquals(0, deniedHits.get());
  }

  @Test
  void deserializedCrawlerOperatorSkipsDisallowedPages() throws Exception {
    CrawlerCore.FetchAndExtractFn fn = viaFlink(new CrawlerCore.FetchAndExtractFn(options()));
    fn.open(null);
    List<CrawledPage> out = new ArrayList<>();
    Collector<CrawledPage> collector =
        new Collector<>() {
          @Override
          public void collect(CrawledPage record) {
            out.add(record);
          }

          @Override
          public void close() {}
        };
    fn.processElement(new UrlRequest(base + deniedPath, "test"), null, collector);
    assertTrue(out.isEmpty(), "disallowed page must not be emitted");
    fn.processElement(new UrlRequest(base + allowedPath, "test"), null, collector);
    assertEquals(1, out.size());
    assertEquals(base + allowedPath, out.get(0).getFinalUrl());
    assertEquals(0, deniedHits.get());
  }
}

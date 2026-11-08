package org.jagentic.core.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.jagentic.core.A2AClient;
import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.llm.OpenAIChatClient;
import org.jagentic.core.pipeline.GraphBuilder;

/**
 * DNS rebinding between the check and the connect. The egress check resolved a host and found only
 * public addresses, and then the HTTP client resolved it again to connect, so a name whose answer
 * changed in between passed as public and was reached on loopback. Now a call resolves once, checks
 * every address, and connects to one it checked, with the host name kept for Host and for TLS.
 */
class DnsRebindingAttackTest {

  private final List<String> seen = new CopyOnWriteArrayList<>();
  private HttpServer internal;

  @BeforeEach
  void start() throws Exception {
    Assumptions.assumeTrue(java.net.InetAddress.getByName("probe.rebind.test") != null);
    internal = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    internal.createContext("/", x -> {
      seen.add(x.getRequestURI().getPath());
      byte[] body = "{\"text\":\"hi\",\"choices\":[{\"message\":{\"content\":\"{\\\"text\\\":\\\"hi\\\"}\"}}]}".getBytes();
      x.sendResponseHeaders(200, body.length);
      try (OutputStream out = x.getResponseBody()) {
        out.write(body);
      }
    });
    internal.start();
    RebindingResolverProvider.reset();
    System.setProperty("agentic.egress.allow_private", "false");
    System.setProperty("agentic.egress.allow_hosts", "");
    System.setProperty("agentic.http.connect_timeout_ms", "500"); // the public address does not answer
  }

  @AfterEach
  void stop() {
    internal.stop(0);
    System.setProperty("agentic.egress.allow_private", "true");
    System.setProperty("agentic.egress.allow_hosts", "127.0.0.1,localhost");
    System.clearProperty("agentic.http.connect_timeout_ms");
  }

  private String at(String name) {
    return "http://" + name + ".rebind.test:" + internal.getAddress().getPort();
  }

  @Test
  void anHttpToolDoesNotReachLoopbackThroughARebindingName() {
    Map<String, Object> doc = Map.of("spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "main"),
            "paths", Map.of("main", Map.of("brain", "rule", "tools", List.of("call"),
                "tool_triggers", Map.of("go", "call"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(Map.of("id", "call", "kind", "http", "effect", "idempotent",
            "url", at("tool") + "/", "timeout_ms", 1500)));
    GraphBuilder.Built built = GraphBuilder.build(doc, null);
    LocalRuntime rt = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, new ConversationLog.InMemory());
    TurnResult r = rt.submit(Event.turn("c", "t", "u", "go"));
    assertEquals(List.of(), seen, "the internal service was reached");
    assertNotEquals(TurnStatus.COMPLETED, r.status);
    assertEquals(1, RebindingResolverProvider.asked("tool.rebind.test"), "the name is resolved once");
  }

  @Test
  void aModelCallDoesNotReachLoopbackThroughARebindingName() {
    OpenAIChatClient model = new OpenAIChatClient("m", "k", at("model") + "/v1");
    assertThrows(RuntimeException.class, () -> model.chat(List.of(Map.of("role", "user", "content", "hi")), List.of()));
    assertEquals(List.of(), seen);
  }

  @Test
  void aPeerDoesNotReachLoopbackThroughARebindingName() {
    assertThrows(RuntimeException.class, () -> new A2AClient(at("peer"), 0).send("c", "hi", "u"));
    assertThrows(RuntimeException.class, () -> new A2AClient(at("card"), 0).card());
    assertEquals(List.of(), seen);
  }

  /** A pinned connection still speaks to the name: Host, SNI and the certificate are the host's. */
  @Test
  void aPinnedHttpsConnectionKeepsTheHostForSniHostAndTheCertificate(@TempDir Path dir) throws Exception {
    Path store = dir.resolve("tls.p12");
    Process keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
        "-genkeypair", "-alias", "s", "-keyalg", "RSA", "-keysize", "2048", "-validity", "1",
        "-dname", "CN=pinned.test", "-ext", "SAN=dns:pinned.test", "-storetype", "PKCS12",
        "-keystore", store.toString(), "-storepass", "changeit", "-keypass", "changeit")
        .redirectErrorStream(true).start();
    Assumptions.assumeTrue(keytool.waitFor() == 0, "keytool could not make a certificate");
    KeyStore ks = KeyStore.getInstance("PKCS12");
    try (FileInputStream in = new FileInputStream(store.toFile())) {
      ks.load(in, "changeit".toCharArray());
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, "changeit".toCharArray());
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);
    SSLContext server = SSLContext.getInstance("TLS");
    server.init(kmf.getKeyManagers(), null, null);
    SSLContext client = SSLContext.getInstance("TLS");
    client.init(null, tmf.getTrustManagers(), null);

    List<String> hosts = new CopyOnWriteArrayList<>();
    List<String> sni = new CopyOnWriteArrayList<>();
    HttpsServer https = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    https.setHttpsConfigurator(new HttpsConfigurator(server));
    https.createContext("/", x -> {
      hosts.add(x.getRequestHeaders().getFirst("Host"));
      ((ExtendedSSLSession) ((HttpsExchange) x).getSSLSession()).getRequestedServerNames()
          .forEach(n -> sni.add(new String(n.getEncoded())));
      byte[] body = "ok".getBytes();
      x.sendResponseHeaders(200, body.length);
      try (OutputStream out = x.getResponseBody()) {
        out.write(body);
      }
    });
    https.start();
    try {
      int port = https.getAddress().getPort();
      PinnedHttp.Response r = PinnedHttp.send("GET", URI.create("https://pinned.test:" + port + "/x"), Map.of(),
          null, Duration.ofSeconds(5), PinnedHttp.anyAddress(), client);
      assertEquals(200, r.status());
      assertEquals("ok", r.text());
      assertEquals(List.of("pinned.test:" + port), hosts);
      assertEquals(List.of("pinned.test"), sni);
      // The certificate is checked against the name, so another name with this certificate is refused.
      assertThrows(java.io.IOException.class, () -> PinnedHttp.send("GET",
          URI.create("https://other.test:" + port + "/x"), Map.of(), null, Duration.ofSeconds(5),
          PinnedHttp.anyAddress(), client));
      assertTrue(hosts.size() == 1);
    } finally {
      https.stop(0);
    }
  }
}

package org.agentic.flink.a2a.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** The JAX-RS filter guarding every gateway path (JSON-RPC, SSE and {@code /rag/*}) except the Agent Card. */
final class BearerAuthFilterTest {

  /** Minimal request context: path, headers, properties and the abort response. */
  private static final class Ctx {
    final String path;
    final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    final Map<String, Object> props = new HashMap<>();
    final AtomicReference<Response> aborted = new AtomicReference<>();

    Ctx(String path, String authorization) {
      this.path = path;
      if (authorization != null) {
        headers.putSingle("Authorization", authorization);
      }
    }

    ContainerRequestContext proxy() {
      UriInfo uri =
          (UriInfo)
              Proxy.newProxyInstance(
                  getClass().getClassLoader(),
                  new Class<?>[] {UriInfo.class},
                  (p, m, a) -> {
                    if (m.getName().equals("getPath")) {
                      return path;
                    }
                    throw new UnsupportedOperationException(m.getName());
                  });
      return (ContainerRequestContext)
          Proxy.newProxyInstance(
              getClass().getClassLoader(),
              new Class<?>[] {ContainerRequestContext.class},
              (p, m, a) -> {
                switch (m.getName()) {
                  case "getUriInfo":
                    return uri;
                  case "getHeaders":
                    return headers;
                  case "setProperty":
                    props.put((String) a[0], a[1]);
                    return null;
                  case "getProperty":
                    return props.get(a[0]);
                  case "abortWith":
                    aborted.set((Response) a[0]);
                    return null;
                  default:
                    throw new UnsupportedOperationException(m.getName());
                }
              });
    }
  }

  private static BearerAuthFilter filter(String tokensSpec, boolean devMode) {
    Map<String, String> cfg = new HashMap<>();
    if (tokensSpec != null) {
      cfg.put("a2a.auth.tokens", tokensSpec);
    }
    cfg.put("a2a.auth.dev.mode", String.valueOf(devMode));
    BearerAuthFilter f = new BearerAuthFilter();
    f.auth = new GatewayAuth(new GatewayConfig(AgenticFlinkConfig.fromMap(cfg)));
    return f;
  }

  @RepeatedTest(10)
  @DisplayName("valid tokens pass and expose the subject; missing or wrong tokens abort with 401")
  void tokensGateEveryProtectedPath() {
    String subject = "svc-" + UUID.randomUUID().toString().substring(0, 8);
    String token = UUID.randomUUID().toString();
    BearerAuthFilter f = filter(subject + "=" + token, false);
    for (String path : List.of("/", "rag/ingest", "/rag/query", "anything/else")) {
      Ctx ok = new Ctx(path, "Bearer " + token);
      f.filter(ok.proxy());
      assertNull(ok.aborted.get(), path);
      GatewayAuth.Principal p = (GatewayAuth.Principal) ok.props.get(GatewayAuth.REQUEST_PROPERTY);
      assertNotNull(p);
      assertEquals(subject, p.subject());

      for (String bad : new String[] {null, "", "Bearer " + UUID.randomUUID(), "Basic " + token, token, "Bearer"}) {
        Ctx denied = new Ctx(path, bad);
        f.filter(denied.proxy());
        Response r = denied.aborted.get();
        assertNotNull(r, path + " header=" + bad);
        assertEquals(401, r.getStatus());
        assertEquals("Bearer", r.getHeaderString("WWW-Authenticate"));
        assertNull(denied.props.get(GatewayAuth.REQUEST_PROPERTY));
      }
    }
  }

  @Test
  @DisplayName("the Agent Card and health probes stay public")
  void agentCardIsPublic() {
    BearerAuthFilter f = filter("t=" + UUID.randomUUID(), false);
    for (String path : List.of("/.well-known/agent-card.json", ".well-known/agent-card.json", "q/health")) {
      Ctx c = new Ctx(path, null);
      f.filter(c.proxy());
      assertNull(c.aborted.get(), path);
    }
  }

  @Test
  @DisplayName("no token configured: 401 for everyone, unless dev mode is explicitly on")
  void failsClosedWithoutTokens() {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        System.getenv("AGENTIC_A2A_TOKEN") == null, "AGENTIC_A2A_TOKEN is set in this environment");
    Ctx c = new Ctx("/", "Bearer " + UUID.randomUUID());
    filter(null, false).filter(c.proxy());
    assertEquals(401, c.aborted.get().getStatus());

    Ctx dev = new Ctx("/", null);
    filter(null, true).filter(dev.proxy());
    assertNull(dev.aborted.get());
    GatewayAuth.Principal p = (GatewayAuth.Principal) dev.props.get(GatewayAuth.REQUEST_PROPERTY);
    assertEquals(GatewayAuth.DEV_SUBJECT, p.subject());
    assertTrue(p.isDevMode());
  }

  @Test
  @DisplayName("claims forwarded to the job carry the subject only, never the token")
  void claimsNeverContainTheToken() {
    String token = UUID.randomUUID().toString();
    GatewayAuth auth = new GatewayAuth(new GatewayConfig(AgenticFlinkConfig.fromMap(Map.of("a2a.auth.tokens", token))));
    Map<String, Object> claims = auth.authenticate("Bearer " + token).claims();
    assertEquals(GatewayAuth.DEFAULT_SUBJECT, claims.get("subject"));
    assertTrue(claims.values().stream().noneMatch(v -> String.valueOf(v).contains(token)));
    assertThrows(IllegalArgumentException.class, () -> GatewayAuth.parse("alice="));
  }
}

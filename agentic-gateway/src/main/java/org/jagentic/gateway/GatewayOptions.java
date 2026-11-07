package org.jagentic.gateway;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.jagentic.gateway.auth.Authenticator;
import org.jagentic.gateway.auth.OidcAuthenticator;
import org.jagentic.gateway.auth.StaticTokens;

/**
 * How a gateway runs, from the environment. It fails closed: with no authentication configured it
 * does not start, it binds loopback unless told otherwise, and it will not bind anything else without
 * a TLS proxy in front of it, or with static tokens at all.
 */
public record GatewayOptions(String host, int port, Authenticator authenticator, Set<String> readerRoles,
                             Duration timeout) {

  /** A gateway on loopback with this authenticator: what a test or an embedding program starts. */
  public static GatewayOptions loopback(Authenticator authenticator) {
    return new GatewayOptions("127.0.0.1", 0, authenticator, Set.of(), Duration.ofSeconds(30));
  }

  /** The same options letting these roles read any conversation of their tenant. */
  public GatewayOptions withReaderRoles(String... roles) {
    return new GatewayOptions(host, port, authenticator, Set.of(roles), timeout);
  }

  public static GatewayOptions fromEnvironment(Map<String, String> env) {
    String host = env.getOrDefault("AGENTIC_GATEWAY_HOST", "127.0.0.1");
    boolean loopback = isLoopback(host);
    if (!loopback && !"true".equalsIgnoreCase(env.get("AGENTIC_GATEWAY_BEHIND_TLS_PROXY"))) {
      throw new IllegalArgumentException("the gateway speaks plain HTTP: bind " + host
          + " only behind a TLS proxy, and say so with AGENTIC_GATEWAY_BEHIND_TLS_PROXY=true");
    }
    Authenticator authenticator;
    if (env.get("AGENTIC_OIDC_ISSUER") != null) {
      String issuer = env.get("AGENTIC_OIDC_ISSUER").strip();
      if (issuer.isEmpty()) {
        throw new IllegalArgumentException("AGENTIC_OIDC_ISSUER is blank");
      }
      String audience = env.get("AGENTIC_OIDC_AUDIENCE");
      if (audience == null || audience.isBlank()) {
        throw new IllegalArgumentException("AGENTIC_OIDC_AUDIENCE is required: a token for anything"
            + " else at this issuer must not work here");
      }
      String jwks = env.getOrDefault("AGENTIC_OIDC_JWKS_URL", issuer + "/protocol/openid-connect/certs");
      requireHttpsUnlessLoopback(jwks);
      authenticator = new OidcAuthenticator(new OidcAuthenticator.Settings(issuer, audience, jwks,
          env.getOrDefault("AGENTIC_OIDC_TENANT_CLAIM", "tenant"),
          env.getOrDefault("AGENTIC_OIDC_ROLES_CLAIM", "realm_access.roles")));
    } else if (env.get("AGENTIC_GATEWAY_STATIC_TOKENS") != null) {
      if (!"true".equalsIgnoreCase(env.get("AGENTIC_GATEWAY_ALLOW_STATIC_TOKENS"))) {
        throw new IllegalArgumentException("static tokens are for local development: set"
            + " AGENTIC_GATEWAY_ALLOW_STATIC_TOKENS=true to use them");
      }
      if (!loopback) {
        throw new IllegalArgumentException("static tokens work on loopback only, not " + host);
      }
      authenticator = StaticTokens.parse(env.get("AGENTIC_GATEWAY_STATIC_TOKENS"));
    } else {
      throw new IllegalArgumentException("no authentication configured: set AGENTIC_OIDC_ISSUER, or"
          + " AGENTIC_GATEWAY_STATIC_TOKENS for local development");
    }
    String readers = env.getOrDefault("AGENTIC_GATEWAY_READER_ROLES", "");
    return new GatewayOptions(host, Integer.parseInt(env.getOrDefault("AGENTIC_GATEWAY_PORT", "8080")),
        authenticator, readers.isBlank() ? Set.of() : Set.of(readers.split("\\s*,\\s*")),
        Duration.ofSeconds(Long.parseLong(env.getOrDefault("AGENTIC_GATEWAY_TIMEOUT_SECONDS", "30"))));
  }

  /**
   * Whoever can change the keys can sign any token, so they come over TLS. Plain HTTP is for an
   * issuer on this machine, such as the Keycloak lab.
   */
  private static void requireHttpsUnlessLoopback(String url) {
    java.net.URI uri;
    try {
      uri = java.net.URI.create(url);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("AGENTIC_OIDC_JWKS_URL is not a url: " + url, e);
    }
    String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
    boolean local = uri.getHost() != null && isLoopback(uri.getHost());
    if (!scheme.equals("https") && !(scheme.equals("http") && local)) {
      throw new IllegalArgumentException("the issuer's keys must come over https, not " + url);
    }
  }

  private static boolean isLoopback(String host) {
    String h = host.toLowerCase(Locale.ROOT);
    if (h.equals("localhost")) {
      return true;
    }
    try {
      return InetAddress.getByName(h).isLoopbackAddress();
    } catch (UnknownHostException e) {
      return false;
    }
  }
}

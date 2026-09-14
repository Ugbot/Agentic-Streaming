package org.agentic.flink.a2a.gateway;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared bearer token authentication for every gateway endpoint except the public Agent Card.
 *
 * <p>Tokens come from {@link GatewayConfig#authTokens()}: one or more {@code subject=token} pairs, or
 * a single bare token whose subject is {@value #DEFAULT_SUBJECT}. The subject becomes the caller's
 * principal and is recorded as the owner of every task the caller creates, so ownership checks on
 * {@code tasks/*} are meaningful when several clients share the gateway.
 *
 * <p>Fail closed: with no tokens configured every request is rejected with 401 unless {@link
 * GatewayConfig#authDevMode()} is on, in which case unauthenticated callers act as the {@value
 * #DEV_SUBJECT} principal. Dev mode is never the default.
 */
@ApplicationScoped
public class GatewayAuth {

  private static final Logger LOG = LoggerFactory.getLogger(GatewayAuth.class);

  public static final String DEFAULT_SUBJECT = "a2a-client";
  public static final String DEV_SUBJECT = "dev-anonymous";
  /** Request property under which the filter stores the authenticated {@link Principal}. */
  public static final String REQUEST_PROPERTY = GatewayAuth.class.getName() + ".principal";

  /** Authenticated caller identity. */
  public static final class Principal {
    private final String subject;
    private final boolean devMode;

    Principal(String subject, boolean devMode) {
      this.subject = Objects.requireNonNull(subject, "subject");
      this.devMode = devMode;
    }

    public String subject() {
      return subject;
    }

    /** True when the caller was admitted only because dev mode is enabled. */
    public boolean isDevMode() {
      return devMode;
    }

    /** Claims forwarded to the Flink job. Never includes the raw token or header. */
    public Map<String, Object> claims() {
      Map<String, Object> claims = new LinkedHashMap<>();
      claims.put("subject", subject);
      claims.put("scheme", devMode ? "dev" : "Bearer");
      return Collections.unmodifiableMap(claims);
    }
  }

  /** Raised when the caller cannot be authenticated. Maps to HTTP 401. */
  public static final class Unauthorized extends RuntimeException {
    private static final long serialVersionUID = 1L;

    Unauthorized(String message) {
      super(message);
    }
  }

  /** Raised when an authenticated caller touches a resource it does not own. Maps to HTTP 403. */
  public static final class Forbidden extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public Forbidden(String message) {
      super(message);
    }
  }

  @Inject GatewayConfig config;

  private volatile Map<String, String> tokenToSubject;

  public GatewayAuth() {}

  public GatewayAuth(GatewayConfig config) {
    this.config = config;
  }

  private Map<String, String> tokens() {
    Map<String, String> t = tokenToSubject;
    if (t == null) {
      t = parse(config.authTokens());
      tokenToSubject = t;
      if (t.isEmpty()) {
        if (config.authDevMode()) {
          LOG.warn(
              "A2A gateway has no bearer token configured and a2a.auth.dev.mode=true: "
                  + "all callers are admitted as '{}'. Do not expose this instance.",
              DEV_SUBJECT);
        } else {
          LOG.error(
              "A2A gateway has no bearer token configured (a2a.auth.tokens / AGENTIC_A2A_TOKEN); "
                  + "every request will be rejected with 401 until one is set");
        }
      }
    }
    return t;
  }

  /** Parses {@code subject=token,subject2=token2} or a single bare token. */
  static Map<String, String> parse(String spec) {
    Map<String, String> out = new LinkedHashMap<>();
    if (spec == null || spec.isBlank()) {
      return out;
    }
    for (String entry : spec.split(",")) {
      String e = entry.trim();
      if (e.isEmpty()) {
        continue;
      }
      int eq = e.indexOf('=');
      if (eq > 0 && eq < e.length() - 1) {
        out.put(e.substring(eq + 1).trim(), e.substring(0, eq).trim());
      } else if (eq < 0) {
        out.put(e, DEFAULT_SUBJECT);
      } else {
        throw new IllegalArgumentException("malformed a2a.auth.tokens entry (expected subject=token)");
      }
    }
    return out;
  }

  public boolean isConfigured() {
    return !tokens().isEmpty();
  }

  /**
   * Authenticates the caller from the {@code Authorization} header.
   *
   * @throws Unauthorized when the header is missing or malformed, the token is unknown, or no token
   *     is configured and dev mode is off
   */
  public Principal authenticate(HttpHeaders headers) {
    String header = headers == null ? null : headers.getHeaderString(HttpHeaders.AUTHORIZATION);
    return authenticate(header);
  }

  public Principal authenticate(String authorizationHeader) {
    Map<String, String> known = tokens();
    if (known.isEmpty()) {
      if (config.authDevMode()) {
        return new Principal(DEV_SUBJECT, true);
      }
      throw new Unauthorized("gateway authentication is not configured");
    }
    if (authorizationHeader == null || authorizationHeader.isBlank()) {
      throw new Unauthorized("missing Authorization header");
    }
    int sp = authorizationHeader.indexOf(' ');
    if (sp <= 0 || !authorizationHeader.substring(0, sp).toLowerCase(Locale.ROOT).equals("bearer")) {
      throw new Unauthorized("Authorization scheme must be Bearer");
    }
    String presented = authorizationHeader.substring(sp + 1).trim();
    if (presented.isEmpty()) {
      throw new Unauthorized("empty bearer token");
    }
    byte[] presentedBytes = presented.getBytes(StandardCharsets.UTF_8);
    String subject = null;
    for (Map.Entry<String, String> e : known.entrySet()) {
      if (MessageDigest.isEqual(e.getKey().getBytes(StandardCharsets.UTF_8), presentedBytes)) {
        subject = e.getValue();
      }
    }
    if (subject == null) {
      throw new Unauthorized("invalid bearer token");
    }
    return new Principal(subject, false);
  }

  /** Ensures {@code owner} (a task's recorded owner) matches the caller, else throws {@link Forbidden}. */
  public static void requireOwner(Principal caller, String owner, String taskId) {
    if (owner == null || !owner.equals(caller.subject())) {
      throw new Forbidden("task " + taskId + " is not owned by the caller");
    }
  }
}

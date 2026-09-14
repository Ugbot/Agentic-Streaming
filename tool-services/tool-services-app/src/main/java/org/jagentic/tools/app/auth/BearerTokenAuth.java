package org.jagentic.tools.app.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.Optional;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Shared bearer token check for every tool-services transport (REST, MCP-HTTP, gRPC).
 *
 * <p>The token comes from {@code tools.auth.token} ({@code TOOL_SERVICES_TOKEN}). When it is unset
 * the service fails closed and rejects every call, unless {@code tools.auth.dev-mode}
 * ({@code TOOL_SERVICES_AUTH_DEV_MODE}) is explicitly {@code true}, which admits unauthenticated
 * callers for local development only.
 */
@ApplicationScoped
public class BearerTokenAuth {

  private static final Logger LOG = Logger.getLogger(BearerTokenAuth.class);

  @ConfigProperty(name = "tools.auth.token")
  Optional<String> token;

  @ConfigProperty(name = "tools.auth.dev-mode", defaultValue = "false")
  boolean devMode;

  public BearerTokenAuth() {}

  public BearerTokenAuth(String token, boolean devMode) {
    this.token = Optional.ofNullable(token);
    this.devMode = devMode;
  }

  @PostConstruct
  void logMode() {
    if (!isConfigured()) {
      if (devMode) {
        LOG.warn("tools.auth.token is unset and tools.auth.dev-mode=true: all callers admitted. "
            + "Do not expose this instance.");
      } else {
        LOG.error("tools.auth.token is unset (TOOL_SERVICES_TOKEN); every call will be rejected with 401");
      }
    }
  }

  public boolean isConfigured() {
    return token.isPresent() && !token.get().isBlank();
  }

  /** True when the Authorization header value authenticates the caller. */
  public boolean accepts(String authorizationHeader) {
    if (!isConfigured()) {
      return devMode;
    }
    if (authorizationHeader == null) {
      return false;
    }
    int sp = authorizationHeader.indexOf(' ');
    if (sp <= 0 || !authorizationHeader.substring(0, sp).toLowerCase(Locale.ROOT).equals("bearer")) {
      return false;
    }
    String presented = authorizationHeader.substring(sp + 1).trim();
    return MessageDigest.isEqual(
        presented.getBytes(StandardCharsets.UTF_8),
        token.get().trim().getBytes(StandardCharsets.UTF_8));
  }
}

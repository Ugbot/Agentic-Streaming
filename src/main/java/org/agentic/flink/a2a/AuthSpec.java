package org.agentic.flink.a2a;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Serializable authentication material for calling a remote A2A agent.
 *
 * <p>A2A conveys credentials via standard HTTP headers (never in the JSON-RPC body). This spec
 * captures the common schemes and renders them to the request headers the {@code A2AClient} sends:
 *
 * <ul>
 *   <li>{@link Scheme#NONE} — no auth header.
 *   <li>{@link Scheme#API_KEY} — a custom header ({@code headerName}: {@code credential}).
 *   <li>{@link Scheme#BEARER} — {@code Authorization: Bearer <credential>} (OAuth2/OIDC tokens).
 *   <li>{@link Scheme#BASIC} — {@code Authorization: Basic <credential>} (pre-encoded).
 * </ul>
 *
 * <p>The credential is typically supplied via config / env rather than hardcoded; treat it as a
 * secret and never log it.
 */
public final class AuthSpec implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum Scheme {
    NONE,
    API_KEY,
    BEARER,
    BASIC
  }

  private static final AuthSpec NONE = new AuthSpec(Scheme.NONE, null, null);

  private final Scheme scheme;
  private final String headerName; // for API_KEY
  private final String credential;

  public AuthSpec(Scheme scheme, String headerName, String credential) {
    this.scheme = Objects.requireNonNull(scheme, "scheme");
    this.headerName = headerName;
    this.credential = credential;
    if (scheme == Scheme.API_KEY && (headerName == null || headerName.isEmpty())) {
      throw new IllegalArgumentException("API_KEY auth requires a headerName");
    }
    if (scheme != Scheme.NONE && (credential == null || credential.isEmpty())) {
      throw new IllegalArgumentException(scheme + " auth requires a credential");
    }
  }

  public static AuthSpec none() {
    return NONE;
  }

  public static AuthSpec apiKey(String headerName, String credential) {
    return new AuthSpec(Scheme.API_KEY, headerName, credential);
  }

  public static AuthSpec bearer(String token) {
    return new AuthSpec(Scheme.BEARER, null, token);
  }

  public static AuthSpec basic(String base64Credentials) {
    return new AuthSpec(Scheme.BASIC, null, base64Credentials);
  }

  public Scheme getScheme() {
    return scheme;
  }

  public String getHeaderName() {
    return headerName;
  }

  public String getCredential() {
    return credential;
  }

  /** Render this spec into the HTTP header(s) a client should attach. Empty for {@link Scheme#NONE}. */
  public Map<String, String> toHeaders() {
    switch (scheme) {
      case API_KEY:
        return Collections.singletonMap(headerName, credential);
      case BEARER:
        return Collections.singletonMap("Authorization", "Bearer " + credential);
      case BASIC:
        return Collections.singletonMap("Authorization", "Basic " + credential);
      case NONE:
      default:
        return new LinkedHashMap<>();
    }
  }

  @Override
  public String toString() {
    // Never leak the credential.
    return "AuthSpec{scheme=" + scheme + (headerName != null ? ", header=" + headerName : "") + '}';
  }
}

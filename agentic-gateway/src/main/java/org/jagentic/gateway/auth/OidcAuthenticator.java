package org.jagentic.gateway.auth;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import org.jagentic.core.Principal;

/**
 * Validates an OIDC access token: its signature against the issuer's JWKS, then issuer, audience,
 * expiry and not-before. Only asymmetric algorithms are accepted, so {@code alg: none} and a token
 * signed with the public key as an HMAC secret both fail.
 */
public final class OidcAuthenticator implements Authenticator {

  /**
   * @param issuer the exact {@code iss}
   * @param audience the {@code aud} a token must name
   * @param jwksUrl where the issuer's keys are; for Keycloak, {@code <issuer>/protocol/openid-connect/certs}
   * @param tenantClaim the claim that carries the tenant
   * @param rolesClaim the claim that carries roles, dotted for a nested one, such as {@code
   *     realm_access.roles}
   */
  public record Settings(String issuer, String audience, String jwksUrl, String tenantClaim, String rolesClaim) {}

  private final DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
  private final Settings settings;

  public OidcAuthenticator(Settings settings) {
    this(settings, keysAt(settings.jwksUrl()));
  }

  /** As above with a key source of the caller's. */
  public OidcAuthenticator(Settings settings, JWKSource<SecurityContext> keys) {
    this.settings = settings;
    processor.setJWSKeySelector(new JWSVerificationKeySelector<>(
        Set.of(JWSAlgorithm.RS256, JWSAlgorithm.PS256, JWSAlgorithm.ES256), keys));
    processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
        settings.audience(),
        new JWTClaimsSet.Builder().issuer(settings.issuer()).build(),
        new HashSet<>(List.of("sub", "exp"))));
  }

  private static JWKSource<SecurityContext> keysAt(String url) {
    try {
      URL jwks = URI.create(url).toURL();
      return JWKSourceBuilder.<SecurityContext>create(jwks).retrying(true).build();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("AGENTIC_OIDC_JWKS_URL is not a url: " + url, e);
    }
  }

  @Override
  public Principal authenticate(String bearerToken) throws Unauthenticated {
    JWTClaimsSet claims;
    try {
      claims = processor.process(bearerToken, null);
    } catch (ParseException | BadJOSEException | JOSEException e) {
      throw new Unauthenticated("token rejected: " + e.getMessage(), e);
    }
    Object tenant = claims.getClaim(settings.tenantClaim());
    return new Principal(claims.getSubject(), tenant == null ? null : String.valueOf(tenant), roles(claims),
        claims.getIssuer());
  }

  private List<String> roles(JWTClaimsSet claims) {
    Object at = claims.getClaims();
    for (String part : settings.rolesClaim().split("\\.")) {
      at = at instanceof Map<?, ?> m ? m.get(part) : null;
    }
    List<String> roles = new ArrayList<>();
    if (at instanceof Collection<?> listed) {
      listed.forEach(r -> roles.add(String.valueOf(r)));
    }
    return roles;
  }
}

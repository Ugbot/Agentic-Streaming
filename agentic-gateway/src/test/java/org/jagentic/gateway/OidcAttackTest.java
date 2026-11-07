package org.jagentic.gateway;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.jagentic.gateway.auth.OidcAuthenticator;
import org.jagentic.gateway.auth.Unauthenticated;
import org.junit.jupiter.api.Test;

/** Tokens that found something, each signed with the issuer's real key. Each test failed before its fix. */
class OidcAttackTest {

  private static final String ISSUER = "https://idp.example/realms/agentic";

  private static String signed(RSAKey key, JWTClaimsSet claims) throws Exception {
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  private static JWTClaimsSet.Builder claims() {
    return new JWTClaimsSet.Builder().issuer(ISSUER).audience("gw").subject("alice")
        .expirationTime(new Date(System.currentTimeMillis() + 60_000)).claim("tenant", "acme");
  }

  /**
   * A tenant claim that was a list or an object became its toString, so {@code ["acme","beta"]} was
   * the tenant {@code [acme, beta]}: a tenant nobody named. A tenant is a string, or there is none.
   * A blank subject threw IllegalArgumentException out of the authenticator, which the gateway
   * answered with a 500 where every other bad token gets a 401.
   */
  @Test
  void aTenantIsAStringAndABlankSubjectIsAnInvalidToken() throws Exception {
    RSAKey key = new RSAKeyGenerator(2048).keyID("k1").generate();
    OidcAuthenticator oidc = new OidcAuthenticator(
        new OidcAuthenticator.Settings(ISSUER, "gw", "", "tenant", "realm_access.roles"),
        new ImmutableJWKSet<>(new JWKSet(key.toPublicJWK())));
    assertNull(oidc.authenticate(signed(key, claims().claim("tenant", List.of("acme", "beta")).build())).tenant());
    assertNull(oidc.authenticate(signed(key, claims().claim("tenant", Map.of("id", "acme")).build())).tenant());
    assertThrows(Unauthenticated.class, () -> oidc.authenticate(signed(key, claims().subject(" ").build())));
  }
}

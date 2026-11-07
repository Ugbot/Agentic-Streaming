package org.jagentic.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.jagentic.core.Principal;
import org.jagentic.gateway.auth.OidcAuthenticator;
import org.jagentic.gateway.auth.Unauthenticated;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tokens the gateway must not believe, each signed for real. */
class OidcAuthenticatorTest {

  private static final String ISSUER = "https://idp.example/realms/agentic";
  private static final String AUDIENCE = "agentic-gateway";
  private static RSAKey key;
  private static RSAKey stranger;
  private static OidcAuthenticator oidc;

  @BeforeAll
  static void keys() throws Exception {
    key = new RSAKeyGenerator(2048).keyID("k1").generate();
    stranger = new RSAKeyGenerator(2048).keyID("k1").generate();
    oidc = new OidcAuthenticator(
        new OidcAuthenticator.Settings(ISSUER, AUDIENCE, "", "tenant", "realm_access.roles"),
        new ImmutableJWKSet<>(new JWKSet(key.toPublicJWK())));
  }

  private static JWTClaimsSet.Builder claims() {
    return new JWTClaimsSet.Builder().issuer(ISSUER).audience(AUDIENCE).subject("alice")
        .expirationTime(new Date(System.currentTimeMillis() + 60_000))
        .claim("tenant", "acme").claim("realm_access", Map.of("roles", List.of("approver")));
  }

  private static String signed(RSAKey with, JWTClaimsSet set) throws Exception {
    SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(with.getKeyID()).build(), set);
    jwt.sign(new RSASSASigner(with));
    return jwt.serialize();
  }

  private static void refused(String token) {
    assertThrows(Unauthenticated.class, () -> oidc.authenticate(token));
  }

  @Test
  void aGoodTokenIsItsSubjectTenantAndRoles() throws Exception {
    Principal p = oidc.authenticate(signed(key, claims().build()));
    assertEquals("alice", p.subject());
    assertEquals("acme", p.tenant());
    assertEquals(List.of("approver"), p.roles());
    assertEquals(ISSUER, p.issuer());
  }

  @Test
  void anExpiredTokenIsRefused() throws Exception {
    refused(signed(key, claims().expirationTime(new Date(System.currentTimeMillis() - 600_000)).build()));
  }

  @Test
  void aTokenNotYetValidIsRefused() throws Exception {
    refused(signed(key, claims().notBeforeTime(new Date(System.currentTimeMillis() + 600_000)).build()));
  }

  @Test
  void aTokenWithNoExpiryIsRefused() throws Exception {
    refused(signed(key, claims().expirationTime(null).build()));
  }

  @Test
  void anotherIssuerIsRefused() throws Exception {
    refused(signed(key, claims().issuer("https://evil.example/").build()));
  }

  @Test
  void anotherAudienceIsRefused() throws Exception {
    refused(signed(key, claims().audience("some-other-api").build()));
  }

  @Test
  void aTokenWithNoSubjectIsRefused() throws Exception {
    refused(signed(key, claims().subject(null).build()));
  }

  @Test
  void aSignatureFromAnotherKeyIsRefused() throws Exception {
    refused(signed(stranger, claims().build()));
  }

  @Test
  void aTamperedPayloadIsRefused() throws Exception {
    String[] parts = signed(key, claims().build()).split("\\.");
    String forged = Base64.getUrlEncoder().withoutPadding().encodeToString(
        claims().subject("mallory").build().toString().getBytes(StandardCharsets.UTF_8));
    refused(parts[0] + "." + forged + "." + parts[2]);
  }

  @Test
  void algNoneIsRefused() {
    Base64.Encoder url = Base64.getUrlEncoder().withoutPadding();
    String header = url.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
    String body = url.encodeToString(claims().build().toString().getBytes(StandardCharsets.UTF_8));
    refused(header + "." + body + ".");
  }

  @Test
  void anHmacSignedWithThePublicKeyIsRefused() throws Exception {
    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims().build());
    jwt.sign(new MACSigner(key.toPublicKey().getEncoded()));
    refused(jwt.serialize());
  }

  @Test
  void rubbishIsRefused() {
    refused("");
    refused(UUID.randomUUID().toString());
    refused("a.b.c");
  }
}

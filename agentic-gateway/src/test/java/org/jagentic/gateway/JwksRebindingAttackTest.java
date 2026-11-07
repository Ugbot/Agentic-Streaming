package org.jagentic.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.jagentic.gateway.auth.Unauthenticated;

/**
 * The issuer's keys may come over plain http only from loopback, and that was decided once, at
 * start, by resolving the JWKS host. The fetch resolved it again, so a name that said loopback at
 * start and something else later had the gateway take its signing keys over plain http from
 * anywhere: whoever answered could sign any token. Each fetch now resolves once, checks that a plain
 * http host is loopback at every address, and connects to the address it checked.
 */
class JwksRebindingAttackTest {

  @AfterEach
  void clear() {
    RebindingResolverProvider.later = null;
  }

  private static InetAddress somewhereElse() throws Exception {
    for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      if (!nif.isUp() || nif.isLoopback()) {
        continue;
      }
      for (InetAddress a : Collections.list(nif.getInetAddresses())) {
        if (a instanceof Inet4Address && !a.isLoopbackAddress() && !a.isLinkLocalAddress()) {
          return a;
        }
      }
    }
    return null;
  }

  @Test
  void theKeysDoNotComeOverPlainHttpFromWhereARebindingNamePoints() throws Exception {
    InetAddress elsewhere = somewhereElse();
    Assumptions.assumeTrue(elsewhere != null, "no address on this machine but loopback");
    AtomicInteger connections = new AtomicInteger();
    try (ServerSocket keys = new ServerSocket()) {
      keys.bind(new InetSocketAddress(InetAddress.getByAddress(new byte[] {0, 0, 0, 0}), 0));
      Thread acceptor = Thread.ofVirtual().start(() -> {
        while (!keys.isClosed()) {
          try (Socket s = keys.accept()) {
            connections.incrementAndGet();
          } catch (java.io.IOException closed) {
            return;
          }
        }
      });
      String jwks = "http://idp.rebind.test:" + keys.getLocalPort() + "/certs";
      GatewayOptions options = GatewayOptions.fromEnvironment(Map.of(
          "AGENTIC_OIDC_ISSUER", "https://idp.example", "AGENTIC_OIDC_AUDIENCE", "gw",
          "AGENTIC_OIDC_JWKS_URL", jwks)); // loopback at start, so plain http is allowed
      RebindingResolverProvider.later = elsewhere;

      RSAKey key = new RSAKeyGenerator(2048).keyID("k1").generate();
      SignedJWT token = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("k1").build(),
          new JWTClaimsSet.Builder().issuer("https://idp.example").audience("gw").subject("alice")
              .expirationTime(new Date(System.currentTimeMillis() + 60_000)).build());
      token.sign(new RSASSASigner(key));
      assertThrows(Unauthenticated.class, () -> options.authenticator().authenticate(token.serialize()));
      Thread.sleep(200);
      assertEquals(0, connections.get(), "the keys were fetched over plain http from " + elsewhere);
      keys.close();
      acceptor.join(1000);
    }
  }
}
